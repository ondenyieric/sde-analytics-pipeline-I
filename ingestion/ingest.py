"""REST API -> PostgreSQL ingestion with deterministic keys, rate limiting and metrics."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from threading import Lock

import psycopg2
import psycopg2.extras
import requests
import yaml
from prometheus_client import (
    CollectorRegistry,
    Gauge,
    push_to_gateway,
    start_http_server,
)
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("ingest")

LAST_SUCCESS_TS = Gauge(
    "ingestion_last_success_timestamp_seconds",
    "Unix timestamp of the last successful ingestion run per resource",
    ["resource"],
)
RUN_FAILURES = Gauge(
    "ingestion_run_failure", "1 when the most recent ingestion run failed", ["resource"]
)
ROWS_LAST_RUN = Gauge(
    "ingestion_rows_last_run", "Rows upserted by the most recent run", ["resource"]
)
RUN_SUCCESS = Gauge(
    "ingestion_run_success",
    "1 when the most recent run succeeded, otherwise 0",
    ["resource"],
)


class RateLimiter:
    def __init__(self, requests_per_minute: int):
        self.interval = 60.0 / requests_per_minute if requests_per_minute > 0 else 0.0
        self._lock = Lock()
        self._last_request = 0.0

    def wait(self) -> None:
        if self.interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            sleep_for = self.interval - (now - self._last_request)
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._last_request = time.monotonic()


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def pg_connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "pipeline"),
        user=os.environ.get("POSTGRES_USER", "pipeline"),
        password=os.environ.get("POSTGRES_PASSWORD", "pipeline"),
    )


def ensure_table(conn, resource: dict) -> None:
    cols_sql = ", ".join(f'"{c}" {t}' for c, t in resource["columns"].items())
    pk = resource["primary_key"]
    ddl = f"""CREATE TABLE IF NOT EXISTS public."{resource['table']}" (
        {cols_sql},
        _ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY ("{pk}")
    );"""
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(requests.RequestException),
)
def fetch_page(
    base_url: str,
    resource: dict,
    skip: int,
    headers: dict,
    timeout: int,
    limiter: RateLimiter,
) -> dict:
    limiter.wait()
    url = f"{base_url.rstrip('/')}{resource['endpoint']}"
    params = {
        resource["limit_param"]: resource["page_size"],
        resource["skip_param"]: skip,
    }
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_all(
    base_url: str, resource: dict, headers: dict, timeout: int, limiter: RateLimiter
) -> list[dict]:
    url = f"{base_url.rstrip('/')}{resource['endpoint']}"
    if not resource.get("paginated"):
        limiter.wait()
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        return payload[resource["list_key"]] if resource.get("list_key") else payload

    items: list[dict] = []
    skip = 0
    while True:
        payload = fetch_page(base_url, resource, skip, headers, timeout, limiter)
        batch = payload.get(resource["list_key"], [])
        if not batch:
            break
        items.extend(batch)
        total = payload.get("total", len(items))
        skip += resource["page_size"]
        if skip >= total:
            break
    return items


def flatten_orders(carts: list[dict]) -> list[dict]:
    """Explode carts into deterministic order-line rows keyed by cart_id:product_id."""
    now = datetime.now(timezone.utc)
    rows = []
    for cart in carts:
        cart_id = cart.get("id")
        for item in cart.get("products", []):
            product_id = item.get("id")
            if cart_id is None or product_id is None:
                log.warning(
                    "Skipping malformed line item: cart_id=%s product_id=%s",
                    cart_id,
                    product_id,
                )
                continue
            rows.append(
                {
                    "id": f"{cart_id}:{product_id}",
                    "user_id": cart.get("userId"),
                    "product_id": product_id,
                    "quantity": item.get("quantity"),
                    "order_ts": now,
                }
            )
    return rows


def upsert_rows(conn, resource: dict, rows: list[dict]) -> int:
    if not rows:
        return 0
    columns = list(resource["columns"].keys())
    pk = resource["primary_key"]
    immutable = set(resource.get("immutable_columns", []))
    update_cols = [c for c in columns if c != pk and c not in immutable]
    col_list = ", ".join(f'"{c}"' for c in columns)
    set_parts = [f'"{c}" = EXCLUDED."{c}"' for c in update_cols]
    set_parts.append('"_ingested_at" = now()')
    set_clause = ", ".join(set_parts)
    sql = f"""INSERT INTO public."{resource['table']}" ({col_list}) VALUES %s
        ON CONFLICT ("{pk}") DO UPDATE SET {set_clause};"""
    # PostgreSQL rejects a single INSERT ... ON CONFLICT statement when the
    # same constrained key occurs more than once in VALUES.  This can happen
    # with imperfect/upstream APIs (e.g. duplicate product lines in a cart).
    # Keep the last occurrence deterministically before issuing the upsert.
    deduped = {}
    for row in rows:
        key = row.get(pk)
        if key is not None:
            deduped[key] = row
    values = [tuple(row.get(c) for c in columns) for row in deduped.values()]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values)
    conn.commit()
    return len(values)


def publish_metrics() -> None:
    gateway = os.environ.get("PUSHGATEWAY_URL")
    if not gateway:
        return
    try:
        registry = CollectorRegistry()
        metric_defs = (LAST_SUCCESS_TS, RUN_FAILURES, ROWS_LAST_RUN, RUN_SUCCESS)
        registry_metrics = {
            metric._name: Gauge(
                metric._name,
                metric._documentation,
                metric._labelnames,
                registry=registry,
            )
            for metric in metric_defs
        }
        for metric in metric_defs:
            target = registry_metrics[metric._name]
            for family in metric.collect():
                for sample in family.samples:
                    target.labels(**sample.labels).set(sample.value)
        push_to_gateway(gateway, job="analytics_ingestion", registry=registry)
    except Exception:
        log.exception("Unable to publish metrics to Pushgateway")


def run_once(config_path: str) -> None:
    config = load_config(config_path)
    base_url = os.environ.get("API_BASE_URL") or os.environ.get(
        config["api"]["base_url_env"], "https://dummyjson.com"
    )
    timeout = int(
        os.environ.get("API_TIMEOUT_SECONDS", config["api"].get("timeout_seconds", 15))
    )
    rpm = int(
        os.environ.get(
            "REQUESTS_PER_MINUTE", config["api"].get("requests_per_minute", 60)
        )
    )
    limiter = RateLimiter(rpm)
    headers: dict[str, str] = {}
    api_key = os.environ.get("API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    conn = pg_connect()
    try:
        for resource in config["resources"]:
            name = resource["name"]
            try:
                log.info(
                    "Fetching resource=%s from %s%s",
                    name,
                    base_url,
                    resource["endpoint"],
                )
                raw_items = fetch_all(base_url, resource, headers, timeout, limiter)
                rows = (
                    flatten_orders(raw_items)
                    if resource.get("flatten_from")
                    else raw_items
                )
                cols = resource["columns"].keys()
                rows = [{c: r.get(c) for c in cols} for r in rows]
                ensure_table(conn, resource)
                n = upsert_rows(conn, resource, rows)
                ROWS_LAST_RUN.labels(resource=name).set(n)
                LAST_SUCCESS_TS.labels(resource=name).set(time.time())
                RUN_SUCCESS.labels(resource=name).set(1)
                RUN_FAILURES.labels(resource=name).set(0)
                log.info("Upserted %d rows into %s", n, resource["table"])
            except Exception:
                RUN_FAILURES.labels(resource=name).set(1)
                RUN_SUCCESS.labels(resource=name).set(0)
                log.exception("Ingestion failed for resource=%s", name)
                raise
    finally:
        conn.close()
        publish_metrics()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", default=os.path.join(os.path.dirname(__file__), "config.yaml")
    )
    parser.add_argument("--serve-metrics", action="store_true")
    parser.add_argument("--metrics-hold-seconds", type=int, default=0)
    args = parser.parse_args()
    if args.serve_metrics:
        start_http_server(8000)
    try:
        run_once(args.config)
    except Exception:
        log.error("Ingestion run failed")
        return 1
    if args.serve_metrics and args.metrics_hold_seconds:
        time.sleep(args.metrics_hold_seconds)
    return 0


if __name__ == "__main__":
    sys.exit(main())
