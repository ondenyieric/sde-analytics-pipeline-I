"""End-to-end orchestration: API -> Postgres -> Debezium/Kafka -> ClickHouse -> dbt."""
from __future__ import annotations

import os
import subprocess
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/dbt")
PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")


def _run_dbt(command: str, failure_metric: str | None = None) -> None:
    result = subprocess.run(
        ["bash", "-lc", f"cd {DBT_PROJECT_DIR} && {command} --profiles-dir {DBT_PROJECT_DIR}"],
        check=False,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    print(result.stderr)
    if result.returncode != 0 and failure_metric:
        from prometheus_client import CollectorRegistry, Counter, push_to_gateway
        registry = CollectorRegistry()
        metric = Counter(failure_metric, "dbt task failures", registry=registry)
        metric.inc()
        try:
            push_to_gateway(PUSHGATEWAY_URL, job="dbt", registry=registry)
        except Exception as exc:
            print(f"Unable to publish dbt failure metric: {exc}")
        raise RuntimeError(f"dbt command failed: {command}")


@dag(
    dag_id="analytics_pipeline",
    description="REST API -> Postgres -> Debezium -> Kafka -> ClickHouse -> dbt",
    schedule="*/3 * * * *", # every 3 minutes
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    },
    tags=["pipeline", "cdc", "dbt", "clickhouse"],
)

def analytics_pipeline():
    ingest = BashOperator(
        task_id="ingest_from_api",
        bash_command="python /opt/ingestion/ingest.py --config /opt/ingestion/config.yaml",
        env={**os.environ, "POSTGRES_HOST": "postgres", "POSTGRES_DB": os.environ.get("POSTGRES_DB", "pipeline"),
             "POSTGRES_USER": os.environ.get("POSTGRES_USER", "pipeline"),
             "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "pipeline"),
             "API_BASE_URL": os.environ.get("API_BASE_URL", "https://dummyjson.com"),
             "API_KEY": os.environ.get("API_KEY", ""),
             "REQUESTS_PER_MINUTE": os.environ.get("REQUESTS_PER_MINUTE", "60"),
             "PUSHGATEWAY_URL": PUSHGATEWAY_URL},
        sla=timedelta(minutes=10),
    )

    @task.sensor(poke_interval=15, timeout=300, mode="reschedule")
    def wait_for_cdc_lag_to_settle() -> bool:
        """Wait until Kafka exporter reports zero lag for ClickHouse CDC consumers."""
        import requests
        try:
            text = requests.get("http://kafka-exporter:9308/metrics", timeout=5).text
            lags = []
            for line in text.splitlines():
                if not line.startswith("kafka_consumergroup_lag{"):
                    continue
                if 'topic="pipeline.' not in line or 'consumergroup="clickhouse_' not in line:
                    continue
                try:
                    lags.append(float(line.rsplit(" ", 1)[-1]))
                except ValueError:
                    continue
            return bool(lags) and max(lags) <= 0
        except requests.RequestException:
            return False

    @task(task_id="dbt_build")
    def dbt_build():
        _run_dbt("dbt run")

    @task(task_id="dbt_test")
    def dbt_test():
        _run_dbt("dbt test", "dbt_test_failures_total")

    ingest >> wait_for_cdc_lag_to_settle() >> dbt_build() >> dbt_test()


analytics_pipeline()
