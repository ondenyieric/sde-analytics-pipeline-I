import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import ingest  # noqa: E402


def test_flatten_orders_uses_deterministic_order_line_key():
    carts = [{"id": 1, "userId": 42, "products": [{"id": 10, "quantity": 2}, {"id": 11, "quantity": 1}]}]
    rows = ingest.flatten_orders(carts)
    assert [r["id"] for r in rows] == ["1:10", "1:11"]
    assert rows[0]["user_id"] == 42
    assert rows[0]["product_id"] == 10
    assert rows[0]["quantity"] == 2
    assert rows[0]["order_ts"].tzinfo == timezone.utc


def test_flatten_orders_is_stable_across_runs():
    carts = [{"id": 7, "userId": 9, "products": [{"id": 3, "quantity": 4}]}]
    first = ingest.flatten_orders(carts)[0]["id"]
    second = ingest.flatten_orders(carts)[0]["id"]
    assert first == second == "7:3"


def test_flatten_orders_skips_malformed_items():
    carts = [{"id": 1, "userId": 42, "products": [{"quantity": 2}, {"id": 11, "quantity": 1}]}]
    rows = ingest.flatten_orders(carts)
    assert [r["id"] for r in rows] == ["1:11"]


def test_fetch_all_stops_when_batch_reaches_total():
    resource = {"endpoint": "/products", "list_key": "products", "paginated": True, "limit_param": "limit", "skip_param": "skip", "page_size": 2}
    responses = [{"products": [{"id": 1}, {"id": 2}], "total": 3}, {"products": [{"id": 3}], "total": 3}]
    with patch("ingest.fetch_page", side_effect=responses) as mocked:
        items = ingest.fetch_all("https://example.com", resource, headers={}, timeout=15, limiter=ingest.RateLimiter(0))
    assert [i["id"] for i in items] == [1, 2, 3]
    assert mocked.call_count == 2


def test_upsert_rows_excludes_immutable_columns_from_updates():
    resource = {"table": "orders", "primary_key": "id", "immutable_columns": ["order_ts"], "columns": {"id": "TEXT", "quantity": "INTEGER", "order_ts": "TIMESTAMP"}}
    rows = [{"id": "1:10", "quantity": 2, "order_ts": datetime.now(timezone.utc)}]
    conn = MagicMock()
    with patch("ingest.psycopg2.extras.execute_values") as mocked_execute:
        assert ingest.upsert_rows(conn, resource, rows) == 1
    sql_arg = mocked_execute.call_args[0][1]
    assert 'ON CONFLICT ("id")' in sql_arg
    assert '"quantity" = EXCLUDED."quantity"' in sql_arg
    assert '"order_ts" = EXCLUDED."order_ts"' not in sql_arg


def test_upsert_rows_no_op_on_empty_rows():
    conn = MagicMock()
    assert ingest.upsert_rows(conn, {"table": "x", "primary_key": "id", "columns": {"id": "INTEGER"}}, []) == 0
    conn.cursor.assert_not_called()


def test_rate_limiter_disabled_for_zero():
    ingest.RateLimiter(0).wait()


def test_upsert_rows_deduplicates_duplicate_primary_keys():
    resource = {
        "table": "orders",
        "primary_key": "id",
        "columns": {"id": "TEXT", "quantity": "INTEGER"},
    }
    rows = [
        {"id": "1:10", "quantity": 1},
        {"id": "1:10", "quantity": 2},
    ]
    conn = MagicMock()
    with patch("ingest.psycopg2.extras.execute_values") as mocked_execute:
        assert ingest.upsert_rows(conn, resource, rows) == 1
    values = mocked_execute.call_args[0][2]
    assert values == [("1:10", 2)]


def test_publish_metrics_passes_registry_to_pushgateway():
    with patch.dict(os.environ, {"PUSHGATEWAY_URL": "http://pushgateway:9091"}):
        with patch("ingest.push_to_gateway") as mocked_push:
            ingest.ROWS_LAST_RUN.labels(resource="orders").set(3)
            ingest.publish_metrics()
    assert mocked_push.call_count == 1
    assert mocked_push.call_args.kwargs["job"] == "analytics_ingestion"
    assert mocked_push.call_args.kwargs["registry"] is not None
