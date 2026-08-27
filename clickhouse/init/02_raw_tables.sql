-- Raw layer: one row per CDC event, append-only. Never overwritten, never
-- queried directly by consumers -- dbt staging models sit on top of this.
-- Ordered by _cdc_ts per docs/design_report.docx §3.2 (high-throughput
-- sequential writes from the Kafka consumer, not point lookups).

CREATE TABLE IF NOT EXISTS raw.products
(
    id           Int64,
    title        String,
    category     String,
    price        Decimal(10, 2),
    stock        Int32,
    _cdc_op      LowCardinality(String),
    _cdc_ts      DateTime64(3),
    _ingested_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (_cdc_ts, id);

CREATE TABLE IF NOT EXISTS raw.orders
(
    id           String,
    user_id      Int64,
    product_id   Int64,
    quantity     Int32,
    order_ts     DateTime64(3),
    _cdc_op      LowCardinality(String),
    _cdc_ts      DateTime64(3),
    _ingested_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (_cdc_ts, id);
