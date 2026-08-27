-- Kafka engine tables are ephemeral consumers -- they hold no data at rest;
-- reading from them consumes the topic. Each has a materialized view that
-- pushes parsed rows into the corresponding raw.* MergeTree table.
-- This is the "sink connector / consumer" step in Figure 1 of the design report.

CREATE TABLE IF NOT EXISTS raw.products_queue
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'pipeline.public.products',
    kafka_group_name = 'clickhouse_products_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS raw.orders_queue
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'pipeline.public.orders',
    kafka_group_name = 'clickhouse_orders_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

-- Debezium envelope (schemas disabled) looks like:
-- {"before": {...}|null, "after": {...}|null, "op": "c"|"u"|"d"|"r", "ts_ms": 123, "source": {...}}
-- For deletes, "after" is null, so fall back to "before" to still capture
-- the primary key + last-known values for the tombstone row.

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.products_mv TO raw.products AS
SELECT
    if(JSONExtractRaw(message, 'after') != 'null', JSONExtractInt(message, 'after', 'id'),
       JSONExtractInt(message, 'before', 'id'))                          AS id,
    if(JSONExtractRaw(message, 'after') != 'null', JSONExtractString(message, 'after', 'title'),
       JSONExtractString(message, 'before', 'title'))                    AS title,
    if(JSONExtractRaw(message, 'after') != 'null', JSONExtractString(message, 'after', 'category'),
       JSONExtractString(message, 'before', 'category'))                 AS category,
    if(JSONExtractRaw(message, 'after') != 'null', JSONExtractFloat(message, 'after', 'price'),
       JSONExtractFloat(message, 'before', 'price'))                     AS price,
    if(JSONExtractRaw(message, 'after') != 'null', JSONExtractInt(message, 'after', 'stock'),
       JSONExtractInt(message, 'before', 'stock'))                       AS stock,
    JSONExtractString(message, 'op')                                         AS _cdc_op,
    toDateTime64(JSONExtractUInt(message, 'ts_ms') / 1000, 3)                 AS _cdc_ts,
    now64(3)                                                                  AS _ingested_at
FROM raw.products_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.orders_mv TO raw.orders AS
SELECT
    if(JSONExtractRaw(message, 'after') != 'null', JSONExtractString(message, 'after', 'id'),
       JSONExtractString(message, 'before', 'id'))                          AS id,
    if(JSONExtractRaw(message, 'after') != 'null', JSONExtractInt(message, 'after', 'user_id'),
       JSONExtractInt(message, 'before', 'user_id'))                     AS user_id,
    if(JSONExtractRaw(message, 'after') != 'null', JSONExtractInt(message, 'after', 'product_id'),
       JSONExtractInt(message, 'before', 'product_id'))                  AS product_id,
    if(JSONExtractRaw(message, 'after') != 'null', JSONExtractInt(message, 'after', 'quantity'),
       JSONExtractInt(message, 'before', 'quantity'))                    AS quantity,
    toDateTime64(if(JSONExtractRaw(message, 'after') != 'null', JSONExtractString(message, 'after', 'order_ts'),
       JSONExtractString(message, 'before', 'order_ts')), 3)  AS order_ts,
    JSONExtractString(message, 'op')                                         AS _cdc_op,
    toDateTime64(JSONExtractUInt(message, 'ts_ms') / 1000, 3)                 AS _cdc_ts,
    now64(3)                                                                  AS _ingested_at
FROM raw.orders_queue;
