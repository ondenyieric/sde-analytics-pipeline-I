# ClickHouse Diagnostic Queries

Run these with `docker compose exec clickhouse clickhouse-client`, or over HTTP:
`curl 'http://localhost:8123/?query=...'`
ui: http://localhost:8123/play

## 1. Connectivity / sanity

```sql
SELECT 1;
SHOW DATABASES;
```

## 2. Confirm the raw/mart layout from the init scripts

```sql
SHOW TABLES FROM raw;
SHOW TABLES FROM mart;
DESCRIBE TABLE raw.products;
DESCRIBE TABLE raw.orders;
```

## 3. Kafka engine tables — are they actually consuming?

```sql
-- Kafka engine tables hold no data at rest; this just confirms they exist
SHOW CREATE TABLE raw.products_queue;
SHOW CREATE TABLE raw.orders_queue;

-- Real diagnostic: consumer group status, errors, assigned partitions
SELECT database, table, consumer_id, assignments.topic, assignments.partition_id,
       assignments.current_offset, exceptions.text, exceptions.time
FROM system.kafka_consumers
WHERE database = 'raw';
```

## 4. Materialized views — are rows landing?

```sql
SHOW CREATE TABLE raw.products_mv;
SELECT count() FROM raw.products;
SELECT count() FROM raw.orders;
SELECT * FROM raw.products ORDER BY _ingested_at DESC LIMIT 20;
SELECT * FROM raw.orders ORDER BY _ingested_at DESC LIMIT 20;
```

## 5. CDC health — check for stuck/duplicate events

```sql
-- op distribution: c=create, u=update, d=delete, r=snapshot read
SELECT _cdc_op, count() FROM raw.products GROUP BY _cdc_op;

-- lag between the source txn and ClickHouse ingest
SELECT id, _cdc_ts, _ingested_at, _ingested_at - _cdc_ts AS lag
FROM raw.orders ORDER BY _ingested_at DESC LIMIT 20;
```

## 6. dbt mart tables (once dbt has run)

```sql
SHOW TABLES FROM mart;
DESCRIBE TABLE mart.fct_orders;
DESCRIBE TABLE mart.dim_products;
SELECT * FROM mart.mart_daily_sales ORDER BY 1 DESC LIMIT 10;
```

## 7. Errors / crashes

```sql
SELECT event_time, name, value FROM system.errors WHERE value > 0 ORDER BY value DESC;
SELECT * FROM system.text_log WHERE level = 'Error' ORDER BY event_time DESC LIMIT 50;
```

## 8. Disk / merge health (MergeTree specific)

```sql
SELECT database, table, formatReadableSize(sum(bytes_on_disk)) AS size, count() AS parts
FROM system.parts WHERE active GROUP BY database, table;

SELECT database, table, elapsed, progress, is_mutation FROM system.merges;
```

## 9. Users / access

Matches `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` in the compose env.

```sql
SELECT * FROM system.users;
SHOW GRANTS FOR default;
```

---

**Suggested order once the connection is fixed:** run #1 → #3 → #4 first — that's
the fastest way to tell whether you're looking at a networking problem, a Kafka
consumer problem, or an MV/schema problem.
