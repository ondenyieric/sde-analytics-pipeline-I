-- MergeTree, partitioned by month (toYYYYMM(order_date)) to keep partitions
-- manageable for a daily-growing fact table, and to let ClickHouse prune
-- whole months on date-ranged queries.
-- Ordered by (order_date, category, order_id) to match the dominant query
-- shape (date-ranged, category-filtered aggregation) so ClickHouse can skip
-- granules instead of scanning the full partition.
-- See docs/design_report.docx §3.2.

{{
    config(
        materialized='table',
        engine='MergeTree',
        partition_by='toYYYYMM(order_date)',
        order_by='(order_date, category, order_id)'
    )
}}

select
    o.order_id,
    o.order_date,
    o.user_id,
    o.product_id,
    p.category,
    o.quantity,
    o.order_ts,
    o.quantity * p.current_price as revenue
from {{ ref('stg_orders') }} as o
inner join {{ ref('dim_products') }} as p
    on o.product_id = p.product_id
