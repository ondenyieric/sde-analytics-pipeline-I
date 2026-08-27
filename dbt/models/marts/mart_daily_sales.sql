-- Pre-aggregated rollup so dashboards hit a small table instead of
-- re-scanning fct_orders on every load (design_report.docx §3.2).
--
-- Implementation note: fct_orders is rebuilt in full on every dbt run in
-- this reference implementation, so this is expressed as a regular
-- aggregate table refreshed alongside it. In a production setup where
-- fct_orders is loaded incrementally, this would instead be a native
-- ClickHouse MATERIALIZED VIEW (AggregatingMergeTree, -State/-Merge
-- combinators) that updates automatically as new rows land in fct_orders,
-- avoiding a full re-aggregation on every run.

{{
    config(
        materialized='table',
        engine='SummingMergeTree',
        order_by='(order_date, category)'
    )
}}

select
    order_date,
    category,
    count(distinct order_id) as total_orders,
    sum(revenue) as total_revenue,
    sum(revenue) / nullif(count(distinct order_id), 0) as avg_order_value
from {{ ref('fct_orders') }}
group by order_date, category
