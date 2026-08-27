-- ReplacingMergeTree(updated_at): products get updated in place upstream;
-- this engine keeps only the latest version per product_id on merge,
-- matching dimension-table semantics without manual dedup logic.
-- See docs/design_report.docx §3.2.



select
    product_id,
    title,
    category,
    price as current_price,
    updated_at
from `staging`.`stg_products`
where not is_deleted