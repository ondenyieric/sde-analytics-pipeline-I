

  create view `staging`.`stg_products__dbt_tmp` 
  
    
    
  as (
    -- Collapses the append-only CDC event history down to current-row
-- semantics: one row per product_id, reflecting the most recent event.
-- is_deleted is derived from the CDC operation type ('d' = delete).

with raw_products as (
    select *
    from `raw`.`products`
),

ranked as (
    select
        id as product_id,
        title,
        category,
        price,
        stock,
        _cdc_op,
        _cdc_ts,
        row_number() over (
            partition by id
            order by _cdc_ts desc
        ) as rn
    from raw_products
)

select
    product_id,
    title,
    category,
    price,
    stock,
    if(_cdc_op = 'd', 1, 0) as is_deleted,
    _cdc_ts as updated_at
from ranked
where rn = 1
  )
      
      
                    -- end_of_sql
                    
                    