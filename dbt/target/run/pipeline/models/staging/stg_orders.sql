

  create view `staging`.`stg_orders__dbt_tmp` 
  
    
    
  as (
    -- Collapses the append-only CDC event history down to current-row
-- semantics: one row per order_id (a cart/product line item), reflecting
-- the most recent event, with deletes excluded from downstream marts.

with raw_orders as (
    select *
    from `raw`.`orders`
),

ranked as (
    select
        id as order_id,
        user_id,
        product_id,
        quantity,
        order_ts,
        _cdc_op,
        _cdc_ts,
        row_number() over (
            partition by id
            order by _cdc_ts desc
        ) as rn
    from raw_orders
)

select
    order_id,
    user_id,
    product_id,
    quantity,
    order_ts,
    toDate(order_ts) as order_date
from ranked
where rn = 1
  and _cdc_op != 'd'
  )
      
      
                    -- end_of_sql
                    
                    