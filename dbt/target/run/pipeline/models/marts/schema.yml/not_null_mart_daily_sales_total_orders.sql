
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_orders
from `mart`.`mart_daily_sales`
where total_orders is null



    ) dbt_internal_test