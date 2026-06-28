
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select * from "iceberg"."seoul_ppltn"."silver_seoul_ppltn"
where false
  
  
      
    ) dbt_internal_test