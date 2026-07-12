select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select passenger_id
from "delearning"."dbt_dev"."stg_titanic"
where passenger_id is null



      
    ) dbt_internal_test