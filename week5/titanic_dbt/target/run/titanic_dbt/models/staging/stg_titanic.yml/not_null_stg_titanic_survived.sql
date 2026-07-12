select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select survived
from "delearning"."dbt_dev"."stg_titanic"
where survived is null



      
    ) dbt_internal_test