select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select survived
from "delearning"."dbt_dev"."fct_survival_by_demographics"
where survived is null



      
    ) dbt_internal_test