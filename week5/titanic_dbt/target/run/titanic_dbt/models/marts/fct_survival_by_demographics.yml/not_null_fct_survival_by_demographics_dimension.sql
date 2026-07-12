select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dimension
from "delearning"."dbt_dev"."fct_survival_by_demographics"
where dimension is null



      
    ) dbt_internal_test