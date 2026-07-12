select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_passengers
from "delearning"."dbt_dev"."fct_survival_by_class"
where total_passengers is null



      
    ) dbt_internal_test