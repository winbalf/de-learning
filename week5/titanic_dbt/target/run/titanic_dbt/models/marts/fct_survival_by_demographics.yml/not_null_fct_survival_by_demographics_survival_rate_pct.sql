select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select survival_rate_pct
from "delearning"."dbt_dev"."fct_survival_by_demographics"
where survival_rate_pct is null



      
    ) dbt_internal_test