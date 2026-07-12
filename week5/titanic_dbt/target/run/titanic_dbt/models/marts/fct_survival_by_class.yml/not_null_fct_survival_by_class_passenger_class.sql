select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select passenger_class
from "delearning"."dbt_dev"."fct_survival_by_class"
where passenger_class is null



      
    ) dbt_internal_test