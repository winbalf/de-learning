select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Singular test: survival rate must be between 0 and 100
-- Returns rows that FAIL the test — dbt expects 0 rows to pass

select
    passenger_class,
    survival_rate_pct
from "delearning"."dbt_dev"."fct_survival_by_class"
where survival_rate_pct < 0
   or survival_rate_pct > 100
      
    ) dbt_internal_test