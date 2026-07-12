-- Singular test: survival rate must be between 0 and 100
-- Returns rows that FAIL the test — dbt expects 0 rows to pass

select
    passenger_class,
    survival_rate_pct
from {{ ref('fct_survival_by_class') }}
where survival_rate_pct < 0
   or survival_rate_pct > 100
