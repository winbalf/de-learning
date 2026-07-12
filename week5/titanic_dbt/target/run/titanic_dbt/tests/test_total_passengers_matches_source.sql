select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Singular test: total passengers in mart must match source row count
-- Any non-zero result means data was lost in transformation

with source_count as (
    select count(*) as cnt from "delearning"."public"."titanic"
),
mart_count as (
    select sum(total_passengers) as cnt from "delearning"."dbt_dev"."fct_survival_by_class"
)
select
    source_count.cnt as source_rows,
    mart_count.cnt   as mart_rows
from source_count, mart_count
where source_count.cnt != mart_count.cnt
      
    ) dbt_internal_test