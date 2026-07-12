
  
    

  create  table "delearning"."dbt_dev"."fct_survival_by_demographics__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/fct_survival_by_demographics.sql
-- Survival rates broken down by gender, age group and fare tier

with passengers as (
    select * from "delearning"."dbt_dev"."int_passengers_enriched"
),

by_gender as (
    select
        'gender'                                                    as dimension,
        gender                                                      as segment,
        count(*)                                                    as total,
        sum(survived)                                               as survived,
        round(100.0 * sum(survived) / count(*)::numeric, 1)        as survival_rate_pct
    from passengers
    group by gender
),

by_age_group as (
    select
        'age_group'                                                 as dimension,
        age_group                                                   as segment,
        count(*)                                                    as total,
        sum(survived)                                               as survived,
        round(100.0 * sum(survived) / count(*)::numeric, 1)        as survival_rate_pct
    from passengers
    group by age_group
),

by_fare_tier as (
    select
        'fare_tier'                                                 as dimension,
        fare_tier                                                   as segment,
        count(*)                                                    as total,
        sum(survived)                                               as survived,
        round(100.0 * sum(survived) / count(*)::numeric, 1)        as survival_rate_pct
    from passengers
    group by fare_tier
)

select * from by_gender
union all
select * from by_age_group
union all
select * from by_fare_tier
order by dimension, survival_rate_pct desc
  );
  