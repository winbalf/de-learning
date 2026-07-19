-- fct_yearly_trends.sql
-- Inspection trends over time

with enriched as (
    select * from {{ ref('int_inspections_enriched') }}
),

trends as (
    select
        inspection_year,
        count(*)                                        as total_inspections,
        sum(passed::integer)                            as total_passed,
        round(
            100.0 * sum(passed::integer) / count(*), 1
        )                                               as pass_rate_pct,
        round(avg(violation_count), 2)                 as avg_violations,
        count(distinct business_name)                  as unique_businesses,
        sum((risk_rank = 1)::integer)                  as high_risk_inspections
    from enriched
    where result in ('Pass', 'Fail')
      and inspection_year between 2010 and 2024
    group by inspection_year
)

select * from trends
order by inspection_year
