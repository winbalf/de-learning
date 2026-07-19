-- fct_inspection_summary.sql
-- Pass/fail rates by facility category and risk level

with enriched as (
    select * from {{ ref('int_inspections_enriched') }}
),

summary as (
    select
        facility_category,
        risk_level,
        risk_rank,
        count(*)                                        as total_inspections,
        sum(passed::integer)                            as total_passed,
        sum((not passed)::integer)                      as total_failed,
        round(
            100.0 * sum(passed::integer) / count(*), 1
        )                                               as pass_rate_pct,
        round(avg(violation_count), 2)                 as avg_violations,
        max(violation_count)                            as max_violations
    from enriched
    where result in ('Pass', 'Fail')
    group by facility_category, risk_level, risk_rank
)

select * from summary
order by risk_rank, pass_rate_pct
