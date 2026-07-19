-- fct_top_failing_businesses.sql
-- Businesses with most failures — useful for risk monitoring

with enriched as (
    select * from {{ ref('int_inspections_enriched') }}
),

business_stats as (
    select
        business_name,
        facility_category,
        risk_level,
        zip_code,
        count(*)                                        as total_inspections,
        sum(passed::integer)                            as total_passed,
        sum((not passed)::integer)                      as total_failed,
        round(
            100.0 * sum(passed::integer) / count(*), 1
        )                                               as pass_rate_pct,
        round(avg(violation_count), 2)                 as avg_violations,
        max(inspection_date)                           as last_inspection_date
    from enriched
    where result in ('Pass', 'Fail')
    group by business_name, facility_category, risk_level, zip_code
    having count(*) >= 3                               -- at least 3 inspections
)

select * from business_stats
order by total_failed desc, pass_rate_pct
limit 50
