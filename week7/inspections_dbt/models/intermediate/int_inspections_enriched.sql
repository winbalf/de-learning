-- int_inspections_enriched.sql

with stg as (
    select * from {{ ref('stg_inspections') }}
),

enriched as (
    select
        *,

        -- Time buckets
        case
            when inspection_year between 2010 and 2014 then '2010-2014'
            when inspection_year between 2015 and 2019 then '2015-2019'
            when inspection_year >= 2020              then '2020+'
            else 'Before 2010'
        end                                            as era,

        -- Facility buckets
        case
            when facility_type ilike '%restaurant%'   then 'Restaurant'
            when facility_type ilike '%school%'        then 'School'
            when facility_type ilike '%grocery%'
              or facility_type ilike '%store%'         then 'Grocery/Store'
            when facility_type ilike '%bakery%'        then 'Bakery'
            when facility_type ilike '%daycare%'
              or facility_type ilike '%children%'      then 'Childcare'
            when facility_type ilike '%hospital%'
              or facility_type ilike '%care%'          then 'Healthcare'
            else 'Other'
        end                                            as facility_category,

        -- High risk flag
        risk_rank = 1                                  as is_high_risk,

        -- Multiple violations flag
        violation_count >= 3                           as has_multiple_violations

    from stg
)

select * from enriched
