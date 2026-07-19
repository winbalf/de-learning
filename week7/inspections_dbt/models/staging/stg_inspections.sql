-- stg_inspections.sql
-- Clean types, handle nulls, standardise values

with source as (
    select * from {{ source('raw', 'raw_inspections') }}
),

cleaned as (
    select
        inspection_id::integer                          as inspection_id,
        upper(trim(dba_name))                          as business_name,
        upper(trim(coalesce(aka_name, dba_name)))      as aka_name,
        license::bigint                                as license_number,
        initcap(trim(coalesce(facility_type,
            'Unknown')))                               as facility_type,

        -- Standardise risk levels
        case
            when risk ilike '%1%' then 'High'
            when risk ilike '%2%' then 'Medium'
            when risk ilike '%3%' then 'Low'
            else 'Unknown'
        end                                            as risk_level,

        case
            when risk ilike '%1%' then 1
            when risk ilike '%2%' then 2
            when risk ilike '%3%' then 3
            else 99
        end                                            as risk_rank,

        trim(address)                                  as address,
        trim(coalesce(city, 'CHICAGO'))                as city,
        trim(coalesce(state, 'IL'))                    as state,
        trim(zip)                                      as zip_code,

        -- Parse date
        to_date(inspection_date, 'MM/DD/YYYY')         as inspection_date,
        extract(year from
            to_date(inspection_date, 'MM/DD/YYYY'))
            ::integer                                  as inspection_year,
        extract(month from
            to_date(inspection_date, 'MM/DD/YYYY'))
            ::integer                                  as inspection_month,

        trim(inspection_type)                          as inspection_type,

        -- Standardise results
        case
            when results ilike '%pass%'   then 'Pass'
            when results ilike '%fail%'   then 'Fail'
            when results ilike '%no entry%' then 'No Entry'
            when results ilike '%out of business%' then 'Out of Business'
            else 'Other'
        end                                            as result,

        case
            when results ilike '%pass%' then true
            else false
        end                                            as passed,

        violations                                     as violations_raw,
        case
            when violations is null or trim(violations) = ''
            then 0
            else array_length(
                string_to_array(violations, '|'), 1
            )
        end                                            as violation_count,

        latitude::numeric                              as latitude,
        longitude::numeric                             as longitude,

        _ingested_at,
        _source_file

    from source
    where inspection_id is not null
)

select * from cleaned
