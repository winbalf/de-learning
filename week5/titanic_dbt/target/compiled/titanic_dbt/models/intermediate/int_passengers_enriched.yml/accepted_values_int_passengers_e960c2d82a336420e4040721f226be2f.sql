
    
    

with all_values as (

    select
        fare_tier as value_field,
        count(*) as n_records

    from "delearning"."dbt_dev"."int_passengers_enriched"
    group by fare_tier

)

select *
from all_values
where value_field not in (
    'budget','economy','standard','premium'
)


