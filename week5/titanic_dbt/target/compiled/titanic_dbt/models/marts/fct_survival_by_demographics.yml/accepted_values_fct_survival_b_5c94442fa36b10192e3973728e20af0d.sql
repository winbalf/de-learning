
    
    

with all_values as (

    select
        dimension as value_field,
        count(*) as n_records

    from "delearning"."dbt_dev"."fct_survival_by_demographics"
    group by dimension

)

select *
from all_values
where value_field not in (
    'gender','age_group','fare_tier'
)


