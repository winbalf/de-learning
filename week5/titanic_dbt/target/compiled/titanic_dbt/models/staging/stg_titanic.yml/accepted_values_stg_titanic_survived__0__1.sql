
    
    

with all_values as (

    select
        survived as value_field,
        count(*) as n_records

    from "delearning"."dbt_dev"."stg_titanic"
    group by survived

)

select *
from all_values
where value_field not in (
    '0','1'
)


