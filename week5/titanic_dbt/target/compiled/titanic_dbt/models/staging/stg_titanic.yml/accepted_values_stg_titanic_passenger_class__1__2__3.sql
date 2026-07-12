
    
    

with all_values as (

    select
        passenger_class as value_field,
        count(*) as n_records

    from "delearning"."dbt_dev"."stg_titanic"
    group by passenger_class

)

select *
from all_values
where value_field not in (
    '1','2','3'
)


