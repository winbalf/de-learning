select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        age_group as value_field,
        count(*) as n_records

    from "delearning"."dbt_dev"."int_passengers_enriched"
    group by age_group

)

select *
from all_values
where value_field not in (
    'child','teenager','adult','senior'
)



      
    ) dbt_internal_test