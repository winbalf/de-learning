select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        embarkation_code as value_field,
        count(*) as n_records

    from "delearning"."dbt_dev"."stg_titanic"
    group by embarkation_code

)

select *
from all_values
where value_field not in (
    'C','Q','S'
)



      
    ) dbt_internal_test