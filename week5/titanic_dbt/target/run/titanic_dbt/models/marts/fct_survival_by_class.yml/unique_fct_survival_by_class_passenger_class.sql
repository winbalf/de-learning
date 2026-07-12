select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    passenger_class as unique_field,
    count(*) as n_records

from "delearning"."dbt_dev"."fct_survival_by_class"
where passenger_class is not null
group by passenger_class
having count(*) > 1



      
    ) dbt_internal_test