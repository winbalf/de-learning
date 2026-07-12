
    
    

select
    passenger_id as unique_field,
    count(*) as n_records

from "delearning"."dbt_dev"."stg_titanic"
where passenger_id is not null
group by passenger_id
having count(*) > 1


