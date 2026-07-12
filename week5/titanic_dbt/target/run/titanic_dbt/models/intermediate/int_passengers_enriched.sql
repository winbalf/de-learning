
  create view "delearning"."dbt_dev"."int_passengers_enriched__dbt_tmp"
    
    
  as (
    -- models/intermediate/int_passengers_enriched.sql
-- Add derived fields and segments on top of staging

with stg as (
    select * from "delearning"."dbt_dev"."stg_titanic"
),

enriched as (
    select
        *,

        -- Age segments
        case
            when age < 13  then 'child'
            when age < 18  then 'teenager'
            when age < 60  then 'adult'
            else 'senior'
        end                                     as age_group,

        -- Fare segments (quartile-based)
        case
            when fare_gbp < 7.9   then 'budget'
            when fare_gbp < 14.5  then 'economy'
            when fare_gbp < 31.0  then 'standard'
            else 'premium'
        end                                     as fare_tier,

        -- Travel alone flag
        case
            when family_size = 0 then true
            else false
        end                                     as is_solo_traveller,

        -- Class label
        case
            when passenger_class = 1 then 'First'
            when passenger_class = 2 then 'Second'
            else 'Third'
        end                                     as class_label

    from stg
)

select * from enriched
  );