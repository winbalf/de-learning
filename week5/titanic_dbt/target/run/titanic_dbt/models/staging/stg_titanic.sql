
  create view "delearning"."dbt_dev"."stg_titanic__dbt_tmp"
    
    
  as (
    -- models/staging/stg_titanic.sql
-- Rename columns to snake_case, cast types, handle nulls
-- Materialised as a VIEW — always reflects latest source data

with source as (
    select * from "delearning"."public"."titanic"
),

renamed as (
    select
        -- identifiers
        "PassengerId"                           as passenger_id,
        "Pclass"                                as passenger_class,

        -- passenger info
        "Name"                                  as full_name,
        lower("Sex")                            as gender,
        coalesce("Age", 28.0)                   as age,          -- 28 is median age

        -- survival
        "Survived"                              as survived,
        case
            when "Survived" = 1 then 'survived'
            else 'died'
        end                                     as survival_status,

        -- ticket info
        "Ticket"                                as ticket_number,
        round("Fare"::numeric, 2)               as fare_gbp,

        -- embarkation
        coalesce("Embarked", 'S')               as embarkation_code,
        case
            when "Embarked" = 'C' then 'Cherbourg'
            when "Embarked" = 'Q' then 'Queenstown'
            when "Embarked" = 'S' then 'Southampton'
            else 'Unknown'
        end                                     as embarkation_port,

        -- family
        "SibSp"                                 as siblings_spouses,
        "Parch"                                 as parents_children,
        "SibSp" + "Parch"                       as family_size

    from source
)

select * from renamed
  );