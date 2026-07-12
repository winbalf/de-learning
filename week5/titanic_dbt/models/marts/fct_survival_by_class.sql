-- models/marts/fct_survival_by_class.sql
-- Survival statistics by passenger class — the key business question

with passengers as (
    select * from {{ ref('int_passengers_enriched') }}
),

summary as (
    select
        passenger_class,
        class_label,
        count(*)                                                    as total_passengers,
        sum(survived)                                               as total_survived,
        sum(1 - survived)                                           as total_died,
        round(100.0 * sum(survived) / count(*)::numeric, 1)        as survival_rate_pct,
        round(avg(fare_gbp)::numeric, 2)                           as avg_fare_gbp,
        round(avg(age)::numeric, 1)                                as avg_age,
        count(*) filter (where is_solo_traveller)                  as solo_travellers,
        count(*) filter (where gender = 'female')                  as female_passengers,
        count(*) filter (where gender = 'male')                    as male_passengers
    from passengers
    group by passenger_class, class_label
)

select * from summary
order by passenger_class