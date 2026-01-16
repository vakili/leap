{{
    config(
        materialized='view'
    )
}}

with age_sex as (
    select
        CENSUS_BLOCK_GROUP,
        "B01001e1" as total_population,
        -- Age 18-54 (prime gym-going age): sum of male and female age groups
        coalesce("B01001e7", 0) + coalesce("B01001e8", 0) + coalesce("B01001e9", 0) +
        coalesce("B01001e10", 0) + coalesce("B01001e11", 0) + coalesce("B01001e12", 0) +
        coalesce("B01001e13", 0) + coalesce("B01001e31", 0) + coalesce("B01001e32", 0) +
        coalesce("B01001e33", 0) + coalesce("B01001e34", 0) + coalesce("B01001e35", 0) +
        coalesce("B01001e36", 0) + coalesce("B01001e37", 0) as pop_age_18_54
    from {{ source('census', 'cbg_b01_2019') }}
),

income as (
    select
        CENSUS_BLOCK_GROUP,
        "B19013e1" as median_household_income
    from {{ source('census', 'cbg_b19_2019') }}
),

employment as (
    select
        CENSUS_BLOCK_GROUP,
        "B23025e3" as employed_population
    from {{ source('census', 'cbg_b23_2019') }}
)

select
    age_sex.CENSUS_BLOCK_GROUP as census_block_group,
    age_sex.total_population,
    age_sex.pop_age_18_54,
    income.median_household_income,
    employment.employed_population,
    -- Calculate percentage of population in prime gym age
    case
        when age_sex.total_population > 0
        then (age_sex.pop_age_18_54::float / age_sex.total_population::float) * 100
        else 0
    end as pct_prime_gym_age
from age_sex
left join income
    on age_sex.CENSUS_BLOCK_GROUP = income.CENSUS_BLOCK_GROUP
left join employment
    on age_sex.CENSUS_BLOCK_GROUP = employment.CENSUS_BLOCK_GROUP
