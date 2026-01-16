{{
    config(
        materialized='view'
    )
}}

with census_with_demo as (
    select
        cb.census_block_group,
        cb.state,
        cb.county,
        cb.geography,
        cb.geometry_json,
        dm.total_population,
        dm.pop_age_18_54,
        dm.pct_prime_gym_age,
        dm.median_household_income,
        dm.employed_population
    from {{ ref('stg_sf_census_blocks') }} cb
    left join {{ ref('stg_sf_demographics') }} dm
        on cb.census_block_group = dm.census_block_group
)

select
    census_block_group,
    state,
    county,
    geography,
    geometry_json,
    total_population,
    pop_age_18_54,
    pct_prime_gym_age,
    median_household_income,
    employed_population,

    -- Calculate demand score (higher = more demand for gym)
    -- Weighted formula:
    -- - 40% weight: population aged 18-54
    -- - 30% weight: median household income (normalized)
    -- - 30% weight: employed population
    (
        coalesce(pop_age_18_54, 0) * 0.4 +
        coalesce(median_household_income, 0) / 1000 * 0.3 +
        coalesce(employed_population, 0) * 0.3
    ) as demand_score,

    -- Flag high-demand areas
    case
        when pop_age_18_54 > 500
            and median_household_income > 75000
            and employed_population > 300
        then true
        else false
    end as is_high_demand_area

from census_with_demo
where total_population > 0  -- Exclude unpopulated areas
