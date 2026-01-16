{{
    config(
        materialized='table'
    )
}}

with demand_areas as (
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
        demand_score,
        is_high_demand_area
    from {{ ref('int_sf_demand_metrics') }}
),

gyms as (
    select
        place_id,
        display_name,
        gym_type,
        geography,
        longitude,
        latitude
    from {{ ref('int_sf_gyms') }}
),

-- Calculate gym accessibility for each census block
gym_accessibility as (
    select
        d.census_block_group,
        any_value(d.state) as state,
        any_value(d.county) as county,
        any_value(d.geometry_json) as geometry_json,
        any_value(d.total_population) as total_population,
        any_value(d.pop_age_18_54) as pop_age_18_54,
        any_value(d.pct_prime_gym_age) as pct_prime_gym_age,
        any_value(d.median_household_income) as median_household_income,
        any_value(d.employed_population) as employed_population,
        any_value(d.demand_score) as demand_score,
        any_value(d.is_high_demand_area) as is_high_demand_area,

        -- Count gyms within 1 mile (1609 meters) from census block centroid
        count(distinct case
            when st_dwithin(st_centroid(d.geography), g.geography, 1609)
            then g.place_id
        end) as gyms_within_1_mile,

        -- Count gyms within 0.5 miles (804 meters) from census block centroid
        count(distinct case
            when st_dwithin(st_centroid(d.geography), g.geography, 804)
            then g.place_id
        end) as gyms_within_half_mile,

        -- Find distance to nearest gym from centroid
        min(st_distance(st_centroid(d.geography), g.geography)) as distance_to_nearest_gym_meters,

        -- Get nearest gym details
        array_agg(
            object_construct(
                'name', g.display_name,
                'type', g.gym_type,
                'distance_meters', st_distance(st_centroid(d.geography), g.geography)
            )
        ) within group (order by st_distance(st_centroid(d.geography), g.geography)) as nearest_gyms

    from demand_areas d
    cross join gyms g
    group by d.census_block_group
)

select
    ga.census_block_group,
    ga.state,
    ga.county,
    d.geography,
    ga.geometry_json,
    ga.total_population,
    ga.pop_age_18_54,
    ga.pct_prime_gym_age,
    ga.median_household_income,
    ga.employed_population,
    ga.demand_score,
    ga.is_high_demand_area,
    ga.gyms_within_1_mile,
    ga.gyms_within_half_mile,
    ga.distance_to_nearest_gym_meters,
    round(ga.distance_to_nearest_gym_meters * 0.000621371, 2) as distance_to_nearest_gym_miles,
    ga.nearest_gyms[0] as nearest_gym,

    -- Calculate accessibility score (lower is worse)
    -- More gyms nearby = better score
    -- Closer gyms = better score
    case
        when ga.gyms_within_half_mile >= 3 then 'Excellent'
        when ga.gyms_within_half_mile >= 1 then 'Good'
        when ga.gyms_within_1_mile >= 2 then 'Fair'
        when ga.gyms_within_1_mile >= 1 then 'Poor'
        else 'Very Poor'
    end as accessibility_rating,

    -- Identify underserved areas (high demand + poor accessibility)
    case
        when ga.is_high_demand_area
            and ga.gyms_within_half_mile = 0
            and ga.gyms_within_1_mile <= 1
        then true
        else false
    end as is_underserved,

    -- Priority score for new gym location
    -- Higher score = better opportunity
    case
        when ga.is_high_demand_area and ga.gyms_within_half_mile = 0
        then ga.demand_score * 2.0  -- Double weight for high demand + no nearby gyms
        when ga.gyms_within_1_mile <= 1
        then ga.demand_score * 1.5  -- 1.5x weight for low gym density
        else ga.demand_score
    end as opportunity_score,

    -- Assign opportunity tier for visualization
    -- More balanced distribution based on both demand and gym accessibility
    case
        -- High Opportunity: Good demand + low gym coverage
        when ga.demand_score >= 450 and ga.gyms_within_half_mile <= 2 then 'High Opportunity'
        -- Medium Opportunity: Decent demand + moderate gym coverage
        when (ga.demand_score >= 350 and ga.gyms_within_half_mile <= 5)
            or (ga.demand_score >= 550 and ga.gyms_within_half_mile <= 8) then 'Medium Opportunity'
        -- Saturated: Many gyms nearby
        when ga.gyms_within_half_mile >= 15 then 'Saturated'
        -- Low Priority: Everything else
        else 'Low Priority'
    end as opportunity_tier

from gym_accessibility ga
join demand_areas d on ga.census_block_group = d.census_block_group
order by opportunity_score desc
