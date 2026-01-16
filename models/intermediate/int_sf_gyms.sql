{{
    config(
        materialized='view'
    )
}}

with gyms as (
    select
        place_id,
        place_name,
        coalesce(common_name, place_name) as display_name,
        primary_category,
        basic_category,
        address,
        phone,
        website,
        operating_status,
        confidence_score,
        geography,
        longitude,
        latitude
    from {{ ref('stg_sf_places') }}
    where (
        lower(basic_category) in ('gym', 'fitness_studio', 'sport_fitness_facility')
        or lower(primary_category) like '%fitness%'
        or lower(primary_category) like '%gym%'
    )
    -- Only include open or unknown status (exclude definitively closed)
    and (
        operating_status is null
        or lower(operating_status) != 'closed'
    )
)

select
    place_id,
    display_name,
    basic_category,
    primary_category,
    address,
    phone,
    website,
    operating_status,
    confidence_score,
    geography,
    longitude,
    latitude,
    -- Categorize gym type for analysis
    case
        when lower(basic_category) = 'gym' then 'Traditional Gym'
        when lower(basic_category) = 'fitness_studio' then 'Boutique Fitness'
        when lower(basic_category) = 'sport_fitness_facility' then 'Sports Facility'
        else 'Other Fitness'
    end as gym_type
from gyms
