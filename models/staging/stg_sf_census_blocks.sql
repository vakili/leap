{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select
        CENSUS_BLOCK_GROUP,
        STATE,
        COUNTY,
        STATE_FIPS,
        COUNTY_FIPS,
        TRACT_CODE,
        BLOCK_GROUP,
        GEOMETRY
    from {{ source('census', 'cbg_geometry_2019') }}
    where STATE = 'CA'
        and COUNTY = 'San Francisco County'
)

select
    CENSUS_BLOCK_GROUP as census_block_group,
    STATE as state,
    COUNTY as county,
    STATE_FIPS as state_fips,
    COUNTY_FIPS as county_fips,
    TRACT_CODE as tract_code,
    BLOCK_GROUP as block_group,
    GEOMETRY as geometry_json,
    -- Convert GeoJSON to Snowflake GEOGRAPHY
    try_to_geography(GEOMETRY) as geography
from source_data
