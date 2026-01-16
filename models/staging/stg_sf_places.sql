{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select
        ID,
        GEOMETRY,
        NAMES,
        CATEGORIES,
        BASIC_CATEGORY,
        ADDRESSES,
        PHONES,
        WEBSITES,
        OPERATING_STATUS,
        CONFIDENCE
    from {{ source('overture', 'PLACE') }}
    where
        -- Use bounding box for San Francisco (city limits)
        st_x(GEOMETRY) between -122.52 and -122.35
        and st_y(GEOMETRY) between 37.70 and 37.84
)

select
    ID as place_id,
    GEOMETRY as geography,
    NAMES:primary::string as place_name,
    NAMES:common[0]:primary::string as common_name,
    CATEGORIES:primary::string as primary_category,
    BASIC_CATEGORY as basic_category,
    ADDRESSES[0]:freeform::string as address,
    PHONES[0]::string as phone,
    WEBSITES[0]::string as website,
    OPERATING_STATUS as operating_status,
    CONFIDENCE as confidence_score,
    -- Extract lat/lon from GEOGRAPHY point
    st_x(GEOMETRY) as longitude,
    st_y(GEOMETRY) as latitude
from source_data
