{{ config(materialized='table') }}

select 
    place_id,
    formatted_address,
    latitude,
    longitude,
    location_type,
    query,
    timestamp,
    
    -- Extract city/state from address components if available
    case 
        when place_data.address_components like '%locality%' then 'City Found'
        else 'No City'
    end as has_city_info,
    
    case 
        when latitude between 24.396308 and 49.384358 
         and longitude between -125.0 and -66.93457 then 'USA'
        else 'Other/Unknown'
    end as country_estimate

from {{ ref('stg_maps_data') }}
where place_id is not null
order by timestamp desc