select 
    place_id,
    formatted_address,
    latitude,
    longitude,
    location_type,
    query,
    timestamp,
    has_city_info,
    country_estimate
from {{ source.bigquery.maps_location_analysis }}