{{ config(materialized='view') }}

select 
    timestamp,
    query,
    place_data.place_id,
    place_data.formatted_address,
    place_data.geometry.location.lat as latitude,
    place_data.geometry.location.lng as longitude,
    place_data.geometry.location_type,
    place_data.types,
    place_data.address_components,
    api_status,
    date(timestamp) as query_date,
    extract(hour from timestamp) as query_hour
from {{ source('raw_maps', 'raw_maps_data') }}
where api_status = 'OK'
  and place_data is not null