{{ config(materialized='table') }}

select 
    query_date,
    query,
    count(*) as total_results,
    count(distinct place_id) as unique_places,
    avg(latitude) as avg_latitude,
    avg(longitude) as avg_longitude,
    min(timestamp) as first_query_time,
    max(timestamp) as last_query_time
from {{ ref('stg_maps_data') }}
group by query_date, query
order by query_date desc, query