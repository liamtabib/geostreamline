select 
    query_date,
    query,
    total_results,
    unique_places,
    avg_latitude,
    avg_longitude,
    first_query_time,
    last_query_time
from {{ source.bigquery.maps_daily_summary }}