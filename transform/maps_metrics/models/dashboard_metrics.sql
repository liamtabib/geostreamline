{{
  config(
    materialized='table',
    description='Dashboard-optimized metrics with one row per city/place_type combination'
  )
}}

WITH latest_data AS (
  SELECT 
    ingestion_timestamp,
    city,
    place_type,
    rating_filter,
    count
  FROM {{ source('maps_data', 'raw_maps_data') }} raw
  WHERE ingestion_timestamp = (
    SELECT MAX(ingestion_timestamp) 
    FROM {{ source('maps_data', 'raw_maps_data') }} inner_raw
    WHERE inner_raw.city = raw.city
  )
),

aggregated_metrics AS (
  SELECT 
    city,
    place_type,
    MAX(ingestion_timestamp) as ingestion_timestamp,
    SUM(CASE WHEN rating_filter IS NULL THEN count ELSE 0 END) AS total_count,
    SUM(CASE WHEN rating_filter = 4.5 THEN count ELSE 0 END) AS excellent_count
  FROM latest_data
  GROUP BY city, place_type
)

SELECT 
  city,
  place_type,
  ingestion_timestamp,
  total_count,
  excellent_count,
  CASE 
    WHEN total_count > 0 
    THEN ROUND((excellent_count / total_count) * 100, 2)
    ELSE 0 
  END AS excellence_percentage,
  -- Add readable timestamp for dashboard
  TIMESTAMP_MILLIS(CAST(ingestion_timestamp/1000000 AS INT64)) as readable_timestamp,
  -- Add derived fields for easier dashboard filtering
  CASE 
    WHEN place_type = 'cafe' THEN 'Cafes'
    WHEN place_type = 'restaurant' THEN 'Restaurants' 
    ELSE INITCAP(place_type)
  END AS place_type_display,
  -- Add ranking within each place type
  RANK() OVER (PARTITION BY place_type ORDER BY (excellent_count / total_count) DESC) as excellence_rank
FROM aggregated_metrics
WHERE total_count > 0
ORDER BY place_type, excellence_percentage DESC