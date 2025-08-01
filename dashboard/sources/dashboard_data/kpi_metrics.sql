-- KPI metrics for dashboard cards
WITH base_data AS (
    SELECT * FROM dashboard_metrics
),
metrics AS (
    SELECT 
        COUNT(DISTINCT city) as total_cities,
        SUM(CASE WHEN place_type = 'cafe' THEN total_count ELSE 0 END) as total_cafes,
        SUM(CASE WHEN place_type = 'restaurant' THEN total_count ELSE 0 END) as total_restaurants,
        ROUND(AVG(CASE WHEN place_type = 'cafe' THEN excellence_percentage END), 1) as avg_cafe_top_rated,
        ROUND(AVG(CASE WHEN place_type = 'restaurant' THEN excellence_percentage END), 1) as avg_restaurant_top_rated,
        MAX(readable_timestamp) as last_updated
    FROM base_data
)
SELECT 
    total_cities,
    total_cafes,
    total_restaurants,
    avg_cafe_top_rated,
    avg_restaurant_top_rated,
    STRFTIME('%B %d, %Y', CAST(last_updated AS TIMESTAMP)) as last_updated_formatted
FROM metrics