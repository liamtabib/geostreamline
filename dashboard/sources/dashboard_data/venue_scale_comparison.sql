-- Question 1: Which cities have the most cafés vs. restaurants?
-- Grouped horizontal bar chart showing total count by place type
SELECT 
    city,
    place_type_display as venue_type,
    total_count as count
FROM read_csv('/Users/liamtabibzadeh/Documents/hobby/maps-api-pipeline/dashboard/sources/dashboard_data/dashboard_metrics.csv')
ORDER BY city, place_type