SELECT 
    city,
    SUM(total_count) as total_places,
    AVG(CASE WHEN place_type = 'cafe' THEN excellence_percentage / 100.0 END) as cafe_excellence,
    AVG(CASE WHEN place_type = 'restaurant' THEN excellence_percentage / 100.0 END) as restaurant_excellence,
    AVG(excellence_percentage / 100.0) as overall_excellence
FROM read_csv('/Users/liamtabibzadeh/Documents/hobby/maps-api-pipeline/dashboard/sources/dashboard_data/dashboard_metrics.csv')
GROUP BY city
ORDER BY overall_excellence DESC