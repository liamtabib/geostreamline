SELECT 
    city,
    place_type_display,
    total_count,
    excellent_count,
    excellence_percentage / 100.0 as excellence_percentage,
    excellence_rank
FROM read_csv('/Users/liamtabibzadeh/Documents/hobby/maps-api-pipeline/dashboard/sources/dashboard_data/dashboard_metrics.csv')
ORDER BY excellence_percentage DESC