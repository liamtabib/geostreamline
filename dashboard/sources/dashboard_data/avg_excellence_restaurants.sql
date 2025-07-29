SELECT AVG(excellence_percentage / 100.0) as avg_excellence_percentage
FROM read_csv('/Users/liamtabibzadeh/Documents/hobby/maps-api-pipeline/dashboard/sources/dashboard_data/dashboard_metrics.csv')
WHERE place_type = 'restaurant'