SELECT SUM(total_count) as total_count
FROM read_csv('/Users/liamtabibzadeh/Documents/hobby/maps-api-pipeline/dashboard/sources/dashboard_data/dashboard_metrics.csv')
WHERE place_type = 'restaurant'