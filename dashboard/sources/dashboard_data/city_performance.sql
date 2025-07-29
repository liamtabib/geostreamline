SELECT 
    city,
    AVG(excellence_percentage / 100.0) as overall_excellence
FROM read_csv('/Users/liamtabibzadeh/Documents/hobby/maps-api-pipeline/dashboard/sources/dashboard_data/dashboard_metrics.csv')
GROUP BY city
ORDER BY overall_excellence DESC