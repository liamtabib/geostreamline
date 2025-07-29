SELECT 
    'Last Updated' as metric,
    CAST(MAX(readable_timestamp) AS VARCHAR) as value
FROM read_csv('/Users/liamtabibzadeh/Documents/hobby/maps-api-pipeline/dashboard/sources/dashboard_data/dashboard_metrics.csv')
UNION ALL
SELECT 
    'Total Cities' as metric,
    CAST(COUNT(DISTINCT city) AS VARCHAR) as value
FROM read_csv('/Users/liamtabibzadeh/Documents/hobby/maps-api-pipeline/dashboard/sources/dashboard_data/dashboard_metrics.csv')
UNION ALL
SELECT 
    'Total Records' as metric,
    CAST(COUNT(*) AS VARCHAR) as value
FROM read_csv('/Users/liamtabibzadeh/Documents/hobby/maps-api-pipeline/dashboard/sources/dashboard_data/dashboard_metrics.csv')