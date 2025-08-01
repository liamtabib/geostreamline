-- Question 2: How big is the 'excellent' slice inside those totals?  
-- Simplified query to test column creation
SELECT 
    city,
    CAST(SUM(total_count) AS BIGINT)   AS total_venues,
    AVG(excellence_percentage) as Top_Rated
FROM read_csv('/Users/liamtabibzadeh/Documents/hobby/maps-api-pipeline/dashboard/sources/dashboard_data/dashboard_metrics.csv')
GROUP BY city
ORDER BY city