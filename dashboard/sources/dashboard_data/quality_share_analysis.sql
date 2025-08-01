-- Question 2: How big is the 'excellent' slice inside those totals?  
-- Simplified query to test column creation
SELECT 
    city,
    CAST(SUM(total_count) AS BIGINT)   AS total_venues,
    AVG(excellence_percentage) as Top_Rated
FROM dashboard_metrics
GROUP BY city
ORDER BY city