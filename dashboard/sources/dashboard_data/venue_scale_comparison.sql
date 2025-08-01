-- Question 1: Which cities have the most cafés vs. restaurants?
-- Grouped horizontal bar chart showing total count by place type
SELECT 
    city,
    place_type_display as venue_type,
    total_count as count
FROM dashboard_metrics
ORDER BY city, place_type