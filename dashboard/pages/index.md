---
sidebar: never
hide_header: true
title: "KPI Dashboard"
---

# Maps Excellence Dashboard

Tracking excellence metrics for cafes and restaurants across European cities using Google Area Insights API data.

<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
<BigValue 
    title='Cities Analyzed'
    data={city_count} 
    value='total_cities' 
    fmt='#,##0'	
/>

<BigValue 
    title='Total Places'
    data={total_places} 
    value='total_count' 
    fmt='#,##0'	
/>

<BigValue 
    title='Average Excellence'
    data={avg_excellence} 
    value='avg_excellence_percentage' 
    fmt='#0.0%'	
/>

<BigValue 
  title='Last Updated'
  data={last_updated} 
  value=readable_timestamp
/>
</div>

## Excellence Rankings

<div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
<div class="chart-container">
<BarChart 
    data={cafe_rankings}
    x=city
    y=excellence_percentage
    title="Cafe Excellence by City"
    yFmt='#0.0%'
    swapXY=true
/>
</div>

<div class="chart-container">
<BarChart 
    data={restaurant_rankings}
    x=city
    y=excellence_percentage
    title="Restaurant Excellence by City"
    yFmt='#0.0%'
    swapXY=true
/>
</div>
</div>

## Detailed Analysis

<div class="dashboard-section">
<DataTable 
  data={detailed_metrics} 
  search=true
  class="evidence-table"
>
    <Column id="city" title="City"/>
    <Column id="place_type_display" title="Type"/>
    <Column id="total_count" title="Total Places" fmt='#,##0'/>
    <Column id="excellent_count" title="Excellent Places" fmt='#,##0'/>
    <Column id="excellence_percentage" title="Excellence %" fmt='#0.0%'/>
    <Column id="excellence_rank" title="Rank"/>
</DataTable>
</div>

```sql city_count
SELECT COUNT(DISTINCT city) as total_cities
FROM dashboard_data.dashboard_metrics
```

```sql total_places
SELECT SUM(total_count) as total_count
FROM dashboard_data.dashboard_metrics
```

```sql avg_excellence
SELECT AVG(excellence_percentage / 100.0) as avg_excellence_percentage
FROM dashboard_data.dashboard_metrics
WHERE excellence_percentage IS NOT NULL
```

```sql last_updated
SELECT MAX(readable_timestamp) as readable_timestamp
FROM dashboard_data.dashboard_metrics
```

```sql cafe_rankings
SELECT 
    city,
    excellence_percentage / 100.0 as excellence_percentage,
    excellence_rank
FROM dashboard_data.dashboard_metrics
WHERE place_type = 'cafe'
ORDER BY excellence_percentage DESC
```

```sql restaurant_rankings
SELECT 
    city,
    excellence_percentage / 100.0 as excellence_percentage,
    excellence_rank
FROM dashboard_data.dashboard_metrics
WHERE place_type = 'restaurant'
ORDER BY excellence_percentage DESC
```

```sql detailed_metrics
SELECT 
    city,
    place_type_display,
    total_count,
    excellent_count,
    excellence_percentage / 100.0 as excellence_percentage,
    excellence_rank
FROM dashboard_data.dashboard_metrics
ORDER BY place_type, excellence_percentage DESC
```

```sql scatter_data
SELECT 
    city,
    place_type_display,
    total_count,
    excellence_percentage / 100.0 as excellence_percentage
FROM dashboard_data.dashboard_metrics
```

```sql city_comparison
SELECT 
    city,
    place_type_display,
    excellence_percentage / 100.0 as excellence_percentage
FROM dashboard_data.dashboard_metrics
ORDER BY city, place_type
```

---

**Excellence Definition**: Places rated 4.5+ stars out of total places found  
**Cities**: Helsinki, Stockholm, Copenhagen, Berlin, London, Amsterdam  
**Data Source**: Google Area Insights API • Updated automatically