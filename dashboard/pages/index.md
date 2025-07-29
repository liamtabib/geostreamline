---
title: Cafés ☕ - Restaurants 🍽️ in European cities
---

## How many Cafés & Restaurants in 6 capital cities?

<BigValue 
    data={cafe_places} 
    value='total_count' 
    title="Café Places"
    fmt='#,##0'	
/>

<BigValue 
    data={avg_excellence_cafes} 
    value='avg_excellence_percentage' 
    title="Excellent cafés"
    fmt='#0.0%'	
/>

<BigValue 
    data={restaurant_places} 
    value='total_count' 
    title="Restaurant Places"
    fmt='#,##0'	
/>

<BigValue 
    data={avg_excellence_restaurants} 
    value='avg_excellence_percentage' 
    title="Excellent restaurants"
    fmt='#0.0%'	
/>

## Cafés by City

<Grid cols=2>
<BarChart 
    data={cafe_rankings}
    x=city
    y=excellence_percentage
    title="Café Excellence by City"
    yFmt='#0.0%'
    swapXY=true
/>
<BarChart 
    data={restaurant_rankings}
    x=city
    y=excellence_percentage
    title="Restaurant Excellence by City"
    yFmt='#0.0%'
    swapXY=true
/>
</Grid>


## City Performance Analysis

<BarChart 
    data={city_performance}
    x=city
    y=overall_excellence
    title="Overall Excellence by City"
    yFmt='#0.0%'
    swapXY=true
/>

## City Comparison

<DataTable 
  data={city_comparison} 
  search=true
>
    <Column id="city" title="City"/>
    <Column id="total_places" title="Total Places" fmt='#,##0'/>
    <Column id="cafe_excellence" title="Café Excellence" fmt='#0.0%'/>
    <Column id="restaurant_excellence" title="Restaurant Excellence" fmt='#0.0%'/>
    <Column id="overall_excellence" title="Overall Excellence" fmt='#0.0%'/>
</DataTable>

## Detailed Breakdown

<DataTable 
  data={detailed_metrics} 
  search=true
>
    <Column id="city" title="City"/>
    <Column id="place_type_display" title="Type"/>
    <Column id="total_count" title="Total Places" fmt='#,##0'/>
    <Column id="excellent_count" title="Excellent Places" fmt='#,##0'/>
    <Column id="excellence_percentage" title="Excellence %" fmt='#0.0%'/>
    <Column id="excellence_rank" title="Rank"/>
</DataTable>

## About This Dashboard

### Excellence Definition
Places rated **4.5+ stars** out of total places found in each city.

### Data Sources
- **Google Places API**: Real-time business data and ratings
- **Cities Covered**: Helsinki, Stockholm, Copenhagen, Berlin, London, Amsterdam
- **Place Types**: Cafés and Restaurants

### Technology Stack
- **Data Pipeline**: [Dagster](https://dagster.io) for orchestration
- **Data Storage**: [Google BigQuery](https://cloud.google.com/bigquery)
- **Analytics**: [DuckDB](https://duckdb.org) for fast queries
- **Dashboard**: [Evidence](https://evidence.dev) for visualization
- **Transformation**: [dbt](https://www.getdbt.com) for data modeling

## Data Freshness

<DataTable 
  data={data_freshness} 
>
    <Column id="metric" title="Metric"/>
    <Column id="value" title="Value"/>
</DataTable>


```sql cafe_places
SELECT SUM(total_count) as total_count
FROM dashboard_data.dashboard_metrics
WHERE place_type = 'cafe'
```

```sql restaurant_places
SELECT SUM(total_count) as total_count
FROM dashboard_data.dashboard_metrics
WHERE place_type = 'restaurant'
```

```sql avg_excellence_restaurants
SELECT AVG(excellence_percentage / 100.0) as avg_excellence_percentage
FROM dashboard_data.dashboard_metrics
WHERE excellence_percentage IS NOT NULL
and place_type = 'restaurant'
```

```sql avg_excellence_cafes
SELECT AVG(excellence_percentage / 100.0) as avg_excellence_percentage
FROM dashboard_data.dashboard_metrics
WHERE excellence_percentage IS NOT NULL
and place_type = 'cafe'
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

```sql city_performance
SELECT 
    city,
    AVG(excellence_percentage / 100.0) as overall_excellence
FROM dashboard_data.dashboard_metrics
GROUP BY city
ORDER BY overall_excellence DESC
```

```sql city_comparison
SELECT 
    city,
    SUM(total_count) as total_places,
    AVG(CASE WHEN place_type = 'cafe' THEN excellence_percentage / 100.0 END) as cafe_excellence,
    AVG(CASE WHEN place_type = 'restaurant' THEN excellence_percentage / 100.0 END) as restaurant_excellence,
    AVG(excellence_percentage / 100.0) as overall_excellence
FROM dashboard_data.dashboard_metrics
GROUP BY city
ORDER BY overall_excellence DESC
```

```sql data_freshness
SELECT 
    'Last Updated' as metric,
    MAX(readable_timestamp) as value
FROM dashboard_data.dashboard_metrics
UNION ALL
SELECT 
    'Total Data Points' as metric,
    COUNT(*)::text as value
FROM dashboard_data.dashboard_metrics
UNION ALL
SELECT 
    'Cities Analyzed' as metric,
    COUNT(DISTINCT city)::text as value
FROM dashboard_data.dashboard_metrics
```

---

**Excellence Definition**: Places rated 4.5+ stars out of total places found  
**Cities**: Helsinki, Stockholm, Copenhagen, Berlin, London, Amsterdam  
**Data Source**: Google Area Insights API • Updated automatically