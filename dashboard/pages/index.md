---
title: Google Maps Pipeline Dashboard 📍
---

# Google Maps Pipeline Dashboard 

This dashboard shows insights from Google Maps API geocoding data processed through our data pipeline.

## Query Summary

<BigValue 
    title='Total Queries Today'
    data={queries_today} 
    value='total_queries' 
    fmt='#,##0'	
/>

<BigValue 
    title='Unique Places Found'
    data={unique_places} 
    value='place_count' 
    fmt='#,##0'	
/>

<BigValue 
    title='API Success Rate'
    data={api_success_rate} 
    value='success_rate' 
    fmt='#0.0%'	
/>

<BigValue 
  title='Data last updated'
  data={last_updated} 
  value=max_timestamp
/>

## Query Activity Over Time

<LineChart 
  data={daily_activity} 
  x=query_date 
  y=total_results
  series=query
  title="Daily Query Results by Search Term"
/>

## Geographic Distribution

<ScatterPlot 
  data={location_data} 
  x=longitude 
  y=latitude 
  series=query
  size=10
  title="Geographic Distribution of Results"
/>

## Query Performance

<Grid cols=2>
<DataTable 
  data={query_summary} 
  title="Query Summary"
  search=false
>
    <Column id="query" title="Search Query"/>
    <Column id="total_results" title="Total Results" />
    <Column id="unique_places" title="Unique Places" />
    <Column id="avg_latitude" title="Avg Latitude" fmt="#0.000"/>
    <Column id="avg_longitude" title="Avg Longitude" fmt="#0.000"/>
</DataTable>

<BarChart 
    data={query_summary}
    x=query
    y=total_results
    title="Results by Query"
    swapXY=true
/>
</Grid>

## Recent Locations

<DataTable 
  data={recent_locations} 
  rows=20
  title="Latest Geocoded Locations"
>
    <Column id="query" title="Query"/>
    <Column id="formatted_address" title="Address"/>
    <Column id="latitude" title="Latitude" fmt="#0.0000"/>
    <Column id="longitude" title="Longitude" fmt="#0.0000"/>
    <Column id="timestamp" title="Timestamp"/>
</DataTable>

```sql queries_today
SELECT COUNT(*) as total_queries
FROM maps_daily_summary
WHERE query_date = CURRENT_DATE()
```

```sql unique_places
SELECT COUNT(DISTINCT place_id) as place_count
FROM maps_location_analysis
```

```sql api_success_rate
SELECT 
    COUNT(CASE WHEN place_id IS NOT NULL THEN 1 END) * 1.0 / COUNT(*) as success_rate
FROM maps_location_analysis
```

```sql last_updated
SELECT MAX(timestamp) as max_timestamp
FROM maps_location_analysis
```

```sql daily_activity
SELECT 
    query_date,
    query,
    total_results
FROM maps_daily_summary
ORDER BY query_date DESC
LIMIT 30
```

```sql location_data
SELECT 
    latitude,
    longitude,
    query,
    formatted_address
FROM maps_location_analysis
WHERE latitude IS NOT NULL 
  AND longitude IS NOT NULL
```

```sql query_summary
SELECT 
    query,
    total_results,
    unique_places,
    avg_latitude,
    avg_longitude
FROM maps_daily_summary
WHERE query_date >= CURRENT_DATE() - INTERVAL '7 days'
ORDER BY total_results DESC
```

```sql recent_locations
SELECT 
    query,
    formatted_address,
    latitude,
    longitude,
    timestamp
FROM maps_location_analysis
WHERE place_id IS NOT NULL
ORDER BY timestamp DESC
LIMIT 50
```

## About This Pipeline

This dashboard is powered by a modern data stack:
- **Google Maps API** for geocoding data
- **Google Cloud Storage** for data lake storage  
- **BigQuery** for data warehousing
- **dbt** for data transformations
- **Dagster** for orchestration
- **Evidence** for visualization

The pipeline runs daily to collect fresh geocoding data and update these insights.

*Pipeline Dashboard v1.0*