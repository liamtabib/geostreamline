---
title: Cafés and Restaurants in North European Cities
---


```sql kpi_data
select * from dashboard_data.kpi_metrics
```

<div class="grid grid-cols-3 gap-4 mb-8">

<BigValue 
    data={kpi_data} 
    value=total_cities
    title="Cities"
/>

<BigValue 
    data={kpi_data} 
    value=total_cafes
    title="Total Cafés"
    fmt='#,##0'
/>

<BigValue 
    data={kpi_data} 
    value=total_restaurants
    title="Total Restaurants"
    fmt='#,##0'
/>

<BigValue 
    data={kpi_data} 
    value=last_updated_formatted
    title="Data last updated on"
/>

<BigValue 
    data={kpi_data} 
    value=avg_cafe_top_rated
    title="Top-Rated Cafés %"
    fmt='#,##0.0"%"'
/>

<BigValue 
    data={kpi_data} 
    value=avg_restaurant_top_rated
    title="Top-Rated Restaurants %"  
    fmt='#,##0.0"%"'
/>

</div>


*Top-Rated: Proportion of venues with an average Google rating of 4.5 stars or higher.*

## Number of Cafés and Restaurants

```sql venue_scale
select * from dashboard_data.venue_scale_comparison
```

<BarChart 
    data={venue_scale}
    x=city
    y=count
    series=venue_type
    type=grouped
    colorPalette={['#d5546e', '#a62098']}
/>

## Top-rated percentage by city

```sql quality_share
select * from dashboard_data.quality_share_analysis
```

<BarChart 
    data={quality_share}
    x=city
    y=Top_Rated
    swapXY=true
    xAxisTitle="Top-Rated Rate (%)"
    yAxisTitle=""
    yGridlines=false
    yMax=50
    colorPalette={['#3d121bff']}
/>

## Correlation between Quality and Number

<ScatterPlot 
    data={quality_share}
    x=total_venues
    y=Top_Rated
    series=city
    xAxisTitle="Cafés and Restaurants"
    yAxisTitle="Top-Rated (%)"
    colorPalette={['#E74C3C', '#729bb6ff', '#2ECC71', '#744904ff', '#674375ff', '#f4680aff']}
    size=10
/>




## Build Your Own Insights.
This dashboard is powered by [Google Cloud](https://cloud.google.com/), [Google Maps Platform](https://developers.google.com/maps), and [Evidence](https://evidence.dev/). You can find the code for this dashboard on [GitHub](https://github.com/mehd-io/pypi-duck-flow).


*Made with ❤️ by 🧢 [Liam](https://www.linkedin.com/in/liamtabibzadeh/)*