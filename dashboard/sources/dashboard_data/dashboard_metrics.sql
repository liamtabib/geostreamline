SELECT 
  city,
  place_type,
  place_type_display,
  total_count,
  excellent_count,
  excellence_percentage,
  excellence_rank,
  readable_timestamp,
  ingestion_timestamp
FROM read_parquet('./sources/dashboard_data/dashboard_metrics.parquet')
ORDER BY place_type, excellence_percentage DESC