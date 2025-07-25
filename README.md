# Maps API Data Pipeline

A complete data pipeline that ingests café and restaurant data from Google Area Insights API, processes it through Google Cloud Storage, and loads it into BigQuery for analysis.

## 🏗️ Architecture

```
Google Area Insights API → GCS (JSON) → GCS (Parquet) → BigQuery → Evidence Dashboard
```

## 📊 Data Flow

1. **API Ingestion**: Fetches café/restaurant counts for European cities
2. **GCS Storage**: Raw JSON files with timestamp-based naming
3. **Data Processing**: Transforms nested JSON to normalized 5-column schema
4. **BigQuery Loading**: Time-series data for analytics
5. **Dashboard**: Evidence.dev visualization

## 🌍 Cities Covered

- Helsinki, Finland
- Stockholm, Sweden  
- Copenhagen, Denmark
- Berlin, Germany
- London, UK
- Amsterdam, Netherlands

## 📁 Project Structure

```
├── ingestion/                 # API data ingestion
│   ├── maps_api_ingestion.py # Main ingestion script
│   └── place_ids.json       # City to Place ID mapping
├── gcs_to_bq/                # Data processing pipeline
│   ├── json_to_parquet.py   # JSON → Parquet transformation
│   └── gcs_handler.py       # GCS → BigQuery loading
├── transform/                # dbt transformations
│   └── maps_metrics/        # dbt project
├── dashboard/                # Evidence.dev dashboard
└── dagster_pipeline.py      # Orchestration
```

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- Google Cloud Project with enabled APIs:
  - Google Area Insights API
  - Cloud Storage API  
  - BigQuery API
- Service account with appropriate permissions

### Setup

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd maps-pipeline
   ```

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Required environment variables**
   ```bash
   GOOGLE_MAPS_API_KEY=your_api_key
   GCS_BUCKET_NAME=your_bucket_name  
   GCP_PROJECT_ID=your_project_id
   GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
   ```

### Run Pipeline

1. **Ingest data from API**
   ```bash
   cd ingestion
   uv run python maps_api_ingestion.py
   ```

2. **Process and load to BigQuery**
   ```bash
   uv run python -c "
   from gcs_to_bq.json_to_parquet import convert_json_to_parquet
   from gcs_to_bq.gcs_handler import load_gcs_to_bq
   import os
   
   # Convert latest JSON to Parquet
   parquet_path = convert_json_to_parquet(
       gcs_bucket=os.getenv('GCS_BUCKET_NAME'),
       json_path_pattern='',
       parquet_output_path='processed/maps_data.parquet',
       project_id=os.getenv('GCP_PROJECT_ID')
   )
   
   # Load to BigQuery
   load_gcs_to_bq(
       gcs_path=parquet_path,
       project_id=os.getenv('GCP_PROJECT_ID'),
       dataset_id='maps_data',
       table_id='raw_maps_data'
   )
   "
   ```

## 📊 Data Schema

The pipeline produces a clean 5-column BigQuery table:

| Column | Type | Description |
|--------|------|-------------|
| `ingestion_timestamp` | INTEGER | When the data was processed |
| `city` | STRING | City name (Helsinki, Stockholm, etc.) |
| `place_type` | STRING | "cafe" or "restaurant" |
| `rating_filter` | FLOAT | NULL for all places, 4.5 for excellent places |
| `count` | INTEGER | Number of places found |

## 🔄 Pipeline Features

- **Incremental Processing**: Only processes new API data files
- **Duplicate Prevention**: File tracking prevents reprocessing
- **Time Series**: Maintains historical data across runs
- **Error Handling**: Skips cities with API errors
- **Data Quality**: Validates and normalizes all data

## 🏢 BigQuery Output

Example data structure:
```sql
SELECT * FROM `your-project.maps_data.raw_maps_data`
ORDER BY city, place_type, rating_filter;
```

| city | place_type | rating_filter | count | ingestion_timestamp |
|------|------------|---------------|-------|-------------------|
| Helsinki | cafe | NULL | 941 | 1753460486116238000 |
| Helsinki | cafe | 4.5 | 315 | 1753460486116238000 |
| Helsinki | restaurant | NULL | 3933 | 1753460486116238000 |
| Helsinki | restaurant | 4.5 | 1099 | 1753460486116238000 |

## 🛠️ Development

### Project Dependencies

- **Data Processing**: pandas, pyarrow
- **Cloud Integration**: google-cloud-storage, google-cloud-bigquery
- **Orchestration**: dagster
- **Analytics**: dbt-core, dbt-bigquery
- **Dashboard**: Evidence.dev
- **Utilities**: python-dotenv, loguru

### Code Style

- **Linting**: Ruff
- **Type Checking**: Python type hints
- **Formatting**: Auto-formatted with Ruff

## 📈 Analytics & Visualization

The pipeline supports rich analytics through:

1. **dbt Transformations**: Staging and mart models
2. **Evidence Dashboard**: Interactive visualizations
3. **Time Series Analysis**: Track changes over time
4. **City Comparisons**: Cross-city analytics

## 🔒 Security

- ✅ Environment variables for all secrets
- ✅ Service account authentication
- ✅ `.gitignore` prevents credential commits
- ✅ Minimal required permissions

## 📝 License

This project is licensed under the MIT License.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For questions or issues, please open a GitHub issue.