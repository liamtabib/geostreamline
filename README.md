# Geostreamline

**Data engineering project analyzing cafés and restaurants across Northern European cities using Google Maps API.**

![Architecture](https://via.placeholder.com/800x200/1a1a1a/ffffff?text=Google+Area+Insights+API+→+GCS+→+BigQuery+→+Evidence+Dashboard)

## What This Pipeline Does

Demonstration of a complete data engineering workflow that:

- **Ingests** venue data from Google Maps API for 6 European cities
- **Processes** raw JSON data into normalized time-series format
- **Transforms** Using dbt for transformations
- **Visualizes** insights through an interactive Evidence.dev dashboard
- **Orchestration** Dagster for event-based triggers

## Project Structure

```
├── ingestion/                 # API data collection
│   ├── maps_api_ingestion.py
│   └── place_ids.json
├── gcs_to_bq/                # Data processing pipeline  
│   ├── json_to_parquet.py
│   └── gcs_handler.py
├── transform/maps_metrics/   # dbt transformations
├── dashboard/                # Evidence.dev visualization
└── dagster_pipeline.py      # Workflow orchestration
```

## Development Setup

### Requirements

- Python 3.12+
- Google Cloud Project with enabled APIs:
  - Area Insights API
  - Cloud Storage API
  - BigQuery API
- Service account with appropriate permissions

### Environment Configuration

```bash
# Clone and install
git clone <repo-url>
cd maps-api-pipeline
uv sync

# Configure credentials
cp .env.example .env
```

Required environment variables:
```bash
GOOGLE_MAPS_API_KEY=your_api_key
GCS_BUCKET_NAME=your_bucket_name  
GCP_PROJECT_ID=your_project_id
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
```

## Pipeline Stages

### 1. Ingestion 🐍

Fetch venue counts from Google Maps API:

```bash
cd ingestion
uv run python maps_api_ingestion.py
```

This creates timestamped JSON files in GCS with venue counts for each city/category combination.

### 2. Transformation

Convert JSON to normalized BigQuery schema:

```bash
uv run python dagster_pipeline.py
```

The pipeline handles:
- JSON → Parquet conversion
- Data validation and normalization  
- Incremental processing (only new files)
- BigQuery loading with time-series preservation

### 3. Visualization

**Live Dashboard**: https://www.geostreamline.dev/

Launch the Evidence.dev dashboard locally:

```bash
cd dashboard
npm install
npm run dev
```

SQL-based dashboard showing cafés and restaurant counts and quality comparison.

## Data Schema

| Column | Type | Description |
|--------|------|-------------|
| `city` | STRING | City name (Helsinki, Stockholm, etc.) |
| `place_type` | STRING | "cafe" or "restaurant" |
| `rating_filter` | FLOAT | Quality filter (4.5+ or NULL for all venues) |
| `count` | INTEGER | Number of venues found |
| `ingestion_timestamp` | INTEGER | When data was collected |

## Cities Covered

Helsinki • Stockholm • Copenhagen • Berlin • London • Amsterdam

## Technical Stack

- **Orchestration**: Dagster
- **Transformations & Data Processing**: BigQuery & dbt
- **Storage**: Google Cloud Storage 
- **Visualization**: Evidence.dev