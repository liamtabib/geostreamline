import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import storage

# Load environment variables from .env file
load_dotenv()

from dagster import (
    asset, 
    AssetExecutionContext,
    AssetKey,
    Config,
    Definitions,
    define_asset_job,
    ScheduleDefinition
)
from dagster_gcp import BigQueryResource, GCSResource
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from gcs_to_bq.gcs_handler import load_gcs_to_bq
from gcs_to_bq.json_to_parquet import convert_json_to_parquet


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        # Keep default behavior for models
        return super().get_asset_key(dbt_resource_props)
    
    def get_deps_asset_keys(self, dbt_resource_props):
        # For models that depend on the maps_data.raw_maps_data source,
        # add dependency on our bq_maps_data asset
        deps = super().get_deps_asset_keys(dbt_resource_props)
        
        # Check if this model uses the raw_maps_data source
        if dbt_resource_props.get("resource_type") == "model":
            depends_on = dbt_resource_props.get("depends_on", {})
            nodes = depends_on.get("nodes", [])
            for node in nodes:
                if "source.maps_metrics.maps_data.raw_maps_data" in node:
                    deps.add(AssetKey(["bq_maps_data"]))
        
        return deps


class MapsConfig(Config):
    gcp_project: str = os.getenv("GCP_PROJECT", "your-project-id")
    gcs_bucket: str = os.getenv("GCS_BUCKET_NAME", "your-bucket-name")
    json_path_pattern: str = os.getenv("JSON_PATH_PATTERN", "")
    parquet_output_path: str = os.getenv("PARQUET_OUTPUT_PATH", "processed/maps_data.parquet")
    bq_dataset: str = os.getenv("BQ_DATASET", "maps_data")
    bq_table: str = os.getenv("BQ_TABLE", "raw_maps_data")
    maps_api_key: str = os.getenv("GOOGLE_MAPS_API_KEY", "")


@asset(
    description="Fetch Maps data from Google Area Insights API",
    group_name="ingestion"
)
def maps_api_ingestion(context: AssetExecutionContext, config: MapsConfig) -> str:
    """Fetch venue data from Google Area Insights API and upload to GCS"""
    try:
        if not config.maps_api_key:
            raise ValueError("GOOGLE_MAPS_API_KEY not found in environment variables")
        
        # Load place IDs
        place_ids_path = os.path.join(os.path.dirname(__file__), 'ingestion', 'place_ids.json')
        with open(place_ids_path, 'r') as f:
            place_ids = json.load(f)
        
        context.log.info(f"Starting Maps API ingestion for {len(place_ids)} cities")
        
        # API configuration
        endpoint = "https://areainsights.googleapis.com/v1:computeInsights"
        headers = {
            "Content-Type": "application/json",
            "X-Goog-FieldMask": "*"
        }
        
        def get_place_count(place_id, place_type, min_rating=None):
            filter_config = {
                "locationFilter": {
                    "region": {
                        "place": f"places/{place_id}"
                    }
                },
                "typeFilter": {
                    "includedTypes": [place_type]
                }
            }
            
            if min_rating:
                filter_config["ratingFilter"] = {
                    "minRating": min_rating
                }
            
            body = {
                "insights": ["INSIGHT_COUNT"],
                "filter": filter_config
            }

            resp = requests.post(
                endpoint,
                params={"key": config.maps_api_key},
                headers=headers,
                json=body,
                timeout=10
            )
            resp.raise_for_status()
            return resp.json()
        
        # Fetch data for all cities
        results = {}
        for city, place_id in place_ids.items():
            context.log.info(f"Processing {city}...")
            city_results = {}
            
            try:
                context.log.info(f"  Fetching cafes for {city}...")
                cafe_result = get_place_count(place_id, "cafe")
                city_results["cafes"] = cafe_result
                
                context.log.info(f"  Fetching excellent cafes for {city}...")
                excellent_cafe_result = get_place_count(place_id, "cafe", 4.5)
                city_results["excellent_cafes"] = excellent_cafe_result
                
                context.log.info(f"  Fetching restaurants for {city}...")
                restaurant_result = get_place_count(place_id, "restaurant")
                city_results["restaurants"] = restaurant_result
                
                context.log.info(f"  Fetching excellent restaurants for {city}...")
                excellent_restaurant_result = get_place_count(place_id, "restaurant", 4.5)
                city_results["excellent_restaurants"] = excellent_restaurant_result
                
                results[city] = city_results
                context.log.info(f"✓ {city} completed")
            except Exception as e:
                context.log.error(f"✗ {city} failed: {e}")
                results[city] = {"error": str(e)}
        
        # Upload to GCS with date-based folder structure
        client = storage.Client(project=config.gcp_project)
        bucket = client.bucket(config.gcs_bucket)
        
        # Use date-based path structure matching current pattern
        date_str = datetime.now().strftime('%Y-%m-%d')
        blob_name = f"maps_data/{date_str}/maps_data.json"
        
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(results, indent=2), content_type='application/json')
        
        gcs_path = f"gs://{config.gcs_bucket}/{blob_name}"
        context.log.info(f"✓ Uploaded Maps data to {gcs_path}")
        context.log.info(f"Processed {len([r for r in results.values() if 'error' not in r])} cities successfully")
        
        return gcs_path
        
    except Exception as e:
        context.log.error(f"Failed to fetch Maps data: {str(e)}")
        raise


@asset(
    description="Convert latest JSON file to Parquet format (incremental)",
    deps=[maps_api_ingestion],
    group_name="preprocessing"
)
def json_to_parquet_conversion(context: AssetExecutionContext, config: MapsConfig) -> str:
    """Convert latest Maps JSON data from GCS to Parquet format (incremental processing)"""
    try:
        context.log.info(f"Processing latest JSON file with prefix: {config.json_path_pattern}")
        
        parquet_path = convert_json_to_parquet(
            gcs_bucket=config.gcs_bucket,
            json_path_pattern=config.json_path_pattern,
            parquet_output_path=config.parquet_output_path,
            project_id=config.gcp_project
        )
        
        if parquet_path:
            context.log.info(f"Successfully processed latest JSON file to: {parquet_path}")
            return parquet_path
        else:
            # Return existing parquet path even if no new files processed
            existing_path = f"gs://{config.gcs_bucket}/{config.parquet_output_path}"
            context.log.info(f"No new files to process, returning existing path: {existing_path}")
            return existing_path
            
    except Exception as e:
        context.log.error(f"Failed to convert JSON to Parquet: {str(e)}")
        raise


@asset(
    description="Load Maps data from GCS to BigQuery",
    deps=[json_to_parquet_conversion],
    group_name="warehouse"
)
def bq_maps_data(context: AssetExecutionContext, config: MapsConfig, json_to_parquet_conversion: str) -> str:
    """Load converted Parquet data from GCS to BigQuery"""
    try:
        context.log.info(f"Loading data from Parquet file: {json_to_parquet_conversion}")
        
        load_gcs_to_bq(
            gcs_path=json_to_parquet_conversion,
            project_id=config.gcp_project,
            dataset_id=config.bq_dataset,
            table_id=config.bq_table
        )
        
        table_id = f"{config.gcp_project}.{config.bq_dataset}.{config.bq_table}"
        context.log.info(f"Loaded data to BigQuery table: {table_id}")
        return table_id
        
    except Exception as e:
        context.log.error(f"Failed to load data to BigQuery: {str(e)}")
        raise




# dbt assets - these will be created automatically from your dbt project  
@dbt_assets(
    manifest=os.path.join(os.path.dirname(__file__), "transform", "maps_metrics", "target", "manifest.json"),
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def maps_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt assets for transforming Maps data"""
    try:
        yield from dbt.cli(["build"], context=context).stream()
    except Exception as e:
        context.log.error(f"dbt build failed: {str(e)}")
        raise


@asset(
    description="Export dashboard metrics to Parquet for Evidence",
    deps=[maps_dbt_assets],
    group_name="dashboard"
)
def export_dashboard_data(context: AssetExecutionContext, config: MapsConfig, bigquery: BigQueryResource) -> str:
    """Export dashboard_metrics table from BigQuery to local Parquet file for Evidence"""
    try:
        # Use injected BigQuery resource
        with bigquery.get_client() as client:
            # Query to get dashboard metrics
            query = f"""
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
            FROM `{config.gcp_project}.{config.bq_dataset}.dashboard_metrics`
            ORDER BY place_type, excellence_percentage DESC
            """
            
            context.log.info("Querying BigQuery for dashboard metrics...")
            df = client.query(query).to_dataframe()
            
            if df.empty:
                context.log.warning("No data returned from BigQuery dashboard_metrics table")
                context.log.info("Creating empty dashboard file for first run")
                
                # Create an empty parquet file with correct schema for first run
                import pandas as pd
                empty_df = pd.DataFrame(columns=[
                    'city', 'place_type', 'place_type_display', 'total_count', 
                    'excellent_count', 'excellence_percentage', 'excellence_rank',
                    'readable_timestamp', 'ingestion_timestamp'
                ])
                
                dashboard_dir = os.path.join(os.path.dirname(__file__), "dashboard", "sources", "dashboard_data")
                os.makedirs(dashboard_dir, exist_ok=True)
                
                duckdb_path = os.path.join(dashboard_dir, "dashboard_data.duckdb")
                
                # Create empty DuckDB database
                import duckdb
                conn = duckdb.connect(duckdb_path)
                conn.execute("DROP TABLE IF EXISTS dashboard_metrics")
                conn.execute("CREATE TABLE dashboard_metrics AS SELECT * FROM empty_df")
                conn.close()
                
                context.log.info(f"Created empty DuckDB database at {duckdb_path}")
                return duckdb_path
            
            # Export to Parquet file in dashboard sources directory
            dashboard_dir = os.path.join(os.path.dirname(__file__), "dashboard", "sources", "dashboard_data")
            os.makedirs(dashboard_dir, exist_ok=True)
            
            duckdb_path = os.path.join(dashboard_dir, "dashboard_data.duckdb")
            
            # Create/update DuckDB database for Evidence.dev
            import duckdb
            conn = duckdb.connect(duckdb_path)
            conn.execute("DROP TABLE IF EXISTS dashboard_metrics")
            conn.execute("CREATE TABLE dashboard_metrics AS SELECT * FROM df")
            conn.close()
            
            context.log.info(f"Created DuckDB with {len(df)} rows at {duckdb_path}")
            context.log.info(f"Data includes: {df['city'].nunique()} cities, {df['place_type'].nunique()} place types")
            
            return duckdb_path
        
    except Exception as e:
        context.log.error(f"Failed to export dashboard data: {str(e)}")
        raise


@asset(
    description="Trigger Evidence dashboard refresh",
    deps=[export_dashboard_data],
    group_name="dashboard"
)
def evidence_dashboard(context: AssetExecutionContext, export_dashboard_data: str) -> str:
    """Trigger Evidence dashboard refresh after data export"""
    context.log.info(f"Dashboard data exported to: {export_dashboard_data}")
    context.log.info("Evidence dashboard ready with fresh data")
    return "dashboard_refreshed"


# Define the main job
maps_pipeline_job = define_asset_job(
    name="maps_pipeline",
    selection=[maps_api_ingestion, json_to_parquet_conversion, bq_maps_data, maps_dbt_assets, export_dashboard_data, evidence_dashboard],
    description="Complete Google Maps data pipeline from API ingestion to dashboard"
)

# Define schedule (daily at 6 AM)
maps_pipeline_schedule = ScheduleDefinition(
    job=maps_pipeline_job,
    cron_schedule="0 6 * * *",  # Daily at 6 AM
    name="daily_maps_pipeline"
)

# Resources
defs = Definitions(
    assets=[maps_api_ingestion, json_to_parquet_conversion, bq_maps_data, maps_dbt_assets, export_dashboard_data, evidence_dashboard],
    jobs=[maps_pipeline_job],
    schedules=[maps_pipeline_schedule],
    resources={
        "gcs": GCSResource(project=os.getenv("GCP_PROJECT", "your-project-id")),
        "bigquery": BigQueryResource(project=os.getenv("GCP_PROJECT", "your-project-id")),
        "dbt": DbtCliResource(
            project_dir=os.path.join(os.path.dirname(__file__), "transform", "maps_metrics"),
            profiles_dir=os.path.join(os.path.dirname(__file__), "transform", "maps_metrics"),
        ),
    }
)