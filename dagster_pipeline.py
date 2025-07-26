import os
from dotenv import load_dotenv

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


@asset(
    description="Convert latest JSON file to Parquet format (incremental)",
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
                
                parquet_path = os.path.join(dashboard_dir, "dashboard_metrics.parquet")
                empty_df.to_parquet(parquet_path, index=False)
                
                context.log.info(f"Created empty dashboard file at {parquet_path}")
                return parquet_path
            
            # Export to Parquet file in dashboard sources directory
            dashboard_dir = os.path.join(os.path.dirname(__file__), "dashboard", "sources", "dashboard_data")
            os.makedirs(dashboard_dir, exist_ok=True)
            
            parquet_path = os.path.join(dashboard_dir, "dashboard_metrics.parquet")
            df.to_parquet(parquet_path, index=False)
            
            context.log.info(f"Exported {len(df)} rows to {parquet_path}")
            context.log.info(f"Data includes: {df['city'].nunique()} cities, {df['place_type'].nunique()} place types")
            
            return parquet_path
        
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
    selection=[json_to_parquet_conversion, bq_maps_data, maps_dbt_assets, export_dashboard_data, evidence_dashboard],
    description="Google Maps data pipeline from JSON to dashboard"
)

# Define schedule (daily at 6 AM)
maps_pipeline_schedule = ScheduleDefinition(
    job=maps_pipeline_job,
    cron_schedule="0 6 * * *",  # Daily at 6 AM
    name="daily_maps_pipeline"
)

# Resources
defs = Definitions(
    assets=[json_to_parquet_conversion, bq_maps_data, maps_dbt_assets, export_dashboard_data, evidence_dashboard],
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