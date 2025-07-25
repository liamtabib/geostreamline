import os
from datetime import datetime
from typing import List

from dagster import (
    asset, 
    AssetExecutionContext,
    Config,
    Definitions,
    define_asset_job,
    ScheduleDefinition
)
from dagster_gcp import BigQueryResource, GCSResource
from dagster_dbt import DbtCliResource, dbt_assets

from gcs_to_bq.gcs_handler import load_gcs_to_bq
from gcs_to_bq.json_to_parquet import convert_json_to_parquet


class MapsConfig(Config):
    gcp_project: str = os.getenv("GCP_PROJECT", "your-project-id")
    gcs_bucket: str = os.getenv("GCS_BUCKET", "your-bucket-name")
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
    context.log.info(f"Processing latest JSON file with prefix: {config.json_path_pattern}")
    
    parquet_path = convert_json_to_parquet(
        gcs_bucket=config.gcs_bucket,
        json_path_pattern=config.json_path_pattern,
        parquet_output_path=config.parquet_output_path,
        project_id=config.gcp_project
    )
    
    if parquet_path:
        context.log.info(f"Successfully processed latest JSON file to: {parquet_path}")
    else:
        context.log.info("No new files to process or processing failed")
    
    return parquet_path


@asset(
    description="Load Maps data from GCS to BigQuery",
    deps=[json_to_parquet_conversion],
    group_name="warehouse"
)
def bq_maps_data(context: AssetExecutionContext, config: MapsConfig, json_to_parquet_conversion: str) -> str:
    """Load converted Parquet data from GCS to BigQuery"""
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




# dbt assets - these will be created automatically from your dbt project
@dbt_assets(
    manifest=os.path.join(os.path.dirname(__file__), "transform", "maps_metrics", "target", "manifest.json"),
    project_dir=os.path.join(os.path.dirname(__file__), "transform", "maps_metrics"),
    profiles_dir=os.path.join(os.path.dirname(__file__), "transform", "maps_metrics"),
)
def maps_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt assets for transforming Maps data"""
    yield from dbt.cli(["build"], context=context).stream()


@asset(
    description="Trigger Evidence dashboard refresh",
    deps=[maps_dbt_assets],
    group_name="dashboard"
)
def evidence_dashboard(context: AssetExecutionContext) -> str:
    """Trigger Evidence dashboard refresh (placeholder)"""
    # In a real implementation, you might:
    # 1. Trigger a webhook to rebuild Evidence
    # 2. Update a flag file that Evidence watches
    # 3. Call Evidence's API if available
    
    context.log.info("Evidence dashboard refresh triggered")
    return "dashboard_refreshed"


# Define the main job
maps_pipeline_job = define_asset_job(
    name="maps_pipeline",
    selection=[json_to_parquet_conversion, bq_maps_data, maps_dbt_assets, evidence_dashboard],
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
    assets=[json_to_parquet_conversion, bq_maps_data, maps_dbt_assets, evidence_dashboard],
    jobs=[maps_pipeline_job],
    schedules=[maps_pipeline_schedule],
    resources={
        "gcs": GCSResource(project=os.getenv("GCP_PROJECT")),
        "bigquery": BigQueryResource(project=os.getenv("GCP_PROJECT")),
        "dbt": DbtCliResource(project_dir=os.path.join(os.path.dirname(__file__), "transform", "maps_metrics")),
    }
)