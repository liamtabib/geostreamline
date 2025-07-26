#!/usr/bin/env python3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up credentials
credentials_path = os.path.expanduser(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# Import Dagster components
from dagster_pipeline import (
    json_to_parquet_conversion, 
    bq_maps_data, 
    maps_dbt_assets,
    export_dashboard_data,
    evidence_dashboard,
    MapsConfig,
    defs
)
from dagster import DagsterInstance, materialize

try:
    print("Starting Dagster pipeline execution...")
    
    # Create Dagster instance
    instance = DagsterInstance.ephemeral()
    
    # Create run config for assets
    run_config = {
        "resources": {
            "config": MapsConfig(
                gcp_project=os.getenv('GCP_PROJECT_ID'),
                gcs_bucket=os.getenv('GCS_BUCKET_NAME'),
                json_path_pattern='',
                parquet_output_path='processed/maps_data.parquet',
                bq_dataset='maps_data',
                bq_table='raw_maps_data'
            )
        }
    }
    
    # Materialize assets in order
    assets_to_materialize = [
        json_to_parquet_conversion,
        bq_maps_data,
        maps_dbt_assets,
        export_dashboard_data,
        evidence_dashboard
    ]
    
    result = materialize(
        assets_to_materialize,
        instance=instance,
        resources=defs.resources,
        run_config=run_config
    )
    
    print(f'Pipeline execution result: {result.success}')
    
    if result.success:
        print("✓ Pipeline completed successfully!")
    else:
        print("✗ Pipeline failed")
        
except Exception as e:
    print(f"Error executing pipeline: {e}")
    import traceback
    traceback.print_exc()