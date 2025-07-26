#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from gcs_to_bq.gcs_handler import load_gcs_to_bq

# Load environment variables
load_dotenv()

# Set up credentials
credentials_path = os.path.expanduser(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

try:
    parquet_gcs_path = f"gs://{os.getenv('GCS_BUCKET_NAME')}/processed/maps_data.parquet"
    
    load_gcs_to_bq(
        gcs_path=parquet_gcs_path,
        project_id=os.getenv('GCP_PROJECT_ID'),
        dataset_id='maps_data',
        table_id='raw_maps_data'
    )
    
    print(f"Successfully loaded data from {parquet_gcs_path} to BigQuery")
    
except Exception as e:
    print(f"Error loading to BigQuery: {e}")
    import traceback
    traceback.print_exc()