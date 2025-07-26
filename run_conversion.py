#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import storage
from gcs_to_bq.json_to_parquet import get_latest_json_file, is_file_already_processed, mark_file_as_processed
import pandas as pd
import json
import tempfile

# Load environment variables
load_dotenv()

# Set up credentials
credentials_path = os.path.expanduser(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Test with manual conversion
try:
    project_id = os.getenv('GCP_PROJECT_ID')
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    
    client = storage.Client(project=project_id, credentials=credentials)
    bucket = client.bucket(bucket_name)
    
    # Find latest JSON file
    latest_json_file = get_latest_json_file(bucket)
    
    if latest_json_file:
        print(f"Found latest file: {latest_json_file.name}")
        
        # Check if already processed
        if not is_file_already_processed(bucket, latest_json_file.name):
            print("Processing file...")
            
            # Download and process JSON
            json_content = latest_json_file.download_as_text()
            data = json.loads(json_content)
            
            # Normalize data
            normalized_records = []
            ingestion_timestamp = pd.Timestamp.now()
            
            for city, city_data in data.items():
                if isinstance(city_data, dict) and "error" not in city_data:
                    for category, api_response in city_data.items():
                        if category == "cafes":
                            place_type, rating_filter = "cafe", None
                        elif category == "excellent_cafes":
                            place_type, rating_filter = "cafe", 4.5
                        elif category == "restaurants":
                            place_type, rating_filter = "restaurant", None
                        elif category == "excellent_restaurants":
                            place_type, rating_filter = "restaurant", 4.5
                        else:
                            continue
                        
                        # Extract count
                        count = None
                        if isinstance(api_response, dict):
                            # Try direct count field first (current structure)
                            if 'count' in api_response:
                                count = api_response['count']
                                if isinstance(count, str):
                                    try:
                                        count = int(count)
                                    except ValueError:
                                        count = None
                            # Try insights structure (alternative structure)
                            elif 'insights' in api_response:
                                insights = api_response['insights']
                                if isinstance(insights, list) and len(insights) > 0:
                                    if 'count' in insights[0]:
                                        count = insights[0]['count']
                                        if isinstance(count, str):
                                            try:
                                                count = int(count)
                                            except ValueError:
                                                count = None
                        
                        if count is not None:
                            normalized_records.append({
                                'ingestion_timestamp': ingestion_timestamp,
                                'city': city,
                                'place_type': place_type,
                                'rating_filter': rating_filter,
                                'count': count
                            })
            
            if normalized_records:
                # Create DataFrame
                df = pd.DataFrame(normalized_records)
                df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp'])
                df['count'] = pd.to_numeric(df['count'], errors='coerce')
                
                # Write to Parquet
                parquet_output_path = 'processed/maps_data.parquet'
                output_blob = bucket.blob(parquet_output_path)
                
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                    df.to_parquet(tmp_file.name, index=False)
                    output_blob.upload_from_filename(tmp_file.name)
                    os.unlink(tmp_file.name)
                
                # Mark as processed
                mark_file_as_processed(bucket, latest_json_file.name)
                
                output_gcs_path = f"gs://{bucket_name}/{parquet_output_path}"
                print(f'Parquet file created at: {output_gcs_path}')
                print(f'Processed {len(df)} records')
            else:
                print("No valid records found")
        else:
            print("File already processed")
    else:
        print("No JSON files found")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()