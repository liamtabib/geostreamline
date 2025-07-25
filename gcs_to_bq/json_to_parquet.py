import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from loguru import logger
import json
import tempfile
import os
from typing import List, Optional


def get_latest_json_file(bucket: storage.Bucket, prefix: str = "") -> Optional[storage.Blob]:
    """
    Find the most recent JSON file based on timestamp in filename
    
    Args:
        bucket: GCS bucket object
        prefix: Prefix to filter files
        
    Returns:
        Latest JSON blob or None if no files found
    """
    json_files = []
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith('.json') and 'cafe_restaurant_data_' in blob.name:
            json_files.append(blob)
    
    if not json_files:
        logger.warning(f"No JSON files found with prefix: {prefix}")
        return None
    
    def extract_timestamp(blob_name: str) -> str:
        """Extract timestamp from cafe_restaurant_data_20250125_143022.json"""
        try:
            # Split by underscore and get last two parts: ['20250125', '143022.json']
            parts = blob_name.split('_')
            if len(parts) >= 3:
                date_part = parts[-2]  # 20250125
                time_part = parts[-1].replace('.json', '')  # 143022
                return f"{date_part}_{time_part}"
            return "00000000_000000"  # fallback for malformed names
        except:
            return "00000000_000000"
    
    # Sort by timestamp and get the latest
    latest_file = max(json_files, key=lambda blob: extract_timestamp(blob.name))
    logger.info(f"Latest JSON file found: {latest_file.name}")
    return latest_file


def is_file_already_processed(bucket: storage.Bucket, filename: str) -> bool:
    """Check if file was already processed"""
    return bucket.blob(f"processed_files/{filename}.processed").exists()


def mark_file_as_processed(bucket: storage.Bucket, filename: str) -> None:
    """Mark file as processed"""
    bucket.blob(f"processed_files/{filename}.processed").upload_from_string("")
    logger.info(f"Marked file as processed: {filename}")


def convert_json_to_parquet(
    gcs_bucket: str,
    json_path_pattern: str,
    parquet_output_path: str,
    project_id: str
) -> str:
    """
    Convert latest JSON file from GCS to Parquet format (incremental processing)
    
    Args:
        gcs_bucket: GCS bucket name
        json_path_pattern: Pattern for JSON files (e.g., "maps_data/*/*.json") - used for prefix
        parquet_output_path: Output path for parquet file in GCS
        project_id: GCP project ID
        
    Returns:
        GCS path to the created parquet file
    """
    try:
        client = storage.Client(project=project_id)
        bucket = client.bucket(gcs_bucket)
        
        # Extract prefix from pattern for searching
        prefix = json_path_pattern.replace("*", "").replace(".json", "")
        
        # Find the latest JSON file
        latest_json_file = get_latest_json_file(bucket, prefix)
        
        if not latest_json_file:
            logger.warning(f"No JSON files found with prefix: {prefix}")
            return None
        
        # Check if this file was already processed
        if is_file_already_processed(bucket, latest_json_file.name):
            logger.info(f"File {latest_json_file.name} already processed, skipping")
            # Return existing parquet path
            return f"gs://{gcs_bucket}/{parquet_output_path}"
            
        logger.info(f"Processing new file: {latest_json_file.name}")
        
        # Process the latest JSON file and collect normalized data
        normalized_records = []
        
        # Download and parse JSON
        json_content = latest_json_file.download_as_text()
        ingestion_timestamp = pd.Timestamp.now()
        
        try:
            # Parse the nested JSON structure
            data = json.loads(json_content)
            
            # Transform nested city->category structure to normalized rows
            for city, city_data in data.items():
                # Skip cities with errors
                if isinstance(city_data, dict) and "error" not in city_data:
                    # Process each place type (cafes, restaurants, etc.)
                    for category, api_response in city_data.items():
                        # Extract place_type and rating_filter from category name
                        if category == "cafes":
                            place_type = "cafe"
                            rating_filter = None
                        elif category == "excellent_cafes":
                            place_type = "cafe" 
                            rating_filter = 4.5
                        elif category == "restaurants":
                            place_type = "restaurant"
                            rating_filter = None
                        elif category == "excellent_restaurants":
                            place_type = "restaurant"
                            rating_filter = 4.5
                        else:
                            continue  # Skip unknown categories
                        
                        # Extract count from API response (structure may vary)
                        count = None
                        if isinstance(api_response, dict):
                            # Try to find count in various possible locations
                            if 'count' in api_response:
                                count = api_response['count']
                                # Convert string count to integer if needed
                                if isinstance(count, str):
                                    try:
                                        count = int(count)
                                    except ValueError:
                                        count = None
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
                        
                        # Only add record if we successfully extracted a count
                        if count is not None:
                            normalized_records.append({
                                'ingestion_timestamp': ingestion_timestamp,
                                'city': city,
                                'place_type': place_type,
                                'rating_filter': rating_filter,
                                'count': count
                            })
                else:
                    logger.warning(f"Skipping city {city} due to error: {city_data.get('error', 'Unknown error')}")
                    
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON in {latest_json_file.name}: {e}")
            return None
        
        if not normalized_records:
            logger.error("No valid records found after normalization")
            return None
            
        logger.info(f"Normalized {len(normalized_records)} records from {latest_json_file.name}")
        
        # Convert new data to DataFrame with exact schema
        new_df = pd.DataFrame(normalized_records)
        
        # Ensure proper data types
        new_df['ingestion_timestamp'] = pd.to_datetime(new_df['ingestion_timestamp'])
        new_df['count'] = pd.to_numeric(new_df['count'], errors='coerce')
        
        # Append to existing parquet or create new one
        output_blob = bucket.blob(parquet_output_path)
        
        # Load existing data if it exists
        existing_df = None
        if output_blob.exists():
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as existing_tmp:
                output_blob.download_to_filename(existing_tmp.name)
                existing_df = pd.read_parquet(existing_tmp.name)
                os.unlink(existing_tmp.name)
        
        # Combine data
        final_df = pd.concat([existing_df, new_df], ignore_index=True) if existing_df is not None else new_df
        logger.info(f"Writing {len(final_df)} total records to parquet")
        
        # Write parquet file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            final_df.to_parquet(tmp_file.name, index=False)
            output_blob.upload_from_filename(tmp_file.name)
            os.unlink(tmp_file.name)
        
        # Mark the file as processed
        mark_file_as_processed(bucket, latest_json_file.name)
        
        output_gcs_path = f"gs://{gcs_bucket}/{parquet_output_path}"
        logger.info(f"Successfully processed {latest_json_file.name} and updated {output_gcs_path}")
        
        return output_gcs_path
        
    except Exception as e:
        logger.error(f"Failed to convert JSON to Parquet: {e}")
        raise