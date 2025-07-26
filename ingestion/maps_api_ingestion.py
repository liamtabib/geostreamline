#!/usr/bin/env python3
"""
Maps API Ingestion Script

Fetches café and restaurant data from Google Places API (INSIGHT_COUNT) 
and uploads to Google Cloud Storage for further processing.

Usage:
    python maps_api_ingestion.py [CITY_KEY]
    
If CITY_KEY is provided, only processes that city.
If not provided, processes all cities in place_ids.json.
"""

import sys
import json
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

ENDPOINT = "https://areainsights.googleapis.com/v1:computeInsights"
HEADERS = {
    "Content-Type": "application/json",
    "X-Goog-FieldMask": "*"
}

def load_place_ids():
    place_ids_path = os.path.join(os.path.dirname(__file__), 'place_ids.json')
    with open(place_ids_path, 'r') as f:
        return json.load(f)

def get_place_count(api_key, place_id, place_type, min_rating=None):
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
        ENDPOINT,
        params={"key": api_key},
        headers=HEADERS,
        json=body,
        timeout=10
    )
    resp.raise_for_status()
    return resp.json()

def upload_to_gcs(results, bucket_name, project_id):
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    blob_name = f"cafe_restaurant_data_{timestamp}.json"
    
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(results, indent=2), content_type='application/json')
    
    print(f"✓ Uploaded to gs://{bucket_name}/{blob_name}")
    return blob_name

def main():
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    project_id = os.getenv('GCP_PROJECT_ID')
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    if not api_key:
        sys.exit("Error: GOOGLE_MAPS_API_KEY not found in .env file")
    if not bucket_name:
        sys.exit("Error: GCS_BUCKET_NAME not found in .env file")
    if not project_id:
        sys.exit("Error: GCP_PROJECT_ID not found in .env file")
    if not credentials_path:
        sys.exit("Error: GOOGLE_APPLICATION_CREDENTIALS not found in .env file")
    
    # Expand tilde in credentials path
    credentials_path = os.path.expanduser(credentials_path)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    city_key = sys.argv[1] if len(sys.argv) > 1 else None
    
    place_ids = load_place_ids()
    results = {}
    
    if city_key:
        if city_key not in place_ids:
            sys.exit(f"City '{city_key}' not found in place_ids.json")
        cities_to_process = {city_key: place_ids[city_key]}
    else:
        cities_to_process = place_ids
    
    for city, place_id in cities_to_process.items():
        print(f"Processing {city}...")
        city_results = {}
        
        try:
            print(f"  Fetching cafes for {city}...")
            cafe_result = get_place_count(api_key, place_id, "cafe")
            city_results["cafes"] = cafe_result
            
            print(f"  Fetching excellent cafes for {city}...")
            excellent_cafe_result = get_place_count(api_key, place_id, "cafe", 4.5)
            city_results["excellent_cafes"] = excellent_cafe_result
            
            print(f"  Fetching restaurants for {city}...")
            restaurant_result = get_place_count(api_key, place_id, "restaurant")
            city_results["restaurants"] = restaurant_result
            
            print(f"  Fetching excellent restaurants for {city}...")
            excellent_restaurant_result = get_place_count(api_key, place_id, "restaurant", 4.5)
            city_results["excellent_restaurants"] = excellent_restaurant_result
            
            results[city] = city_results
            print(f"✓ {city} completed")
        except Exception as e:
            print(f"✗ {city} failed: {e}")
            results[city] = {"error": str(e)}
    
    try:
        upload_to_gcs(results, bucket_name, project_id)
    except Exception as e:
        print(f"✗ Failed to upload to GCS: {e}")

if __name__ == "__main__":
    main()
