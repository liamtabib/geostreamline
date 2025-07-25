from google.cloud import bigquery
from loguru import logger


def load_gcs_to_bq(gcs_path: str, project_id: str, dataset_id: str, table_id: str) -> None:
    """Load data from GCS to BigQuery"""
    try:
        client = bigquery.Client(project=project_id)
        
        # Ensure dataset exists
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Using dataset {dataset_id}")
        
        # Load parquet to BigQuery (replace table contents)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        
        table_ref = dataset_ref.table(table_id)
        load_job = client.load_table_from_uri(gcs_path, table_ref, job_config=job_config)
        load_job.result()
        
        logger.info(f"Loaded {load_job.output_rows} rows to {project_id}.{dataset_id}.{table_id}")
        
    except Exception as e:
        logger.error(f"Failed to load GCS to BigQuery: {e}")
        raise


