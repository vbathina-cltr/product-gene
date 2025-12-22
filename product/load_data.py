from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def load_parquet_to_bigquery(project_id, dataset_id, table_id, gcs_uri):
    """
    Loads Parquet data from Google Cloud Storage into a BigQuery table.

    If the table already exists, the data will be appended.
    If you want to overwrite the table, change write_disposition to 'WRITE_TRUNCATE'.

    Args:
        project_id (str): Your Google Cloud project ID.
        dataset_id (str): The ID of your BigQuery dataset.
        table_id (str): The ID of the destination table.
        gcs_uri (str): The URI of the Parquet files in GCS (e.g., 'gs://my-bucket/data/*.parquet').
    """
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Configure the load job
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.autodetect = True  # Automatically infer the schema
    
    # Use WRITE_TRUNCATE to overwrite the table, or WRITE_APPEND to add data.
    # WRITE_EMPTY fails if the table is not empty.
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    print(f"Starting BigQuery load job to load {gcs_uri} into {dataset_id}.{table_id}")

    # Start the load job
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    load_job.result()  # Wait for the job to complete

    # Check the job's status
    destination_table = client.get_table(table_ref)
    print(f"Load job {load_job.job_id} finished.")
    print(f"Loaded {destination_table.num_rows} rows into {dataset_id}.{table_id}.")


if __name__ == '__main__':
    # Example usage:
    # Make sure you have authenticated with `gcloud auth application-default login`
    
    # --- Configuration ---
    # Replace with your actual values
    GCS_URI = "gs://phonic-raceway-481118-duckdb-data-20251214/food.parquet"

    PROJECT_ID = "phonic-raceway-481118-v0"
    DATASET_ID = "Product_Staging"
    TABLE_ID = "product-staging"
    
    load_parquet_to_bigquery(PROJECT_ID, DATASET_ID, TABLE_ID, GCS_URI)

