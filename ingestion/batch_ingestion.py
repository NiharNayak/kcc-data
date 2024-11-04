import logging
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat
from utils import get_bq_table_id

# Initialize logging
logging.basicConfig(level=logging.INFO)


def ingest_gdrive_data(dataset_name, dataset_uri):
    """Load data from Google Drive into BigQuery."""

    table_id = get_bq_table_id(dataset_name)

    job_config = LoadJobConfig(
        source_format=SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        autodetect=True,  # Automatically detect schema
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table on each load
    )

    try:
        # Initialize BigQuery client using a context manager
        with bigquery.Client() as client:
            # Load data from Google Drive URL
            load_job = client.load_table_from_uri(dataset_uri, table_id, job_config=job_config)

            # Wait for job to complete
            load_job.result()
            logging.info(f"Data loaded to {table_id} successfully.")

    except Exception as e:
        logging.error(f"Failed to load data: {e}")
