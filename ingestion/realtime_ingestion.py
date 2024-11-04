import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
import csv
from utils import get_bq_table_id


def get_csv_header_and_schema(gcs_uri):
    """Retrieve the CSV header and generate a BigQuery schema."""
    storage_client = storage.Client()
    bucket_name, blob_name = gcs_uri.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Read the first line as header
    header_line = blob.download_as_text().splitlines()[0]
    header = header_line.split(",")

    # Set each column to STRING while ingesting, later convert them to appropriate format in DBT
    schema = ", ".join([f"{col}:STRING" for col in header])
    return header, schema


def parse_csv_line(header):
    """Parse a single CSV line into a dictionary using the provided header."""

    def _parse_line(line):
        return dict(zip(header, csv.reader([line]).__next__()))

    return _parse_line


def ingest_gdrive_data(dataset_name, dataset_uri):
    """Load data from Google Drive into BigQuery in batch mode using Dataflow"""

    table_id = get_bq_table_id(dataset_name)

    # Dataflow options
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project="your_project",
        region="your_region",
        temp_location="gs://your_bucket/temp"
    )

    header, schema = get_csv_header_and_schema(dataset_uri)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | "Read CSV from GCS" >> beam.io.ReadFromText(dataset_uri, skip_header_lines=1)
         | "Parse CSV" >> beam.Map(parse_csv_line(header))
         | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    table_id,
                    schema=schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    # handle the state for atleast one delivery.
                    batch_size=500
                ))
    print(f"Data loaded to {table_id} using Dataflow in batch mode.")
