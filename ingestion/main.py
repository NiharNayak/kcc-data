import sys
from ingestion.batch_ingestion import ingest_gdrive_data
from ingestion.realtime_ingestion import ingest_gdrive_data
import typer
from strenum import StrEnum

class IngestionMode(StrEnum):
    BATCH = "batch"
    REALTIME = "realtime"

def ingest_files(data_set_name: str, file_uri: str, ingestion_mode: IngestionMode ="batch"):

    if ingestion_mode == IngestionMode.BATCH:
        ingest_gdrive_data(data_set_name, file_uri)
    elif ingestion_mode == IngestionMode.REALTIME:
        ingest_gdrive_data(data_set_name, file_uri)
    else:
        print("Invalid option. Please choose 'batch' or 'realtime'.")
        sys.exit(1)

if __name__ == "__main__":
    typer.run(ingest_files)

# ingest_files("Sales_Data", "https://drive.google.com/file/d/1lo-JfvICoV0h5uRBbr_lOIUIKuHC7-sL")
# ingest_files("Store_Data", "https://drive.google.com/file/d/1lyzHcgxkTe9G7cJQywGXdwPjx7EqyizH")
# ingest_files("Product_Data", "https://drive.google.com/file/d/1lzuj2RRSu8na034PdANDKhT_VW4R2y0x")
