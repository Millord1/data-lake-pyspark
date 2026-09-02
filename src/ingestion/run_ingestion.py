from src.ingestion.velib_histo import run_histo_ingestion
from src.ingestion.velib_ingestion import ingest_stations_velib, upload_parquet_to_minio


def run_ingestion():
    ingest_stations_velib()
    upload_parquet_to_minio()
    run_histo_ingestion()
