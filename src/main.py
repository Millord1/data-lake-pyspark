from src.ingestion.velib_ingestion import ingest_stations_velib, upload_parquet_to_minio


def main():
    ingest_stations_velib()
    upload_parquet_to_minio()


if __name__ == "__main__":
    main()
