from enum import StrEnum

KAFKA_TOPIC = "velib-status"


class StorageConfig:
    BUCKET_NAME = "data"
    RAW_FOLDER = "raw"
    CLEANED_FOLDER = "curated"
    SCHEMA_NAME = "public"

    @classmethod
    def get_bucket_path(cls, raw: bool = True, dataset_name: str = "") -> str:
        """Génère le chemin S3 cible (ex: s3://data/raw/public/stations)."""
        folder = cls.RAW_FOLDER if raw else cls.CLEANED_FOLDER
        path = f"s3://{cls.BUCKET_NAME}/{folder}/{cls.SCHEMA_NAME}"
        if dataset_name:
            path = f"{path}/{dataset_name}"
        return path


class GTFSTableNames(StrEnum):
    agency: str = "agency"
    calendar: str = "calendar"
    calendar_dates: str = "calendar_dates"
    routes: str = "routes"
    stop_times: str = "stop_times"
    stops: str = "stops"
    transfers: str = "transfers"
    trips: str = "trips"


class SourcesUrls(StrEnum):
    velib_station: str = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/exports/parquet?lang=fr&timezone=Europe%2FBerlin"
    velib_historique: str = "https://github.com/lovasoa/historique-velib-opendata/releases/download/latest/stations.zip"
    velib_api: str = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/records?"
    meteo: str = ""
