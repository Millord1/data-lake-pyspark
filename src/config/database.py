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


class SourcesUrls(StrEnum):
    velib_station: str = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/exports/parquet?lang=fr&timezone=Europe%2FBerlin"
    velib_api: str = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/records?"
    meteo: str = ""


def get_spark_s3_path():
    return StorageConfig.get_bucket_path().replace("s3", "s3a") + "/historique/"


VELIB_HISTORY = [
    (
        "2018-01-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2018-01-01-1514784352.gz",
    ),
    (
        "2017-12-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-12-01-1512105936.gz",
    ),
    (
        "2017-11-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-11-01-1509513968.gz",
    ),
    (
        "2017-10-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-10-01-1506831970.gz",
    ),
    (
        "2017-09-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-09-01-1504239946.gz",
    ),
    (
        "2017-08-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-08-01-1501561594.gz",
    ),
    (
        "2017-07-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-07-01-1498883174.gz",
    ),
    (
        "2017-06-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-06-01-1496291195.gz",
    ),
    (
        "2017-05-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-05-01-1493613233.gz",
    ),
    (
        "2017-04-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-04-01-1491020774.gz",
    ),
    (
        "2017-03-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-03-01-1488345976.gz",
    ),
    (
        "2017-02-01",
        "http://vlsstats.ifsttar.fr/rawdata/RawData/RawData_OLD/"
        "data_all_Paris.jjson_2017-02-01-1485926761.gz",
    ),
]
