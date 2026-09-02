from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

KAFKA_TOPIC = "velib-status"


class AnalysisQuery(StrEnum):
    TEST_QUERY = "test_query"
    COUNT_STATIONS = "count_stations"
    TOP_STATIONS = "top_stations"
    AVAILABLE_BIKES_BY_STATION = "available_bikes_by_station"


@dataclass(frozen=True)
class AnalysisConfig:
    sql_file: Path = Path("src/sql/analyses.sql")
    view_name: str = "velib"


class SparkFiles(StrEnum):
    histo_files: str = "velib_historique_*.parquet"
    real_time_files: str = "velib_realtime_*.parquet"


class AnalysisQueries:
    def __init__(self, sql_file: Path):
        self.queries = self._load_queries(sql_file)

    @staticmethod
    def _load_queries(sql_file: Path) -> dict[str, str]:
        content = sql_file.read_text()

        queries = {}

        for block in content.split("-- name: ")[1:]:
            name, query = block.split("\n", 1)
            queries[name.strip()] = query.strip()

        return queries

    def get(self, query: AnalysisQuery) -> str:
        return self.queries[query.value]


class StorageConfig:
    BUCKET_NAME = "data"
    RAW_FOLDER = "raw"
    CLEANED_FOLDER = "curated"
    SCHEMA_NAME = "public"

    @classmethod
    def get_bucket_path(
        cls,
        raw: bool = True,
        dataset_name: str = "",
    ) -> str:
        """Retourne un chemin S3 complet."""
        folder = cls.RAW_FOLDER if raw else cls.CLEANED_FOLDER
        path = f"s3://{cls.BUCKET_NAME}/" f"{folder}/" f"{cls.SCHEMA_NAME}"
        if dataset_name:
            path = f"{path}/{dataset_name}"

        return path

    @classmethod
    def get_spark_s3_path(cls):
        return cls.get_bucket_path().replace("s3", "s3a") + "/historique"

    @classmethod
    def get_s3_key(
        cls,
        raw: bool = True,
        dataset_name: str = "",
        filename: str = "",
    ) -> str:
        """Retourne uniquement la clé S3 utilisée par boto3."""
        folder = cls.RAW_FOLDER if raw else cls.CLEANED_FOLDER
        parts = [
            folder,
            cls.SCHEMA_NAME,
        ]

        if dataset_name:
            parts.append(dataset_name)
        if filename:
            parts.append(filename)

        return "/".join(parts)


class SourcesUrls(StrEnum):
    velib_station: str = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/exports/parquet?lang=fr&timezone=Europe%2FBerlin"
    velib_api: str = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/records?"
    meteo: str = ""


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
