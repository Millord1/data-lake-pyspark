from enum import StrEnum


class SourcesUrls(StrEnum):
    velib_station: str = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/exports/parquet?lang=fr&timezone=Europe%2FBerlin"
    velib_historique: str = "https://github.com/lovasoa/historique-velib-opendata/releases/download/latest/stations.zip"
    velib_api: str = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/records?"
    meteo: str = "https://archive-api.open-meteo.com/v1/archive"


OPEN_METEO_ARCHIVE_URL=https://archive-api.open-meteo.com/v1/archive
METEO_LATITUDE=48.8534
METEO_LONGITUDE=2.3488
METEO_TIMEZONE=Europe/Paris
METEO_START_DATE=2021-01-01
METEO_END_DATE=2025-12-31