from enum import StrEnum


class SourcesUrls(StrEnum):
    velib_station: str = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/exports/parquet?lang=fr&timezone=Europe%2FBerlin"
    velib_historique: str = "https://github.com/lovasoa/historique-velib-opendata/releases/download/latest/stations.zip"
    velib_api: str = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/records?"
    meteo: str = "https://archive-api.open-meteo.com/v1/archive"


OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
METEO_LATITUDE = 48.8534
METEO_LONGITUDE = 2.3488
METEO_TIMEZONE = "Europe/Paris"
METEO_START_DATE = "2021-01-01"
METEO_END_DATE = "2025-12-31"

METEO_HOURLY_VARIABLES: list[str] = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "pressure_msl",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_high",
    "cloud_cover_mid",
    "snow_depth",
    "wind_speed_100m",
    "wind_direction_100m",
]
