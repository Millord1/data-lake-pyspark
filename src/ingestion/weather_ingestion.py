from datetime import UTC, datetime

import requests
from pymongo import MongoClient
from pymongo.collection import Collection

from src.config.api import (
    METEO_END_DATE,
    METEO_HOURLY_VARIABLES,
    METEO_LATITUDE,
    METEO_LONGITUDE,
    METEO_START_DATE,
    METEO_TIMEZONE,
    SourcesUrls,
)
from src.config.database import MongoConfig


def get_mongo_collection(collection_name: str = "raw_weather_archive") -> Collection:
    """Instantiate a MongoDB client and return the target collection."""
    client = MongoClient(MongoConfig.get_uri())
    db = client[MongoConfig.get_database_name()]
    return db[collection_name]


def fetch_historical_weather(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    hourly_vars: list[str],
    tz: str = "Europe/Paris",
) -> dict:
    """Fetch raw historical weather metrics from Open-Meteo Archive API.

    Args:
        latitude: Geographic latitude.
        longitude: Geographic longitude.
        start_date: Start date string (YYYY-MM-DD).
        end_date: End date string (YYYY-MM-DD).
        hourly_vars: List of weather metric fields to query.
        tz: Timezone string.

    Returns:
        dict: Raw JSON response from Open-Meteo API.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(hourly_vars),
        "timezone": tz,
    }
    response = requests.get(SourcesUrls.meteo, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def ingest_weather_to_mongodb() -> None:
    """Fetch Open-Meteo historical data partitioned by year and land raw JSON into MongoDB."""
    collection = get_mongo_collection()

    start_year = int(METEO_START_DATE.split("-")[0])
    end_year = int(METEO_END_DATE.split("-")[0])

    print(f"Starting weather ingestion ({start_year} -> {end_year}) into MongoDB...")

    for year in range(start_year, end_year + 1):
        year_start = f"{year}-01-01" if year != start_year else METEO_START_DATE
        year_end = f"{year}-12-31" if year != end_year else METEO_END_DATE

        print(f"Fetching data for year {year} ({year_start} to {year_end})...")

        raw_payload = fetch_historical_weather(
            latitude=METEO_LATITUDE,
            longitude=METEO_LONGITUDE,
            start_date=year_start,
            end_date=year_end,
            hourly_vars=METEO_HOURLY_VARIABLES,
            tz=METEO_TIMEZONE,
        )

        document = {
            "source": "open-meteo-archive",
            "year": year,
            "start_date": year_start,
            "end_date": year_end,
            "latitude": METEO_LATITUDE,
            "longitude": METEO_LONGITUDE,
            "ingested_at": datetime.now(UTC).isoformat(),
            "payload": raw_payload,
        }

        # Update existing year record or insert new one
        result = collection.update_one(
            {"source": "open-meteo-archive", "year": year},
            {"$set": document},
            upsert=True,
        )

        if result.upserted_id:
            print(f"Year {year} inserted (ID: {result.upserted_id}).")
        else:
            print(f"Year {year} updated successfully.")

    print("Weather data ingestion into MongoDB Landing Zone complete.")


if __name__ == "__main__":
    ingest_weather_to_mongodb()
