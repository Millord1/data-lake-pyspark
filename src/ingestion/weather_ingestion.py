import json
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.config.database import (
    SourcesUrls,
    StorageConfig,
)
from src.config.database import (
    WeatherQueryConfig as WeatherConfig,
)
from src.driver.boto3_driver import S3Connector
from src.driver.mongo_driver import MongoConnector


class WeatherIngestor:
    """Ingère la météo brute dans MongoDB et l'exporte en JSON brut vers MinIO."""

    def __init__(self, local_dir: Path = Path("/tmp/weather")):
        self.local_dir = local_dir
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.dataset_name = "weather"

    def fetch_year_from_api(self, year: int) -> dict:
        """Télécharge les données horaires météo pour une année donnée."""
        start_year = int(WeatherConfig.start_date.split("-")[0])
        end_year = int(WeatherConfig.end_date.split("-")[0])

        year_start = f"{year}-01-01" if year != start_year else WeatherConfig.start_date
        year_end = f"{year}-12-31" if year != end_year else WeatherConfig.end_date

        params = {
            "latitude": WeatherConfig.latitude,
            "longitude": WeatherConfig.longitude,
            "start_date": year_start,
            "end_date": year_end,
            "hourly": ",".join(WeatherConfig.hourly_variables),
            "timezone": WeatherConfig.timezone,
        }

        response = requests.get(SourcesUrls.meteo, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def ingest_to_mongo(self) -> None:
        """Récupère l'API Open-Meteo et insère les documents bruts dans MongoDB."""
        start_year = int(WeatherConfig.start_date.split("-")[0])
        end_year = int(WeatherConfig.end_date.split("-")[0])

        with MongoConnector() as mongo:
            collection = mongo.get_collection()

            for year in range(start_year, end_year + 1):
                if mongo.document_exists({"year": year}):
                    print(
                        f"✓ Météo {year} déjà présente dans MongoDB. Ingestion ignorée."
                    )
                    continue

                print(f"Téléchargement météo {year} depuis Open-Meteo...")
                payload = self.fetch_year_from_api(year)

                document = {
                    "source": "open-meteo-archive",
                    "year": year,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "payload": payload,
                }

                collection.update_one({"year": year}, {"$set": document}, upsert=True)
                print(f"✓ Météo {year} insérée dans MongoDB.")

    def export_mongo_to_minio(self) -> None:
        """Exporte les documents JSON bruts depuis MongoDB vers le bucket MinIO."""
        bucket = StorageConfig.BUCKET_NAME
        start_year = int(WeatherConfig.start_date.split("-")[0])
        end_year = int(WeatherConfig.end_date.split("-")[0])

        with MongoConnector() as mongo, S3Connector() as s3:
            s3.ensure_bucket(bucket)
            collection = mongo.get_collection()

            for year in range(start_year, end_year + 1):
                filename = f"weather_{year}.json"
                key = StorageConfig.get_s3_key(
                    raw=True,
                    dataset_name=self.dataset_name,
                    filename=filename,
                )

                if s3.exists(bucket, key):
                    print(f"✓ {filename} existe déjà dans MinIO. Export ignoré.")
                    continue

                # Exclure le champ interne ObjectId '_id' pour la sérialisation standard
                doc = collection.find_one({"year": year}, {"_id": 0})
                if not doc:
                    print(f"⚠ Aucun document trouvé dans MongoDB pour {year}.")
                    continue

                temp_file = self.local_dir / filename
                with temp_file.open("w", encoding="utf-8") as f:
                    json.dump(doc, f)

                s3.upload_file(
                    local_path=str(temp_file),
                    bucket=bucket,
                    key=key,
                )

                if temp_file.exists():
                    temp_file.unlink()

                print(f"✓ {filename} uploadé dans MinIO ({key}).")

    def run(self) -> None:
        """Exécute l'ingestion Mongo et le transfert brut vers MinIO."""
        self.ingest_to_mongo()
        self.export_mongo_to_minio()


if __name__ == "__main__":
    WeatherIngestor().run()
