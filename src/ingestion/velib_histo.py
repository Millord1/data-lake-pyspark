from pathlib import Path

import requests
from dotenv import load_dotenv

from src.config.database import VELIB_HISTORY, StorageConfig
from src.driver.duckdb_driver import DuckDBConnector

load_dotenv()


class VelibHistoryIngestor:
    """Gère le téléchargement et le traitement ETL
    des archives JSON historiques Vélib."""

    def __init__(self, local_dir: Path = Path("/tmp/velib_historique")):
        self.local_dir = local_dir
        self.local_dir.mkdir(parents=True, exist_ok=True)

    def download_source(self, source_url: str, date_str: str) -> Path:
        """Télécharge le fichier historique Vélib (.jjson.gz)
        correspondant à une date."""
        local_file = self.local_dir / f"velib_historique_{date_str}.jjson.gz"

        print(f"Téléchargement : {source_url}")

        response = requests.get(source_url, stream=True, timeout=300)
        response.raise_for_status()

        with local_file.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

        return local_file

    def ingest_single_day(self, source_url: str, date_str: str) -> None:
        """Transforme le fichier JSON local en Parquet vers MinIO/S3 via DuckDB."""
        source_file = self.download_source(source_url, date_str)

        base_path = StorageConfig.get_bucket_path(
            raw=True,
            dataset_name="historique",
        )
        output_path = f"{base_path}/velib_historique_{date_str}.parquet"

        print(f"Ingestion vers : {output_path}")

        with DuckDBConnector() as db:
            db.execute(
                f"""
                COPY (
                    SELECT
                        station.status AS status,
                        station.contract_name AS contract_name,
                        to_timestamp(station.download_date) AS download_date,
                        CAST(station.bike_stands AS BIGINT) AS bike_stands,
                        CAST(station.number AS BIGINT) AS station_id,
                        to_timestamp(station.last_update / 1000) AS last_update,
                        CAST(
                            station.available_bike_stands AS BIGINT
                        ) AS available_bike_stands,
                        CAST(station.available_bikes AS BIGINT) AS available_bikes
                    FROM read_json_auto('{source_file}')
                    CROSS JOIN UNNEST(json) AS t(station)
                )
                TO '{output_path}'
                (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD,
                    OVERWRITE_OR_IGNORE
                );
                """
            )

            count = db.fetchone(
                f"""
                SELECT COUNT(*)
                FROM read_parquet('{output_path}')
                """
            )[0]

            print(f"✓ {date_str} : {count:,} lignes")

    def run(self, history_list: list[tuple[str, str]] = VELIB_HISTORY) -> None:
        """Exécute l'ingestion pour l'ensemble de l'historique fourni."""
        for date_str, url in history_list:
            try:
                self.ingest_single_day(url, date_str)
            except requests.HTTPError as error:
                print(f"✗ {date_str} : fichier inaccessible ({error})")
            except Exception as error:
                print(f"✗ {date_str} : erreur ({error})")


if __name__ == "__main__":
    ingestor = VelibHistoryIngestor()
    ingestor.run()
