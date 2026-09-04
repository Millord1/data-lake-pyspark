from pathlib import Path

import requests
from dotenv import load_dotenv

from src.config.database import VELIB_HISTORY, StorageConfig
from src.driver.boto3_driver import S3Connector
from src.driver.duckdb_driver import DuckDBConnector

load_dotenv()


class VelibHistoryIngestor:
    """Manage archive ingestion"""

    def __init__(
        self,
        local_dir: Path = Path("/tmp/velib_historique"),
    ):
        self.local_dir = local_dir
        self.local_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    def _minio_object_exists(
        self,
        filename: str,
    ) -> bool:
        """Check if an object already exists

        Args:
            filename (str): filename to find

        Returns:
            bool: True if exists
        """
        bucket = StorageConfig.BUCKET_NAME
        key = StorageConfig.get_s3_key(
            raw=True,
            dataset_name="historique",
            filename=filename,
        )

        with S3Connector() as s3:
            return s3.exists(bucket, key)

    def download_source(
        self,
        source_url: str,
        date_str: str,
    ) -> Path:
        """Download an historical archive

        Args:
            source_url (str): Url to call
            date_str (str): Date to put on the file name

        Returns:
            Path: Path of downloaded file
        """

        filename = f"velib_historique_{date_str}.jjson.gz"
        local_file = self.local_dir / filename
        response = requests.get(
            source_url,
            stream=True,
            timeout=300,
        )

        response.raise_for_status()
        with local_file.open("wb") as file:
            for chunk in response.iter_content(
                chunk_size=1024 * 1024,
            ):
                if chunk:
                    file.write(chunk)
        return local_file

    def ingest_single_day(
        self,
        source_url: str,
        date_str: str,
    ) -> None:
        """Ingest a signle day of data

        Args:
            source_url (str): Url to call
            date_str (str): Date to put on the file name
        """
        filename = f"velib_historique_{date_str}.parquet"
        if self._minio_object_exists(filename):
            print(f"✓ {filename} existe déjà dans MinIO. " "Ingestion ignorée.")
            return

        source_file = self.download_source(
            source_url,
            date_str,
        )
        base_path = StorageConfig.get_bucket_path(
            raw=True,
            dataset_name="historique",
        )

        destination = f"{base_path}/{filename}"
        print(f"Ingestion vers : {destination}")

        with DuckDBConnector() as db:
            db.execute(
                f"""
                COPY (
                    SELECT
                        station.status AS status,

                        station.contract_name
                            AS contract_name,

                        to_timestamp(
                            station.download_date
                        ) AS download_date,

                        CAST(
                            station.bike_stands AS BIGINT
                        ) AS bike_stands,

                        CAST(
                            station.number AS BIGINT
                        ) AS station_id,

                        to_timestamp(
                            station.last_update / 1000
                        ) AS last_update,

                        CAST(
                            station.available_bike_stands
                            AS BIGINT
                        ) AS available_bike_stands,

                        CAST(
                            station.available_bikes AS BIGINT
                        ) AS available_bikes

                    FROM read_json_auto(
                        '{source_file}'
                    )

                    CROSS JOIN UNNEST(json)
                        AS t(station)
                )
                TO '{destination}'
                (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD,
                    OVERWRITE_OR_IGNORE
                )
                """
            )

            count = db.fetchone(
                f"""
                SELECT COUNT(*)
                FROM read_parquet('{destination}')
                """
            )[0]

        print(f"✓ {date_str} : " f"{count:,} lignes")

    def run(
        self,
        history_list: list[tuple[str, str]] = VELIB_HISTORY,
    ) -> None:
        """Ingest all historical data

        Args:
            history_list (list[tuple[str, str]], optional): List of URLs to call.
            Defaults to VELIB_HISTORY.
        """

        for date_str, url in history_list:
            try:
                self.ingest_single_day(
                    source_url=url,
                    date_str=date_str,
                )

            except requests.HTTPError as error:
                print(f"✗ {date_str} : " f"fichier inaccessible ({error})")

            except Exception as error:
                print(f"✗ {date_str} : " f"erreur ({error})")
