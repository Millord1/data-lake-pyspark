import os
from pathlib import Path

import duckdb
import requests
from dotenv import load_dotenv

from src.config.database import VELIB_HISTORY, StorageConfig

load_dotenv()

LOCAL_DIR = Path("/tmp/velib_historique")


def download_source(source_url: str, date_str: str) -> Path:
    """Télécharge le fichier historique Vélib correspondant à une date."""

    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    local_file = LOCAL_DIR / f"velib_historique_{date_str}.jjson.gz"

    print(f"Téléchargement : {source_url}")

    response = requests.get(
        source_url,
        stream=True,
        timeout=300,
    )
    response.raise_for_status()

    with local_file.open("wb") as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file.write(chunk)

    return local_file


def ingest_velib_history(source_url: str, date_str: str) -> None:
    source_file = download_source(source_url, date_str)

    conn = duckdb.connect()

    try:
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")

        conn.execute(
            f"""
            CREATE OR REPLACE SECRET minio (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{os.environ.get("MINIO_USER")}',
                SECRET '{os.environ.get("MINIO_PASSWORD")}',
                ENDPOINT '{os.environ.get("MINIO_ENDPOINT")}',
                REGION 'us-east-1',
                URL_STYLE 'path',
                USE_SSL false
            );
            """
        )

        base_path = StorageConfig.get_bucket_path(
            raw=True,
            dataset_name="historique",
        )

        output_path = f"{base_path}/velib_historique_{date_str}.parquet"

        print(f"Ingestion vers : {output_path}")

        conn.execute(
            f"""
            COPY (
                SELECT
                    station.status AS status,
                    station.contract_name AS contract_name,

                    to_timestamp(station.download_date)
                        AS download_date,

                    CAST(station.bike_stands AS BIGINT)
                        AS bike_stands,

                    CAST(station.number AS BIGINT)
                        AS station_id,

                    to_timestamp(station.last_update / 1000)
                        AS last_update,

                    CAST(station.available_bike_stands AS BIGINT)
                        AS available_bike_stands,

                    CAST(station.available_bikes AS BIGINT)
                        AS available_bikes

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

        count = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{output_path}')
            """
        ).fetchone()[0]

        print(f"✓ {date_str} : {count:,} lignes")

    finally:
        conn.close()


def run() -> None:
    for date_str, url in VELIB_HISTORY:
        try:
            ingest_velib_history(url, date_str)
        except requests.HTTPError as error:
            print(f"✗ {date_str} : fichier inaccessible ({error})")
        except Exception as error:
            print(f"✗ {date_str} : erreur ({error})")
