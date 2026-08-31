import os
import shutil
from pathlib import Path

import boto3
import duckdb
import kagglehub
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from src.config.database import SourcesUrls, StorageConfig

project_root = Path(__file__).resolve().parents[2]
load_dotenv(project_root / ".env")

MINIO_ACCESS_KEY = os.environ.get("MINIO_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_PASSWORD")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")


def download_and_move_dataset() -> str:
    """Download the dataset (parquet) and move it to the data/ folder

    Raises:
        ValueError: KAGGLE secrets
        RuntimeError: Download error
        FileNotFoundError: Downloaded file not found

    Returns:
        str: Path to the downloaded file
    """
    target_dir = project_root / "data"
    target_file = target_dir / "velib_historique.parquet"

    if target_file.exists():
        print(f"Fichier local {target_file} déjà présent.")
        return str(target_file)

    if not os.getenv("KAGGLE_USERNAME") or not os.getenv("KAGGLE_API_TOKEN"):
        raise ValueError(
            "Les variables KAGGLE_USERNAME ou KAGGLE_API_TOKEN sont manquantes"
        )

    print("Téléchargement du dataset Kaggle...")
    cache_path = Path(kagglehub.dataset_download("adrienmorel97/velib-data"))

    all_files = [f for f in cache_path.rglob("*") if f.is_file()]
    if not all_files:
        raise RuntimeError(f"Téléchargement échoué ou dossier vide : {cache_path}")

    parquet_files = [f for f in all_files if f.suffix == ".parquet"]
    if not parquet_files:
        raise FileNotFoundError(
            f"Fichiers trouvés : {[f.name for f in all_files]}, mais aucun .parquet"
        )

    source_file = parquet_files[0]
    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(source_file, target_file)

    print(f"Fichier copié avec succès vers {target_file}")
    return str(target_file)


def ingest_stations_velib() -> None:
    """Push stations dataset to minIO using DuckDB

    Returns:
        str: Path to the downloaded file
    """

    con = duckdb.connect()

    try:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

        con.execute(
            f"""
            CREATE OR REPLACE SECRET minio (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{MINIO_ACCESS_KEY}',
                SECRET '{MINIO_SECRET_KEY}',
                ENDPOINT '{MINIO_ENDPOINT}',
                REGION 'us-east-1',
                URL_STYLE 'path',
                USE_SSL false
            );
            """
        )

        base_path = StorageConfig.get_bucket_path(raw=True, dataset_name="stations")
        destination = f"{base_path}/velib_stations.parquet"

        print(f"Ingestion Vélib Stations vers MinIO : {destination}")

        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet('{SourcesUrls.velib_station}')
            )
            TO '{destination}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                OVERWRITE_OR_IGNORE
            )
            """
        )

        print("Ingestion des stations terminée.")

        result = con.execute(
            f"""
            SELECT
                COUNT(*) AS nb_stations
            FROM read_parquet('{destination}')
            """
        ).fetchone()

        print(f"Nombre de stations ingérées : {result[0]}")

    finally:
        con.close()


def upload_parquet_to_minio() -> str:
    """Upload the downloaded parquet to minIO (historical) using boto3

    Raises:
        RuntimeError: Transfer to minIO

    Returns:
        str: S3 path
    """
    local_parquet_path = download_and_move_dataset()
    filename = Path(local_parquet_path).name

    bucket_name = StorageConfig.BUCKET_NAME
    s3_key = (
        f"{StorageConfig.RAW_FOLDER}/{StorageConfig.SCHEMA_NAME}/historique/{filename}"
    )

    if MINIO_ENDPOINT and not MINIO_ENDPOINT.startswith(("http://", "https://")):
        boto_endpoint = f"http://{MINIO_ENDPOINT}"

    s3_client = boto3.client(
        "s3",
        endpoint_url=boto_endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )

    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3_client.create_bucket(Bucket=bucket_name)

    target_s3_path = f"s3://{bucket_name}/{s3_key}"
    print(f"Transfert de {local_parquet_path} vers MinIO ({target_s3_path})...")

    try:
        s3_client.upload_file(local_parquet_path, bucket_name, s3_key)
        print("Upload de l'historique vers MinIO terminé avec succès.")
        return target_s3_path
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Échec du transfert vers MinIO : {e}") from e
