import shutil
from pathlib import Path

import kagglehub
from dotenv import load_dotenv

from src.config.database import SourcesUrls, StorageConfig
from src.driver.boto3_driver import S3Connector
from src.driver.duckdb_driver import DuckDBConnector

project_root = Path(__file__).resolve().parents[2]
load_dotenv(project_root / ".env")


class VelibDataIngestor:
    """Classe chargée de l'ingestion et du transfert des données Vélib."""

    def __init__(self, project_root: Path = project_root):
        self.project_root = project_root
        self.data_dir = self.project_root / "data"

    def download_and_move_dataset(self) -> str:
        """Télécharge le jeu de données historique Vélib depuis Kaggle."""
        target_file = self.data_dir / "velib_historique.parquet"

        if target_file.exists():
            print(f"Fichier local déjà présent : {target_file}")
            return str(target_file)

        print("Téléchargement du dataset Kaggle...")

        try:
            cache_path = Path(kagglehub.dataset_download("adrienmorel97/velib-data"))
        except Exception as e:
            raise RuntimeError(
                f"Échec du téléchargement du dataset Kaggle : {e}"
            ) from e

        parquet_files = list(cache_path.rglob("*.parquet"))

        if not parquet_files:
            all_files = [f.name for f in cache_path.rglob("*") if f.is_file()]
            raise FileNotFoundError(
                f"Aucun fichier Parquet trouvé dans {cache_path}. "
                f"Fichiers disponibles : {all_files}"
            )

        source_file = parquet_files[0]
        self.data_dir.mkdir(parents=True, exist_ok=True)

        shutil.copy2(source_file, target_file)
        print(f"Fichier copié avec succès vers {target_file}")

        return str(target_file)

    def ingest_stations_velib(self) -> None:
        """Ingère les données des stations Vélib vers MinIO avec DuckDB."""
        base_path = StorageConfig.get_bucket_path(
            raw=True,
            dataset_name="stations",
        )
        destination = f"{base_path}/velib_stations.parquet"

        print(f"Ingestion Vélib Stations vers MinIO : {destination}")

        with DuckDBConnector() as db:
            db.execute(
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

            result = db.fetchone(
                f"""
                SELECT COUNT(*) AS nb_stations
                FROM read_parquet('{destination}')
                """
            )

            print(f"Nombre de stations ingérées : {result[0]:,}")

        print("Ingestion des stations terminée.")

    def upload_parquet_to_minio(self) -> str:
        """Téléverse le jeu de données historique local vers MinIO."""
        local_parquet_path = self.download_and_move_dataset()
        filename = Path(local_parquet_path).name

        bucket_name = StorageConfig.BUCKET_NAME
        s3_key = (
            f"{StorageConfig.RAW_FOLDER}/"
            f"{StorageConfig.SCHEMA_NAME}/"
            f"historique/"
            f"{filename}"
        )
        target_s3_path = f"s3://{bucket_name}/{s3_key}"

        print(f"Transfert de {local_parquet_path} vers MinIO ({target_s3_path})...")

        try:
            with S3Connector() as s3:
                s3.ensure_bucket(bucket_name)
                s3.upload_file(
                    local_path=local_parquet_path,
                    bucket=bucket_name,
                    key=s3_key,
                )
        except Exception as e:
            raise RuntimeError(f"Échec du transfert vers MinIO : {e}") from e

        print("Upload de l'historique vers MinIO terminé avec succès.")
        return target_s3_path

    def run_pipeline(self) -> None:
        """Exécute l'ensemble du processus d'ingestion."""
        self.ingest_stations_velib()
        self.upload_parquet_to_minio()
