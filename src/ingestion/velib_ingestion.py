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
    """Gère l'ingestion des datasets Vélib."""

    def __init__(
        self,
        project_root: Path = project_root,
    ):
        self.project_root = project_root
        self.data_dir = project_root / "data"

    def _minio_object_exists(
        self,
        dataset_name: str,
        filename: str,
    ) -> bool:
        """Vérifie si un fichier existe déjà dans MinIO."""

        bucket = StorageConfig.BUCKET_NAME

        key = StorageConfig.get_s3_key(
            raw=True,
            dataset_name=dataset_name,
            filename=filename,
        )

        with S3Connector() as s3:
            return s3.exists(bucket, key)

    def download_and_move_dataset(self) -> str:
        """Télécharge le dataset historique depuis Kaggle."""

        target_file = self.data_dir / "velib_historique.parquet"

        if target_file.exists():
            print(f"Fichier local déjà présent : {target_file}")
            return str(target_file)

        print("Téléchargement du dataset Kaggle...")

        try:
            cache_path = Path(kagglehub.dataset_download("adrienmorel97/velib-data"))
        except Exception as error:
            raise RuntimeError("Échec du téléchargement du dataset Kaggle.") from error

        parquet_files = list(cache_path.rglob("*.parquet"))

        if not parquet_files:
            raise FileNotFoundError(f"Aucun fichier Parquet trouvé dans {cache_path}.")

        source_file = parquet_files[0]

        self.data_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        shutil.copy2(
            source_file,
            target_file,
        )

        print(f"Fichier copié vers : {target_file}")

        return str(target_file)

    def ingest_stations_velib(self) -> None:
        """Ingère les stations Vélib dans MinIO."""

        dataset_name = "stations"
        filename = "velib_stations.parquet"

        if self._minio_object_exists(
            dataset_name,
            filename,
        ):
            print(f"✓ {filename} existe déjà dans MinIO. " "Ingestion ignorée.")
            return

        base_path = StorageConfig.get_bucket_path(
            raw=True,
            dataset_name=dataset_name,
        )

        destination = f"{base_path}/{filename}"
        print(f"Ingestion des stations vers : {destination}")

        with DuckDBConnector() as db:
            db.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_parquet(
                        '{SourcesUrls.velib_station}'
                    )
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
                SELECT COUNT(*)
                FROM read_parquet('{destination}')
                """
            )

        print(f"✓ {result[0]:,} stations ingérées.")

    def upload_parquet_to_minio(self) -> str:
        """Upload le dataset historique vers MinIO."""

        dataset_name = "historique"
        filename = "velib_historique.parquet"

        if self._minio_object_exists(
            dataset_name,
            filename,
        ):
            base_path = StorageConfig.get_bucket_path(
                raw=True,
                dataset_name=dataset_name,
            )

            destination = f"{base_path}/{filename}"

            print(f"✓ {filename} existe déjà dans MinIO. " "Upload ignoré.")

            return destination

        local_parquet_path = self.download_and_move_dataset()

        bucket = StorageConfig.BUCKET_NAME

        key = StorageConfig.get_s3_key(
            raw=True,
            dataset_name=dataset_name,
            filename=filename,
        )

        with S3Connector() as s3:
            s3.ensure_bucket(bucket)

            s3.upload_file(
                local_path=local_parquet_path,
                bucket=bucket,
                key=key,
            )

        base_path = StorageConfig.get_bucket_path(
            raw=True,
            dataset_name=dataset_name,
        )

        destination = f"{base_path}/{filename}"

        print(f"✓ Upload terminé : {destination}")

        return destination

    def run_pipeline(self) -> None:
        """Exécute l'ingestion complète."""

        self.ingest_stations_velib()
        self.upload_parquet_to_minio()
