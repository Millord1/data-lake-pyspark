from pathlib import Path

from src.config.database import AnalysisQuery, StorageConfig
from src.driver.boto3_driver import S3Connector


def export_analysis(output_dir: str = "data/analysis") -> None:
    """Exporte les résultats d'analyse CSV depuis MinIO vers un dossier local."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    with S3Connector() as s3:
        for query_type in AnalysisQuery:
            analysis_name = query_type.value
            prefix = StorageConfig.get_analysis_key(analysis_name)
            objects = s3.list_objects(
                bucket=StorageConfig.BUCKET_NAME,
                prefix=prefix,
            )
            csv_files = [key for key in objects if key.endswith(".csv")]

            if not csv_files:
                print(f"⚠ Aucun CSV trouvé pour {analysis_name}")
                continue

            source_key = csv_files[0]
            destination = output_path / f"{analysis_name}.csv"
            s3.download_file(
                bucket=StorageConfig.BUCKET_NAME,
                key=source_key,
                local_path=str(destination),
            )

            print(f"✓ {analysis_name} → {destination}")


if __name__ == "__main__":
    export_analysis()
