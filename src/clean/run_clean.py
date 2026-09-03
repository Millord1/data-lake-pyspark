from src.clean.spark_clean import VelibCleaner
from src.config.database import AnalysisConfig, SparkFiles, StorageConfig
from src.driver.spark_driver import SparkConnector
from tests.integrity import validate_velib


def clean_velib():
    """Nettoie les données Vélib."""

    with SparkConnector() as spark:
        cleaner = VelibCleaner()

        historique_df = spark.get_data(SparkFiles.histo_files)
        realtime_df = spark.get_data(SparkFiles.real_time_files)
        stations_df = spark.get_stations()

        historique_clean = cleaner.clean_historique(
            historique_df,
            stations_df,
        )
        validate_velib(historique_clean)

        realtime_clean = cleaner.clean_realtime(realtime_df, stations_df)
        validate_velib(realtime_clean)

        cleaned_df = historique_clean.unionByName(
            realtime_clean,
            allowMissingColumns=True,
        )

        validate_velib(cleaned_df)

        output_path = StorageConfig.get_bucket_path(
            raw=False,
            dataset_name=AnalysisConfig.view_name,
        ).replace("s3", "s3a")

        (cleaned_df.write.mode("overwrite").partitionBy("status").parquet(output_path))

        print(f"✓ Données nettoyées : {output_path}")


if __name__ == "__main__":
    clean_velib()
