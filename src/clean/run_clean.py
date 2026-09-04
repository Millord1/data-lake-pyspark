from pyspark.sql import functions as F

from src.clean.spark_clean import VelibCleaner, WeatherCleaner
from src.config.database import AnalysisConfig, SparkFiles, StorageConfig
from src.driver.spark_driver import SparkConnector
from tests.integrity import validate_velib


def clean_velib():
    """Nettoie les données Vélib, transforme la météo et fusionne les deux datasets."""

    with SparkConnector() as spark:
        # 1. Nettoyage des données Vélib
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

        # 2. Nettoyage et aplatissement de la météo brute
        weather_cleaner = WeatherCleaner()
        raw_weather_df = spark.get_weather()
        weather_clean = weather_cleaner.clean(raw_weather_df)

        # 3. Jointure Vélib + Météo sur l'heure tronquée
        final_df = (
            cleaned_df.withColumn("hour_key", F.date_trunc("hour", F.col("ts_utc")))
            .join(
                weather_clean,
                F.col("hour_key") == weather_clean["weather_ts"],
                how="left",
            )
            .drop("hour_key", "weather_ts")
        )

        # 4. Écriture du jeu de données Curated partitionné par status
        output_path = StorageConfig.get_bucket_path(
            raw=False,
            dataset_name=AnalysisConfig.view_name,
        ).replace("s3", "s3a")

        (final_df.write.mode("overwrite").partitionBy("status").parquet(output_path))

        print(f"Données Vélib + Météo nettoyées : {output_path}")


if __name__ == "__main__":
    clean_velib()
