from src.clean.spark_clean import clean_velib_df
from src.config.database import AnalysisConfig, StorageConfig
from src.driver.spark_driver import SparkConnector
from tests.integrity import validate_velib


def clean_velib():
    """Run clean for velib and integrity checks"""
    with SparkConnector() as spark:
        df = spark.get_data()

        print("RAW")
        validate_velib(df)

        cleaned_df = clean_velib_df(df)

        print("CURATED")
        validate_velib(cleaned_df)

        output_path = StorageConfig.get_bucket_path(
            raw=False,
            dataset_name=AnalysisConfig.view_name,
        )

        (cleaned_df.write.mode("overwrite").partitionBy("status").parquet(output_path))
