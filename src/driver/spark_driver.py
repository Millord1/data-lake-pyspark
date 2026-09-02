import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

from src.config.database import StorageConfig

load_dotenv(override=False)


class SparkConnector:
    def __init__(self, app_name: str = "VelibAnalysis"):
        self.spark = (
            SparkSession.builder.appName(app_name)
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4",
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                os.environ.get("MINIO_ENDPOINT"),
            )
            .config(
                "spark.hadoop.fs.s3a.access.key",
                os.environ.get("MINIO_USER"),
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                os.environ.get("MINIO_PASSWORD"),
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.legacy.parquet.nanosAsLong", "true")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")

    def get_data(
        self,
        filename_pattern: str = "*.parquet",
    ) -> DataFrame:
        """Lit les données Vélib depuis MinIO."""

        path = StorageConfig.get_spark_s3_path()

        print(f"Lecture Spark : {path}")
        print(f"Pattern : {filename_pattern}")

        return self.spark.read.option(
            "pathGlobFilter",
            filename_pattern,
        ).parquet(path)

    def get_stations(self) -> DataFrame:
        path = StorageConfig.get_bucket_path(
            raw=True,
            dataset_name="stations",
        ).replace("s3://", "s3a://")

        return self.spark.read.parquet(path)

    def create_view(
        self,
        df: DataFrame,
        view_name: str = "velib",
    ) -> None:
        """Create view for PySpark

        Args:
            df (DataFrame): The PySpark DataFrame
            view_name (str, optional): Defaults to "velib".
        """
        df.createOrReplaceTempView(view_name)

    def sql(self, query: str) -> DataFrame:
        """Run the SQL query on PySpark

        Args:
            query (str): The SQL query as string

        Returns:
            DataFrame: PySpark DataFrame
        """
        return self.spark.sql(query)

    def stop(self) -> None:
        """Stop PySpark"""
        self.spark.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
