import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

from src.config.database import AnalysisConfig, MongoConfig, SparkFiles, StorageConfig

load_dotenv(override=False)


class SparkConnector:
    def __init__(self, app_name: str = "VelibAnalysis"):
        # Resolve MongoDB URI
        mongo_uri = os.environ.get(
            "MONGO_URI",
            f"mongodb://{os.environ.get('MONGO_USER', 'admin')}:{os.environ.get('MONGO_PASSWORD', 'password123')}@{os.environ.get('MONGO_HOST', MongoConfig.default_host)}:{os.environ.get('MONGO_PORT', MongoConfig.default_port)}/?authSource=admin",
        )

        packages = [
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        ]

        self.spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.jars.packages", ",".join(packages))
            # MinIO / S3A settings
            .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT"))
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_USER"))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_PASSWORD"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.legacy.parquet.nanosAsLong", "true")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .config("spark.sql.parquet.mergeSchema", "false")
            # MongoDB settings
            .config("spark.mongodb.read.connection.uri", mongo_uri)
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")

    def get_weather_from_mongo(
        self,
        database: str = MongoConfig.database_name,
        collection: str = MongoConfig.collection_name,
    ) -> DataFrame:
        """Reads raw weather archives directly from MongoDB via Spark Connector."""
        return (
            self.spark.read.format("mongodb")
            .option("database", database)
            .option("collection", collection)
            .load()
        )

    def get_weather(self) -> DataFrame:
        """Lit les archives météo Parquet brutes depuis MinIO."""
        path = StorageConfig.get_bucket_path(
            raw=True,
            dataset_name="weather",
        ).replace("s3://", "s3a://")

        return self.spark.read.parquet(f"{path}/weather.parquet")

    def get_data(
        self,
        filename_pattern: str = "*.parquet",
    ) -> DataFrame:
        path = StorageConfig.get_spark_s3_path()
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
        df.createOrReplaceTempView(view_name)

    def sql(self, query: str) -> DataFrame:
        return self.spark.sql(query)

    def get_curated_data(
        self,
        status: str = SparkFiles.open_folder,
    ) -> DataFrame:
        base_path = StorageConfig.get_bucket_path(
            raw=False,
            dataset_name=AnalysisConfig.view_name,
        ).replace("s3://", "s3a://")

        df = self.spark.read.parquet(base_path)
        if status:
            status_val = status.value if hasattr(status, "value") else str(status)
            df = df.filter(df.status == status_val)

        return df

    def stop(self) -> None:
        self.spark.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
