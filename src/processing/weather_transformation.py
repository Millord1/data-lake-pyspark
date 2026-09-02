from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.database import MongoConfig


def get_spark_session(app_name: str = "WeatherTransformation") -> SparkSession:
    """Build Spark session configured for MongoDB and MinIO/S3 connectivity."""
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.mongodb.read.connection.uri",
            MongoConfig.get_uri(),
        )
        .config(
            "spark.mongodb.read.database",
            MongoConfig.get_database_name(),
        )
        .config(
            "spark.mongodb.read.collection",
            "raw_weather_archive",
        )
        # MinIO S3A settings
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def transform_weather_raw(df_mongo: DataFrame) -> DataFrame:
    """Explode nested Open-Meteo arrays into relational hourly rows."""
    return (
        df_mongo.select(
            F.explode(
                F.arrays_zip(
                    "payload.hourly.time",
                    "payload.hourly.temperature_2m",
                    "payload.hourly.relative_humidity_2m",
                    "payload.hourly.precipitation",
                    "payload.hourly.wind_speed_10m",
                )
            ).alias("weather")
        )
        .select(
            F.to_timestamp("weather.time").alias("timestamp"),
            F.col("weather.temperature_2m").cast("float").alias("temperature_c"),
            F.col("weather.relative_humidity_2m").cast("integer").alias("humidity_pct"),
            F.col("weather.precipitation").cast("float").alias("precipitation_mm"),
            F.col("weather.wind_speed_10m").cast("float").alias("wind_speed_kmh"),
        )
        .filter(F.col("timestamp").isNotNull())
    )
