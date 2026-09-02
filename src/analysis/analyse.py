import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.config.database import get_spark_s3_path

load_dotenv(override=False)


def run():
    spark = (
        SparkSession.builder.appName("VelibAnalysis")
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

    bucket_path = get_spark_s3_path()
    df = spark.read.parquet(bucket_path)

    df.createOrReplaceTempView("velib")

    spark.sql("""
        SELECT
            *
            FROM velib
            LIMIT 5
    """).show()

    spark.stop()
