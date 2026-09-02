from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_velib(df: DataFrame) -> None:
    print("DATA QUALITY")

    df.select(
        F.count("*").alias("rows"),
        F.countDistinct("station_id").alias("stations"),
        F.sum(F.col("bikes").isNull().cast("int")).alias("null_bikes"),
        F.sum(F.col("capacity").isNull().cast("int")).alias("null_capacity"),
        F.sum(F.col("lat").isNull().cast("int")).alias("null_lat"),
        F.sum(F.col("lon").isNull().cast("int")).alias("null_lon"),
        F.min("ts_utc").alias("min_ts"),
        F.max("ts_utc").alias("max_ts"),
    ).show(truncate=False)
