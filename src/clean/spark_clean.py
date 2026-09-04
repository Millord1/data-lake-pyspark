import struct

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


class VelibCleaner:
    """Nettoie et normalise les données Vélib."""

    COMMON_COLUMNS = [
        "station_id",
        "capacity",
        "bikes",
        "mechanical",
        "ebike",
        "available_stands",
        "lat",
        "lon",
        "ts_utc",
        "tbin_utc",
        "status",
    ]

    @staticmethod
    def decode_lon(value):
        """Décode la longitude depuis le WKB de coordonnees_geo."""

        if value is None:
            return None

        try:
            return struct.unpack(
                "<d",
                bytes(value[5:13]),
            )[0]
        except (struct.error, IndexError):
            return None

    @staticmethod
    def decode_lat(value):
        """Décode la latitude depuis le WKB de coordonnees_geo."""

        if value is None:
            return None

        try:
            return struct.unpack(
                "<d",
                bytes(value[13:21]),
            )[0]
        except (struct.error, IndexError):
            return None

    def prepare_stations(
        self,
        df: DataFrame,
    ) -> DataFrame:
        """Prépare le référentiel des stations."""

        decode_lon_udf = F.udf(
            self.decode_lon,
            DoubleType(),
        )

        decode_lat_udf = F.udf(
            self.decode_lat,
            DoubleType(),
        )

        return (
            df.withColumn(
                "lon",
                decode_lon_udf(F.col("coordonnees_geo")),
            )
            .withColumn(
                "lat",
                decode_lat_udf(F.col("coordonnees_geo")),
            )
            .withColumn(
                "station_id",
                F.col("stationcode").cast("long"),
            )
            .select(
                "station_id",
                "lat",
                "lon",
            )
            .filter(F.col("station_id").isNotNull())
            .filter(F.col("lat").isNotNull())
            .filter(F.col("lon").isNotNull())
            .dropDuplicates(["station_id"])
        )

    def clean_historique(
        self,
        df: DataFrame,
        stations_df: DataFrame,
    ) -> DataFrame:
        """Nettoie les données historiques."""

        stations = self.prepare_stations(stations_df)

        return (
            df.withColumn(
                "station_id",
                F.col("station_id").cast("long"),
            )
            .withColumn(
                "capacity",
                F.col("bike_stands").cast("int"),
            )
            .withColumn(
                "bikes",
                F.col("available_bikes").cast("int"),
            )
            .withColumn(
                "available_stands",
                F.col("available_bike_stands").cast("int"),
            )
            .withColumn(
                "ts_utc",
                F.to_timestamp("last_update"),
            )
            .withColumn(
                "status",
                F.upper(F.trim(F.col("status"))),
            )
            # .dropDuplicates(
            #     [
            #         "station_id",
            #         "ts_utc",
            #     ]
            # )
            .join(
                stations,
                on="station_id",
                how="left",
            )
            .filter(F.col("station_id").isNotNull())
            .filter(F.col("station_id") > 0)
            .filter(F.col("ts_utc").isNotNull())
            .filter(F.col("lat").isNotNull())
            .filter(F.col("lon").isNotNull())
            .filter(
                F.col("lat").between(
                    48.0,
                    49.0,
                )
            )
            .filter(
                F.col("lon").between(
                    1.5,
                    3.0,
                )
            )
            .filter(F.col("capacity") > 0)
            .filter(F.col("bikes") >= 0)
            .filter(F.col("bikes") <= F.col("capacity"))
            .filter(
                F.col("status").isin(
                    "OPEN",
                    "CLOSED",
                )
            )
            .withColumn(
                "mechanical",
                F.lit(None).cast("int"),
            )
            .withColumn(
                "ebike",
                F.lit(None).cast("int"),
            )
            .withColumn(
                "tbin_utc",
                F.lit(None).cast("timestamp"),
            )
            .select(*self.COMMON_COLUMNS)
        )

    def clean_realtime(
        self,
        df: DataFrame,
        stations_df: DataFrame,
    ) -> DataFrame:
        """Nettoie les données temps réel."""

        stations = self.prepare_stations(stations_df)

        return (
            df.withColumn(
                "station_id",
                F.col("station_id").cast("long"),
            )
            .withColumn(
                "bikes",
                F.col("bikes").cast("int"),
            )
            .withColumn(
                "capacity",
                F.col("capacity").cast("int"),
            )
            .withColumn(
                "mechanical",
                F.col("mechanical").cast("int"),
            )
            .withColumn(
                "ebike",
                F.col("ebike").cast("int"),
            )
            .withColumn(
                "ts_utc",
                F.to_timestamp("ts_utc"),
            )
            .withColumn(
                "tbin_utc",
                F.to_timestamp("tbin_utc"),
            )
            .join(
                stations,
                on="station_id",
                how="left",
            )
            .withColumn(
                "available_stands",
                (F.col("capacity") - F.col("bikes")).cast("int"),
            )
            .withColumn(
                "status",
                F.lit(None).cast("string"),
            )
            .filter(F.col("station_id").isNotNull())
            .filter(F.col("station_id") > 0)
            .filter(F.col("ts_utc").isNotNull())
            .filter(F.col("lat").isNotNull())
            .filter(F.col("lon").isNotNull())
            .filter(
                F.col("lat").between(
                    48.0,
                    49.0,
                )
            )
            .filter(
                F.col("lon").between(
                    1.5,
                    3.0,
                )
            )
            .filter(F.col("capacity") > 0)
            .filter(F.col("bikes") >= 0)
            .filter(F.col("mechanical") >= 0)
            .filter(F.col("ebike") >= 0)
            .filter(F.col("bikes") <= F.col("capacity"))
            .filter(F.col("mechanical") <= F.col("capacity"))
            .filter(F.col("ebike") <= F.col("capacity"))
            .filter(F.col("mechanical") + F.col("ebike") == F.col("bikes"))
            .filter(F.col("tbin_utc").isNull() | (F.col("tbin_utc") <= F.col("ts_utc")))
            .select(*self.COMMON_COLUMNS)
            # .dropDuplicates(
            #     [
            #         "station_id",
            #         "ts_utc",
            #     ]
            # )
        )

    def clean(
        self,
        historique_df: DataFrame,
        realtime_df: DataFrame,
        stations_df: DataFrame,
    ) -> DataFrame:
        """Nettoie et fusionne toutes les données Vélib."""

        historique = self.clean_historique(
            historique_df,
            stations_df,
        )

        realtime = self.clean_realtime(
            realtime_df,
            stations_df,
        )

        return historique.unionByName(realtime)


class WeatherCleaner:
    """Nettoie et aplatit les archives JSON brutes d'Open-Meteo."""

    @staticmethod
    def clean(df_raw: DataFrame) -> DataFrame:
        """Déplie séries temporelles parallèles en lignes horaires relationnelles."""
        return (
            df_raw.select(
                F.explode(
                    F.arrays_zip(
                        "payload.hourly.time",
                        "payload.hourly.temperature_2m",
                        "payload.hourly.relative_humidity_2m",
                        "payload.hourly.precipitation",
                        "payload.hourly.wind_speed_10m",
                    )
                ).alias("w")
            )
            .select(
                F.to_timestamp("w.time").alias("weather_ts"),
                F.col("w.temperature_2m").cast("double").alias("temp_c"),
                F.col("w.relative_humidity_2m").cast("int").alias("humidity_pct"),
                F.col("w.precipitation").cast("double").alias("precip_mm"),
                F.col("w.wind_speed_10m").cast("double").alias("wind_speed_kmh"),
            )
            .filter(F.col("weather_ts").isNotNull())
            .dropDuplicates(["weather_ts"])
        )
