from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_velib_df(df: DataFrame) -> DataFrame:
    """
    Nettoie les données Vélib brutes.

    - Normalisation des types
    - Suppression des lignes invalides
    - Vérifications métier
    - Normalisation des statuts
    - Déduplication
    """

    cleaned = (
        df
        # Types
        .withColumn("station_id", F.col("station_id").cast("long"))
        .withColumn("bikes", F.col("bikes").cast("int"))
        .withColumn("capacity", F.col("capacity").cast("int"))
        .withColumn("mechanical", F.col("mechanical").cast("int"))
        .withColumn("ebike", F.col("ebike").cast("int"))
        .withColumn("lat", F.col("lat").cast("double"))
        .withColumn("lon", F.col("lon").cast("double"))
        .withColumn("ts_utc", F.to_timestamp("ts_utc"))
        .withColumn("tbin_utc", F.to_timestamp("tbin_utc"))
        # Normalisation texte
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        # Champs obligatoires
        .filter(F.col("station_id").isNotNull())
        .filter(F.col("ts_utc").isNotNull())
        .filter(F.col("lat").isNotNull())
        .filter(F.col("lon").isNotNull())
        # Identifiants
        .filter(F.col("station_id") > 0)
        # Coordonnées Paris / région parisienne
        .filter(F.col("lat").between(48.0, 49.0))
        .filter(F.col("lon").between(1.5, 3.0))
        # Valeurs numériques
        .filter(F.col("bikes") >= 0)
        .filter(F.col("capacity") > 0)
        .filter(F.col("mechanical") >= 0)
        .filter(F.col("ebike") >= 0)
        # Cohérence métier
        # Le nombre total de vélos ne peut pas dépasser la capacité
        .filter(F.col("bikes") <= F.col("capacity"))
        # Les mécaniques + électriques doivent correspondre
        # au nombre total de vélos.
        .filter(F.col("mechanical") + F.col("ebike") == F.col("bikes"))
        # Chaque catégorie ne peut pas dépasser la capacité
        .filter(F.col("mechanical") <= F.col("capacity"))
        .filter(F.col("ebike") <= F.col("capacity"))
        # Statut
        .filter(F.col("status").isNotNull())
        .filter(F.col("status").isin("OPEN", "CLOSED"))
        # Cohérence timestamp
        .filter(F.col("tbin_utc").isNull() | (F.col("tbin_utc") <= F.col("ts_utc")))
        # Déduplication
        .dropDuplicates(
            [
                "station_id",
                "ts_utc",
            ]
        )
    )

    return cleaned
