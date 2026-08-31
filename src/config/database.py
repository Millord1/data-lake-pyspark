KAFKA_TOPIC = "velib-status"


class StorageConfig:
    BUCKET_NAME = "data"
    RAW_FOLDER = "raw"
    CLEANED_FOLDER = "curated"
    SCHEMA_NAME = "public"

    @classmethod
    def get_bucket_path(cls, raw: bool = True, dataset_name: str = "") -> str:
        """Génère le chemin S3 cible (ex: s3://data/raw/public/stations)."""
        folder = cls.RAW_FOLDER if raw else cls.CLEANED_FOLDER
        path = f"s3://{cls.BUCKET_NAME}/{folder}/{cls.SCHEMA_NAME}"
        if dataset_name:
            path = f"{path}/{dataset_name}"
        return path
