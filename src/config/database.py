import os

from dotenv import load_dotenv

load_dotenv()

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


class MongoConfig:
    HOST: str = os.getenv("MONGO_HOST", "localhost")
    PORT: int = int(os.getenv("MONGO_PORT", "27017"))
    USER: str = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
    PASSWORD: str = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "password123")
    DATABASE: str = os.getenv("MONGO_DB_NAME", "smartcity_landing")

    @classmethod
    def get_uri(cls) -> str:
        return f"mongodb://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/?authSource=admin"
