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
    USER: str | None = os.getenv("MONGO_USER")
    PASSWORD: str | None = os.getenv("MONGO_PASSWORD")
    DATABASE: str | None = os.getenv("MONGO_DB_NAME")

    @classmethod
    def _validate(cls) -> None:
        """Ensure all required database credentials exist in the environment."""
        missing = [
            key
            for key, val in {
                "MONGO_USER": cls.USER,
                "MONGO_PASSWORD": cls.PASSWORD,
                "MONGO_DB_NAME": cls.DATABASE,
            }.items()
            if not val
        ]
        if missing:
            raise ValueError(
                f"Missing critical environment variable(s): {', '.join(missing)}. "
                f"Check your .env file."
            )

    @classmethod
    def get_uri(cls) -> str:
        """Return the formatted MongoDB connection URI."""
        cls._validate()
        return f"mongodb://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/?authSource=admin"

    @classmethod
    def get_database_name(cls) -> str:
        """Return the target database name."""
        cls._validate()
        return cls.DATABASE  # type: ignore[return-value]
