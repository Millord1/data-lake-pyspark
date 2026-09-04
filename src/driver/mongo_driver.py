import os

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from src.config.database import MongoConfig

load_dotenv()


class MongoConnector:
    """Context manager for MongoDB connectivity for local and container."""

    def __init__(self, database_name: str = MongoConfig.database_name):
        self.database_name = os.environ.get("MONGO_DB_NAME", database_name)

        # 1. Prioritize direct URI if injected by Docker/Airflow
        direct_uri = os.environ.get("MONGO_URI")
        if direct_uri:
            self.uri = direct_uri
        else:
            # 2. Fall back to host/port/auth variables
            user = os.environ.get("MONGO_USER", "admin")
            password = os.environ.get("MONGO_PASSWORD", "password123")
            host = os.environ.get("MONGO_HOST", MongoConfig.default_host)
            port = int(os.environ.get("MONGO_PORT", MongoConfig.default_port))
            self.uri = f"mongodb://{user}:{password}@{host}:{port}/?authSource=admin"

        self.client: MongoClient | None = None
        self.db: Database | None = None

    def __enter__(self):
        self.client = MongoClient(self.uri)
        self.db = self.client[self.database_name]
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client:
            self.client.close()

    def get_collection(
        self, collection_name: str = MongoConfig.collection_name
    ) -> Collection:
        """Return the target collection instance."""
        if self.db is None:
            raise RuntimeError("Database connection not initialized.")
        return self.db[collection_name]

    def document_exists(
        self, query: dict, collection_name: str = MongoConfig.collection_name
    ) -> bool:
        """Check if matching document exists (idempotency check)."""
        collection = self.get_collection(collection_name)
        return collection.count_documents(query, limit=1) > 0
