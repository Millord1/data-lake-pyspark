import os

import duckdb
from dotenv import load_dotenv

from src.config.database import StorageConfig

load_dotenv()


class DuckDBConnector:
    def __init__(self):
        self.conn = duckdb.connect()
        self._create_bucket(StorageConfig.BUCKET_NAME)

    def __enter__(self):
        self._configure_minio()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def _configure_minio(self) -> None:
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")

        self.conn.execute(
            f"""
            CREATE OR REPLACE SECRET minio (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{os.environ["MINIO_USER"]}',
                SECRET '{os.environ["MINIO_PASSWORD"]}',
                ENDPOINT '{os.environ["MINIO_ENDPOINT"]}',
                REGION 'us-east-1',
                URL_STYLE 'path',
                USE_SSL false
            );
            """
        )

    def _create_bucket(self, bucket: str) -> str:
        """S'assure que le bucket MinIO existe ou le crée via S3/DuckDB."""
        self.conn.execute(f"CREATE BUCKET IF NOT EXISTS s3://{bucket};")
        return bucket

    def execute(self, query: str):
        return self.conn.execute(query)

    def fetchone(self, query: str):
        return self.conn.execute(query).fetchone()

    def fetchall(self, query: str):
        return self.conn.execute(query).fetchall()
