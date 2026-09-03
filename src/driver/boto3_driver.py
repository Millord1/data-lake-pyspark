import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


class S3Connector:
    """Connecteur S3 compatible avec MinIO."""

    def __init__(self):
        endpoint = os.environ.get("MINIO_ENDPOINT")

        if not endpoint.startswith(("http://", "https://")):
            endpoint = f"http://{endpoint}"

        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ.get("MINIO_USER"),
            aws_secret_access_key=os.environ.get("MINIO_PASSWORD"),
            region_name="us-east-1",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()

    def bucket_exists(self, bucket: str) -> bool:
        """Vérifie si un bucket existe."""
        try:
            self.client.head_bucket(Bucket=bucket)
            return True

        except ClientError as error:
            code = error.response["Error"]["Code"]

            if code in ("404", "NoSuchBucket"):
                return False

            raise

    def ensure_bucket(self, bucket: str) -> None:
        """Crée le bucket s'il n'existe pas."""
        if not self.bucket_exists(bucket):
            self.client.create_bucket(Bucket=bucket)

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
    ) -> list[str]:
        response = self.client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
        )

        return [obj["Key"] for obj in response.get("Contents", [])]

    def download_file(
        self,
        bucket: str,
        key: str,
        local_path: str,
    ) -> None:
        self.client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=local_path,
        )

    def exists(self, bucket: str, key: str) -> bool:
        """Vérifie si un objet existe dans un bucket."""
        try:
            self.client.head_object(
                Bucket=bucket,
                Key=key,
            )
            return True

        except ClientError as error:
            code = error.response["Error"]["Code"]

            if code in ("404", "NoSuchKey", "NotFound"):
                return False

            raise

    def upload_file(
        self,
        local_path: str,
        bucket: str,
        key: str,
    ) -> None:
        """Upload un fichier local vers S3/MinIO."""
        self.client.upload_file(
            Filename=local_path,
            Bucket=bucket,
            Key=key,
        )
