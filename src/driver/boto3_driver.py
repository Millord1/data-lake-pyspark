import os

import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()


class S3Connector:
    def __init__(self):
        endpoint = os.environ["MINIO_ENDPOINT"]

        if not endpoint.startswith(("http://", "https://")):
            endpoint = f"http://{endpoint}"

        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
            region_name="us-east-1",
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()

    def upload_file(
        self,
        local_path: str,
        bucket: str,
        key: str,
    ) -> str:
        self.client.upload_file(
            local_path,
            bucket,
            key,
        )

        return f"s3://{bucket}/{key}"

    def bucket_exists(self, bucket: str) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket)
            return True
        except Exception:
            return False

    def create_bucket(self, bucket: str) -> None:
        self.client.create_bucket(Bucket=bucket)

    def ensure_bucket(self, bucket: str) -> None:
        if not self.bucket_exists(bucket):
            self.create_bucket(bucket)
