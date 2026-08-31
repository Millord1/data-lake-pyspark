from enum import StrEnum


class StorageConfig:
    BUCKET_NAME = "data"
    RAW_FOLDER = "raw"
    CLEANED_FOLDER = "curated"
    SCHEMA_NAME = "public"

    @classmethod
    def get_bucket_path(cls, raw: bool = True) -> str:
        """Generate the bucket path to differenciate raw and curated

        Args:
            raw (bool, optional): raw path or curated. Default on raw

        Returns:
            str: S3 bucket path
        """
        folder = cls.RAW_FOLDER if raw else cls.CLEANED_FOLDER
        return f"s3://{cls.BUCKET_NAME}/{folder}/{cls.SCHEMA_NAME}"


class TableNames(StrEnum):
    agency: str = "agency"
    calendar: str = "calendar"
    calendar_dates: str = "calendar_dates"
    routes: str = "routes"
    stop_times: str = "stop_times"
    stops: str = "stops"
    transfers: str = "transfers"
    trips: str = "trips"
