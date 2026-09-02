from src.config.database import AnalysisConfig, StorageConfig

output_path = StorageConfig.get_bucket_path(
    raw=False,
    dataset_name=AnalysisConfig.view_name,
).replace("s3", "s3a")

print(output_path)
