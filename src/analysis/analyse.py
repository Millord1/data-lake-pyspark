from src.config.database import (
    AnalysisConfig,
    AnalysisQueries,
    AnalysisQuery,
    StorageConfig,
)
from src.driver.spark_driver import SparkConnector


def run_analysis():
    queries = AnalysisQueries(AnalysisConfig.sql_file)

    with SparkConnector() as spark:
        df = spark.get_curated_data()

        spark.create_view(df, AnalysisConfig.view_name)

        for query_type in AnalysisQuery:
            result = spark.sql(queries.get(query_type))

            output_path = StorageConfig.get_analysis_path(query_type)

            result.coalesce(1).write.mode("overwrite").option("header", True).csv(
                output_path
            )


if __name__ == "__main__":
    run_analysis()
