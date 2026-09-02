from src.analysis.spark_driver import SparkConnector
from src.config.database import AnalysisConfig, AnalysisQueries, AnalysisQuery


def run_analysis():
    config = AnalysisConfig()
    queries = AnalysisQueries(config.sql_file)

    with SparkConnector() as spark:
        df = spark.get_data()

        spark.create_view(df, config.view_name)

        result = spark.sql(queries.get(AnalysisQuery.TEST_QUERY))

        result.show()
