from src.config.database import AnalysisConfig, AnalysisQueries, AnalysisQuery
from src.driver.spark_driver import SparkConnector


def run_analysis():
    queries = AnalysisQueries(AnalysisConfig.sql_file)

    with SparkConnector() as spark:
        df = spark.get_data()

        spark.create_view(df, AnalysisConfig.view_name)

        result = spark.sql(queries.get(AnalysisQuery.TEST_QUERY))

        result.show()
