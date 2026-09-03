from src.config.database import AnalysisConfig, AnalysisQueries, SparkFiles
from src.driver.spark_driver import SparkConnector

with SparkConnector() as spark:
    queries = AnalysisQueries(AnalysisConfig.sql_file)

    df_open = spark.get_curated_data(status=SparkFiles.open_folder)

    spark.create_view(df_open, view_name=AnalysisConfig.curated_view_name)
    res = spark.sql(queries.get(AnalysisQueries.AVERAGE_BIKES))

    res.show(10)
