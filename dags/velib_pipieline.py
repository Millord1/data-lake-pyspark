from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag, task


@dag(
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["velib"],
)
def velib_pipeline():
    @task
    def ingest():
        from src.ingestion.run_ingestion import run_ingestion

        run_ingestion()

    clean = SparkSubmitOperator(
        task_id="clean",
        application="/opt/airflow/src/clean/run_clean.py",
        conn_id="spark_default",
        name="velib-clean",
    )

    analyse = SparkSubmitOperator(
        task_id="analyse",
        application="/opt/airflow/src/analysis/analyse.py",
        conn_id="spark_default",
        name="velib-analyse",
    )

    ingest() >> clean >> analyse


velib_pipeline()
