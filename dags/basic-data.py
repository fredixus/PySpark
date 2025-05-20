from airflow.decorators import dag, task
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def bacis_csv_file():

    submit_job = SparkSubmitOperator(
        task_id="submit_job",
        conn_id="basic_csv_file",
        application="include/scripts/read.py",
        verbose=True,
    )

    submit_job

bacis_csv_file()