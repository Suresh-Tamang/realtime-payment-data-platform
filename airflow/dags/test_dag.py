from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=30),
    "email_on_failure": True,
    "email": ["sureshtamangj@gmail.com"],
}

with DAG(
    dag_id="daily_settlement_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["payments", "analytics", "staging"],
) as dag:

    # silver layer parquet file data to Postgres Warehouse
    silver_to_warehouse = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/spark-apps/batch/silver_to_warehouse.py",
        conn_id="spark_default",
        application_args=[],
        conf={"spark.master": "spark://spark-master:7077"},
    )
    
