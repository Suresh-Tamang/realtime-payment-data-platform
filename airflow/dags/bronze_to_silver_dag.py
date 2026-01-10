from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="medallion_pipeline_hourly",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=['spark', 'payments']
) as dag:

    # Task 1: Clean Bronze to Silver
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver_task",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit /opt/spark-apps/batch/bronze_to_silver.py
        """
    )

    # Task 2: Aggregate Silver to Gold
    silver_to_gold = BashOperator(
        task_id="silver_to_gold_task",
        bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
        --jars /opt/spark/jars/postgresql-42.7.3.jar \
        /opt/spark-apps/batch/silver_to_gold.py
    """
    )

    # CRITICAL: Define the dependency
    bronze_to_silver >> silver_to_gold
