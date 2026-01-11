from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="fraud_medallion_pipeline_hourly",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=['spark', 'fraud_payments']
) as dag:

    # Task 1: Clean Bronze to Silver
    fraud_bronze_to_silver = BashOperator(
        task_id="bronze_fraud_to_silver_task",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark-apps/batch/bronze_fraud_to_silver.py
        """
    )

    # Task 2: Aggregate Silver to Gold
    fraud_silver_to_gold = BashOperator(
        task_id="silver_fraud_to_gold_task",
        bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/spark-apps/batch/silver_fraud_to_gold.py
    """
    )

    # CRITICAL: Define the dependency
    fraud_bronze_to_silver >> fraud_silver_to_gold
