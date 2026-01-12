from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="daily_payment_settlement",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=['spark', 'payments',"settlement"]
) as dag:

    # Task 1: Clean Bronze to Silver
    generate_payment_settlement = BashOperator(
        task_id="daily_payment_settlement",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark-apps/reports/generate_settlement_report.py
        """
    )
    
    generate_payment_settlement

