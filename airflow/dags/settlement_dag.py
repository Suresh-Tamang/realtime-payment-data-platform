from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla":timedelta(minutes=5),
    "email_on_failure": True,
    "email":["sureshtamangj@gmail.com"],
}

with DAG(
    dag_id="daily_settlement_report",
    start_date=datetime(2026,1,1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["payments","analytics","staging"],
) as dag:
    # To generate daily settlement report
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        docker exec dbt dbt run 
        """
    )
    # To generate daily settlement report
    daily_settlement = BashOperator(
        task_id="payment_settlement",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/spark-apps/reports/settlement_daily.py
        """
    )
    
    # To generate daily fraud settlement report
    daily_fraud_settlement = BashOperator(
        task_id="fraud_settlement",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/spark-apps/reports/fraud_settlement_daily.py
        """
    )
    
    dbt_run>>daily_settlement >> daily_fraud_settlement