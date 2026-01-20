from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["sureshtamangj@gmail.com"],
}

with DAG(
    dag_id="hourly_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["payments", "analytics", "staging"],
) as dag:

    # silver layer parquet file data to Postgres Warehouse
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/spark-apps/batch/bronze_to_silver.py
        """
    )
    # silver to warehouse
    silver_to_warehouse = BashOperator(
        task_id = "silver_to_warehouse",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/spark-apps/batch/silver_to_warehouse.py
        """
    )
    
    # To update warehouse hourly
    dbt_run_hourly = BashOperator(
        task_id="dbt_run",
        bash_command="""
        docker exec dbt dbt run 
        """
    )

    bronze_to_silver >> silver_to_warehouse >> dbt_run_hourly
    
    
