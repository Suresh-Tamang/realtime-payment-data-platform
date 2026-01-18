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
    dag_id="daily_settlement_report",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["payments", "analytics", "staging"],
) as dag:
    # To generate daily settlement report
    dbt_run_hourly = BashOperator(
        task_id="dbt_run",
        bash_command="""
        docker exec dbt dbt run --project-dir /usr/app/dbt/payments_dbt && \
        docker exec dbt dbt test --project-dir /usr/app/dbt/payments_dbt
        """
    )
    
    dbt_run_hourly