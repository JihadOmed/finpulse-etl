from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import the ETL functions
from scripts.extract_yfinance import extract_data
from scripts.transform_spark import transform_data
from scripts.load_mysql import load_data

default_args = {
    "owner": "jihad",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="yahoo_finance_etl",
    default_args=default_args,
    description="Daily Yahoo Finance ETL: Extract -> Transform (Spark) -> Load (MySQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["finance", "spark", "mysql"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    extract >> transform >> load
