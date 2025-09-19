from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def bronze_etl_task():
    from bronze_etl_pipeline import main as bronze_main
    bronze_main()

def gold_etl_task():
    from gold_etl_pipeline import gold_etl_task as gold_main
    gold_main()

def silver_etl_task():
    from silver_etl_pipeline import main as silver_main
    silver_main()

with DAG(
    dag_id='etl_pipeline_dag',
    default_args=default_args,
    description='ETL pipeline DAG',
    schedule_interval='0 0 * * *',  # Esegui ogni giorno a mezzanotte
    start_date=datetime(2025, 9, 15, tzinfo=timezone.utc),
    catchup=False,
) as dag:
    bronze = PythonOperator(
        task_id="bronze_etl",
        python_callable=bronze_etl_task,
    )
    silver = PythonOperator(
        task_id="silver_etl",
        python_callable=silver_etl_task,
    )
    gold = PythonOperator(
        task_id="gold_etl",
        python_callable=gold_etl_task,
    )
    bronze >> silver >> gold