from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone

def write_ciao():
    with open('/opt/airflow/dags/ciao.txt', 'w', encoding='utf-8') as f:
        f.write('ciao')

with DAG(
    dag_id='test_txt_dag',
    start_date=datetime(2025, 9, 15, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
) as dag:
    write_ciao_task = PythonOperator(
        task_id='write_ciao',
        python_callable=write_ciao,
    )
