from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def test_run():
    print("Airflow is running.")
    return True

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2026,1,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    description='Test DAG',
    schedule_interval=None,
    catchup=False
)

test_task = PythonOperator(
    task_id='test_print',
    python_callable=test_run,
    dag=dag
)