from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from project root
load_dotenv('/opt/airflow/project/.env')

# Get API credentials
DHS_API_KEY = os.getenv('DHS_API_KEY')
DHS_APP_TOKEN = os.getenv('DHS_APP_TOKEN')

# API endpoints
HISTORICAL_API_ENDPT = "https://data.cityofnewyork.us/api/v3/views/dwrg-kzni/query.json"
DAILY_API_ENDPT = "https://data.cityofnewyork.us/api/v3/views/k46n-sa2m/query.json"

# S3 Bucket
S3_BUCKET = "dhs-shelter-data"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dhs_historical_load',
    default_args=default_args,
    description='One-time load of historical DHS shelter data',
    schedule_interval=None,
    catchup=False,
    tags=['dhs','historical','initial-load']
)

def extract_upload_historical_data(**context):
    """
    Extract all historical data from 2013-2021 (historical endpoint)
    """
    print("Retrieving historical data from DHS API")

    headers = {
        'X-App-Token': DHS_APP_TOKEN
    }

    params = {
        '$limit': 5000,
        '$$app_token': DHS_APP_TOKEN
    }

    response = requests.get(HISTORICAL_API_ENDPT, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    print(f"Extracted {len(data)} historical records")

    # Upload to S3
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"raw/shelter_census/historical/historical_data_{timestamp}.json"

    s3_hook.load_string(
        string_data=json.dumps(data, indent=2),
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True
    )

    print(f"Uploaded {len(data)} records to s3://{S3_BUCKET}/{s3_key}")

    # Push the S3 path to XCom
    context['ti'].xcom_push(key='historical_s3_key', value=s3_key)
    context['ti'].xcom_push(key='historical_record_count', value=len(data))

    return s3_key

def extract_upload_daily_data(**context):
    """
    Extract all current daily data frorm 2021-present (daily endpoint)
    """
    print("Retrieving daily data from DHS API")

    headers = {
        'X-App-Token': DHS_APP_TOKEN
    }

    params = {
        '$limit': 5000,
        '$$app_token': DHS_APP_TOKEN
    }

    response = requests.get(DAILY_API_ENDPT, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    print(f"Extracted {len(data)} records from Daily API.")

    # Upload to S3
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"raw/shelter_census/daily/daily_data_{timestamp}.json"

    s3_hook.load_string(
        string_data=json.dumps(data, indent=2),
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True
    )
    print(f"Uploaded {len(data)} records to s3://{S3_BUCKET}/{s3_key}")

    # Push S3 path to XCom
    context['ti'].xcom_push(key='daily_s3_key', value=s3_key)
    context['ti'].xcom_push(key='daily_record_count', value=len(data))

    return s3_key

def verify_s3_uploads(**context):
    """
    Verify both files were successfully uploaded to S3
    """
    ti = context['ti']

    # Get S3 keys and record counts stored in XCom from prior tasks
    historical_key = ti.xcom_pull(key='historical_s3_key', task_ids='extract_upload_historical')
    historical_record_count = ti.xcom_pull(key='historical_record_count', task_ids='extract_upload_historical')

    daily_key = ti.xcom_pull(key='daily_s3_key', task_ids='extract_upload_daily')
    daily_record_count = ti.xcom_pull(key='daily_record_count', task_ids='extract_upload_daily')

    # print summary
    print(f"Historical API: {historical_record_count} records at s3://{S3_BUCKET}/{historical_key}")
    print(f"Daily API: {daily_record_count} records at s3://{S3_BUCKET}/{daily_key}")
    print(f"TOTAL RECORDS: {historical_record_count + daily_record_count}")

    # Push summary to XCom for next task
    context['ti'].xcom_push(key='total_records', value=historical_record_count + daily_record_count)
    context['ti'].xcom_push(key='historical_s3_path', value=f's3://{S3_BUCKET}/{historical_key}')
    context['ti'].xcom_push(key='daily_s3_path', value=f's3://{S3_BUCKET}/{daily_key}')

    return "S3 Upload Verification Completed"


def load_to_snowflake(**context):
    """
    Load data from S3 into Snowflake "COPY INTO" statement
    """
    ti = context['ti']

    total_records = ti.xcom_pull(task_ids='verify_s3_uploads', key='total_records')
    historical_path = ti.xcom_pull(task_ids='verify_s3_uploads', key='historical_s3_path')
    daily_path = ti.xcom_pull(task_ids='verify_s3_uploads', key='daily_s3_path')

    print(f"Initializing load of {total_records} into Snowflake...")
    print(f"Historical API file: {historical_path}")
    print(f"Daily API file: {daily_path}")

    return "Success"

# Define tasks
extract_historical_task = PythonOperator(
    task_id='extract_upload_historical',
    python_callable=extract_upload_historical_data,
    dag=dag
)

extract_daily_task = PythonOperator(
    task_id='extract_upload_daily',
    python_callable=extract_upload_daily_data,
    dag=dag
)

verify_s3_upload_task = PythonOperator(
    task_id='verify_s3_uploads',
    python_callable=verify_s3_uploads,
    dag=dag
)

load_snowflake_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag
)

# Configure dependencies for DAG
[extract_historical_task, extract_daily_task] >> verify_s3_upload_task >> load_snowflake_task


