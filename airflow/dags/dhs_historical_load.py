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

def extract_historical_data(**context):
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

    # Store in XCom for pass to next task
    context['ti'].xcom_push(key='historical_data', value=data)

    return len(data)

def extract_daily_data(**context):
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

    # Store in XCom for pass to next task
    context['ti'].xcom_push(key='daily_data', value=data)

    return len(data)

def upload_to_s3(**context):
    """
    Upload extracted data to S3 as JSON files
    """
    # Get data from previous tasks
    ti = context['ti']

    historical_data = ti.xcom_pull(task_ids='extract_historical_data', key='historical_data')
    daily_data = ti.xcom_pull(task_ids='extract_daily_data', key='daily_data')

    # Combine datasets
    full_data = historical_data + daily_data
    print(f"Total records to upload: {len(full_data)}")

    # Use S3 Hook through Airflow
    s3_hook = S3Hook(aws_conn_id='aws_conn')

    # Create S3 Key
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"raw/shelter_census/historical/full_load_{timestamp}.json"

    # Upload to S3 using S3 Hook
    s3_hook.load_string(
        string_data=json.dumps(full_data, indent=2),
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True
    )

    print(f"Successfully uploaded data to S3: {S3_BUCKET}/{s3_key}")

    # Store S3 path for next task
    context['ti'].xcom_push(key='s3_path', value=f"s3://{S3_BUCKET}/{s3_key}")

    return s3_key

def load_to_snowflake(**context):
    pass


