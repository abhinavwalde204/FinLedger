from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "finledger",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

def validate_source(**ctx):
    import psycopg2
    conn = psycopg2.connect(
        host="localhost", dbname="finledger_oltp",
        user="admin", password="secret"
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM transactions")
    count = cur.fetchone()[0]
    print(f"Source transaction count: {count}")
    if count < 10:
        raise ValueError(f"Too few records: {count}")
    conn.close()

def validate_warehouse(**ctx):
    import duckdb
    con = duckdb.connect('/workspaces/FinLedger/finledger_warehouse.duckdb')
    count = con.execute("SELECT COUNT(*) FROM main.stg_sessions").fetchone()[0]
    print(f"Warehouse session count: {count}")
    if count == 0:
        raise ValueError("No sessions in warehouse!")
    con.close()

def run_ge(**ctx):
    result = subprocess.run(
        ["python3", "/workspaces/FinLedger/great_expectations/ge_suite.py"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if "Success: False" in result.stdout:
        raise ValueError("GE quality contract breached!")
    print("Quality check passed!")

def sync_parquet(**ctx):
    import boto3, os
    s3 = boto3.client('s3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    os.makedirs('/workspaces/FinLedger/data/bronze/sessions', exist_ok=True)
    paginator = s3.get_paginator('list_objects_v2')
    count = 0
    for page in paginator.paginate(Bucket='finledger-lake', Prefix='bronze/sessions/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            filename = key.replace('/', '_')
            local_path = f'/workspaces/FinLedger/data/bronze/sessions/{filename}'
            s3.download_file('finledger-lake', key, local_path)
            count += 1
    print(f"Synced {count} parquet files from MinIO")

with DAG(
    dag_id="finledger_daily_pipeline",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finledger"]
) as dag:

    t1 = PythonOperator(
        task_id="validate_source",
        python_callable=validate_source
    )
    t2 = PythonOperator(
        task_id="sync_parquet_from_minio",
        python_callable=sync_parquet
    )
    t3 = BashOperator(
        task_id="dbt_run_silver",
        bash_command="cd /workspaces/FinLedger/dbt_project/finledger_dbt && dbt run --select stg_sessions"
    )
    t4 = BashOperator(
        task_id="dbt_run_gold",
        bash_command="cd /workspaces/FinLedger/dbt_project/finledger_dbt && dbt run --select fact_anomalies"
    )
    t5 = BashOperator(
        task_id="dbt_test",
        bash_command="cd /workspaces/FinLedger/dbt_project/finledger_dbt && dbt test"
    )
    t6 = PythonOperator(
        task_id="great_expectations_check",
        python_callable=run_ge
    )
    t7 = PythonOperator(
        task_id="validate_warehouse",
        python_callable=validate_warehouse
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7