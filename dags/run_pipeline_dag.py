from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from run_pipeline import run_pipeline


# DEFAULT ARGS
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 23),
    'email': 'ossained22k7@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# DAG DEFINITION
with DAG(
    dag_id='financial_market',
    default_args=default_args,
    description='This represents financial market Data Management pipeline',
    schedule='@daily',
    catchup=False,
    tags=['finance', 'etl', 'stock']
) as dag:

    run_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline
    )
