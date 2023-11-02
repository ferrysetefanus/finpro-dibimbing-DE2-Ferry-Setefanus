from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import requests

default_args = {
    "owner": "ferry",
    "retry_delay": timedelta(minutes=5),
}

def extract_file():
    url = "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"
    output = '/opt/airflow/datasets/la_crime.csv'
    
    with requests.get(url, stream=True) as response:
        with open(output, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)


with DAG(
    dag_id="crime_dag",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60),
    description="dag for crime analysis",
    start_date=days_ago(1)
) as dag:
    
    start = DummyOperator(task_id="start")

    extract_file = PythonOperator(
        task_id="extract_file",
        python_callable=extract_file
    )

    transform = SparkSubmitOperator(
        
    )

    end = DummyOperator(task_id="end")

    start >> extract_file >> end