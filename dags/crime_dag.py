from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "ferry",
    "retry_delay": timedelta(minutes=5),
}

def extract_file():
    url = ""
    output = 'opt/airflow/dataset/la_crime.csv'
    gdown.download(url, output, quiet=False)

