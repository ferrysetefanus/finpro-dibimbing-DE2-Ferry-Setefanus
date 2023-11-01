from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import gdown

default_args = {
    "owner": "ferry",
    "retry_delay": timedelta(minutes=5),
}

def extract_file():
    url = "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"
    output = 'opt/airflow/datasets/la_crime.csv'
    gdown.download(url, output, quiet=False)

