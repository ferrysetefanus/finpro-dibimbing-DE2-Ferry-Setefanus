import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
import pyspark.sql.functions as F
from pyspark.sql.types import *

dotenv_path = Path('../.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_HOST')
metabase_db = os.getenv('METABASE_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

spark_master = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
script_path = "/spark-scripts/postgresql-42.6.0.jar"

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('la_crime')
        .setMaster(f'spark://{spark_master}:{spark_port}')
        .set("spark.jars", script_path)
    ))

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}/{metabase_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringType': 'unspecified'
}

crime_df = spark.read.csv("/opt/datasets/la_crime.csv")

transformed_crime_df = crime_df.select("*")









