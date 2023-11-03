#import semua library yang dibutuhkan
import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import to_date
from itertools import chain

# mendefinisikan path menuju file .env agar variabel yang sudah dideklarasikan dapat terbaca
dotenv_path = Path('/.env')
load_dotenv(dotenv_path=dotenv_path)

# mendefinisikan variabel untuk menampung nilai variabel yang sudah ditentukan pada file .env
postgres_host = os.getenv('POSTGRES_HOST')
metabase_db = os.getenv('METABASE_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
spark_master = os.getenv('SPARK_MASTER_HOST_NAME')
spark_port = os.getenv('SPARK_MASTER_PORT')
jar_path = "/jars/postgresql-42.2.18.jar"

# mendefinisikan spark session
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('la_crime')
        .setMaster(f'spark://{spark_master}:{spark_port}')
        .set("spark.jars", jar_path)
    ))

sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# mendefinisikan koneksi ke hydra
jdbc_url = f'jdbc:postgresql://{postgres_host}/{metabase_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringType': 'unspecified'
}

# membaca file csv yang akan ditransform
crime_df = spark.read.csv("/datasets/la_crime.csv", header=True, inferSchema=True)

# membuat salinan file csv yang akan ditransform
transformed_crime_df = crime_df.select("*")

# Melakukan caching DataFrame di disk
transformed_crime_df.persist(storageLevel=pyspark.StorageLevel.DISK_ONLY)

# drop kolom - kolom yang tidak dibutuhkan
transformed_crime_df = transformed_crime_df.drop(*['DR_NO', 'AREA', 'Rpt Dist No', 'Part 1-2', 'Crm Cd',
                         'Mocodes', 'Premis Cd', 'Weapon Used Cd', 'Status', 'Crm Cd 1',
                         'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4', 'Cross Street'])

# melakukan rename terhadap kolom kolom yang digunakan
transformed_crime_df = transformed_crime_df.withColumnRenamed("Date Rptd", "date_reported") \
        .withColumnRenamed("DATE OCC", "date_occurance") \
        .withColumnRenamed("TIME OCC", "time_occurance") \
        .withColumnRenamed("AREA NAME", "area_name") \
        .withColumnRenamed("Crm Cd Desc", "crime_description") \
        .withColumnRenamed("Vict Age", "victim_age") \
        .withColumnRenamed("Vict Sex", "victim_sex") \
        .withColumnRenamed("Vict Descent", "victim_descendant") \
        .withColumnRenamed("Premis Desc", "premis_description") \
        .withColumnRenamed("Weapon Desc", "weapon_description") \
        .withColumnRenamed("Status Desc", "case_status") \
        .withColumnRenamed("LOCATION", "location") \
        .withColumnRenamed("LAT", "lat") \
        .withColumnRenamed("LON", "lon")


# Menggunakan to_timestamp untuk mengubah tipe data kolom date_reported dan date_occurance
transformed_crime_df = transformed_crime_df.withColumn("date_reported", to_date(transformed_crime_df["date_reported"], "MM/dd/yyyy hh:mm:ss a"))
transformed_crime_df = transformed_crime_df.withColumn("date_occurance", to_date(transformed_crime_df["date_occurance"], "MM/dd/yyyy hh:mm:ss a"))

# Mengubah kolom "time_occurance" menjadi tipe data string
transformed_crime_df = transformed_crime_df.withColumn("time_occurance", F.col("time_occurance").cast("string"))

# Mengubah format waktu
transformed_crime_df = transformed_crime_df.withColumn("time_occurance",
                   F.expr("concat(lpad(substring(time_occurance, 1, length(time_occurance) - 2), 2, '0'), ':', substring(time_occurance, -2, 2))"))

# Mengubah format waktu menjadi 'HH:mm'
transformed_crime_df = transformed_crime_df.withColumn("time_occurance", F.expr("date_format(time_occurance, 'HH:mm')"))


# Mengisi nilai null dalam kolom "weapon description" dengan "UNKNOWN WEAPON/OTHER WEAPON"
transformed_crime_df = transformed_crime_df.fillna({'weapon_description': 'UNKNOWN WEAPON/OTHER WEAPON'})

# Menggunakan fungsi abs untuk membuat nilai negatif menjadi positif dalam kolom "victim age"
transformed_crime_df = transformed_crime_df.withColumn("victim_age", F.expr("abs(victim_age)"))

# Menggunakan regexp_replace untuk mengganti spasi ganda dengan satu spasi
transformed_crime_df = transformed_crime_df.withColumn("location", F.regexp_replace(F.col("location"), "\\s+", " "))

# Menggunakan trim untuk menghilangkan spasi tambahan di awal dan akhir teks
transformed_crime_df = transformed_crime_df.withColumn("location", F.trim(F.col("location")))

# Melakukan mapping terhadap value yang ada di kolom victim_sex dan victim_descendant
sex_mapping = {'F': 'female', 'M': 'male', 'X': 'unknown', 'H': 'female',  '-': 'unknown'}
desc_mapping = {'A': 'other asian', 'B': 'black', 'C': 'chinese',
                'D': 'cambodian', 'F': 'filipino', 'G': 'guamanian',
                'H': 'hispanic/latin/mexican', 'I': 'american indian/alaskan native',
                'J': 'japanese', 'K': 'korean', 'L': 'laotian', 'O': 'other', 'P': 'pacific islander',
                'S': 'samoan', 'U': 'hawaiian', 'V': 'vietnamese', 'W': 'white',
                'X': 'unknown', 'Z': 'asian indian',
                }

sex_mapping_expr = F.create_map([F.lit(x) for x in chain(*sex_mapping.items())])
desc_mapping_expr = F.create_map([F.lit(x) for x in chain(*desc_mapping.items())])

transformed_crime_df = transformed_crime_df.withColumn('victim_sex', sex_mapping_expr[transformed_crime_df['victim_sex']])
transformed_crime_df = transformed_crime_df.withColumn('victim_descendant', desc_mapping_expr[transformed_crime_df['victim_descendant']])


# Mengisi nilai null dalam kolom "victim sex" dengan "unknown"
transformed_crime_df = transformed_crime_df.fillna({'victim_sex': 'unknown'})

# Mengisi nilai null dalam kolom "victim descendant" dengan "unknown"
transformed_crime_df = transformed_crime_df.fillna({'victim_descendant': 'unknown'})

# write hasil transformasi dataframe ke hydra postgres
transformed_crime_df.write.mode("overwrite").jdbc(url=jdbc_url, table="la_crime", mode="overwrite", properties=jdbc_properties)

# unpersist untuk membebaskan memory
transformed_crime_df.unpersist()
