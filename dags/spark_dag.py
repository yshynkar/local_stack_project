from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import shutil

# --- КОНФИГУРАЦИЯ ---
AWS_ENDPOINT = "http://localstack:4566"
AWS_KEY = "test"
AWS_SECRET = "test"
BUCKET_NAME = "city-bikes"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}


def process_data_with_spark(**context):
    """
    1. Скачивает bikes_2020-05.csv из S3.
    2. Запускает Spark для подсчета поездок.
    3. Загружает результаты (departure_stats, return_stats) обратно в S3.
    """
    import boto3
    # Импорт Spark внутри функции, чтобы Airflow не ругался при парсинге DAG
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import count

    # 1. Подключение к S3
    s3 = boto3.client('s3', endpoint_url=AWS_ENDPOINT,
                      aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)

    
    target_file = "bikes_2020-05.csv"
    local_input = f"/tmp/{target_file}"

    print(f"Downloading raw/{target_file} from S3...")
    try:
        s3.download_file(BUCKET_NAME, f"raw/{target_file}", local_input)
    except Exception as e:
        print(f"CRITICAL ERROR: Could not download file. Run the upload DAG first! Error: {e}")
        raise e

    # 2. Запуск Spark
    print("Starting Spark Session...")
    spark = SparkSession.builder \
        .appName("BikeMetrics") \
        .master("local[*]") \
        .getOrCreate()

    # Чтение CSV (с заголовками)
    df = spark.read.option("header", "True").csv(local_input)

    print(f"Rows count: {df.count()}")

    # 3.1 Метрика: Отправления (Departure station name)
    # Группировка по станции и считаем количество
    dep_counts = df.groupBy("Departure station name").count().withColumnRenamed("count", "trips_started")

    # 3.2 Метрика: Возвраты (Return station name)
    ret_counts = df.groupBy("Return station name").count().withColumnRenamed("count", "trips_ended")

    # Сохранение во временную папку
    # Spark сохраняет как папку с part-файлами, поэтому сохраняю в /tmp/deps и /tmp/rets
    out_dep = "/tmp/metrics_departure"
    out_ret = "/tmp/metrics_return"

    # Чистка, если папки уже есть
    if os.path.exists(out_dep): shutil.rmtree(out_dep)
    if os.path.exists(out_ret): shutil.rmtree(out_ret)

    # Coalesce(1) собирает всё в 1 файл
    dep_counts.coalesce(1).write.header(True).csv(out_dep)
    ret_counts.coalesce(1).write.header(True).csv(out_ret)

    # 4. Загрузка результатов обратно в S3
    def upload_spark_result(local_folder, s3_name):
        # Spark создает файлы вида part-00000-....csv
        for f in os.listdir(local_folder):
            if f.endswith(".csv"):
                full_path = os.path.join(local_folder, f)
                s3_key = f"metrics/{s3_name}"
                print(f"Uploading result to s3://{BUCKET_NAME}/{s3_key}")
                s3.upload_file(full_path, BUCKET_NAME, s3_key)

    upload_spark_result(out_dep, "departure_counts.csv")
    upload_spark_result(out_ret, "return_counts.csv")

    spark.stop()
    print("Spark job finished successfully!")


with DAG('helsinki_bikes_spark_job',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    spark_task = PythonOperator(
        task_id='spark_calc_metrics',
        python_callable=process_data_with_spark
    )
