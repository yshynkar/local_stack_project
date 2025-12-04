from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3

# --- НАСТРОЙКИ ---
# Внутри Docker обращание к LocalStack по имени контейнера
AWS_ENDPOINT = "http://localstack:4566"
AWS_REGION = "us-east-1"
AWS_KEY = "test"
AWS_SECRET = "test"
BUCKET_NAME = "city-bikes"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}


def get_s3_client():
    """Создает клиент для работы с нашим локальным S3"""
    return boto3.client(
        's3',
        endpoint_url=AWS_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )


def upload_files_to_s3(**context):
    """
    1. Создает бакет (если нет).
    2. Ищет файлы в папке data/monthly.
    3. Грузит их в S3.
    """
    s3 = get_s3_client()

    # 1. Создал бакет
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} created.")
    except Exception as e:
        # Если бакет уже есть, это нормально
        print(f"Bucket info: {e}")

    # 2. Путь к данным внутри Docker контейнера
    # (примонтировал локальную папку data в /opt/airflow/data)
    folder_path = "/opt/airflow/data/monthly"

    if not os.path.exists(folder_path):
        print(f"ERROR: Folder {folder_path} not found inside Docker!")
        return

    # 3. Загрузка
    files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    print(f"Found {len(files)} files to upload.")

    for filename in files:
        file_path = os.path.join(folder_path, filename)
        s3_key = f"raw/{filename}"  # Путь в S3: raw/имя_файла

        print(f"Uploading {filename} to s3://{BUCKET_NAME}/{s3_key}...")
        s3.upload_file(file_path, BUCKET_NAME, s3_key)


with DAG('helsinki_bikes_upload',
         default_args=default_args,
         schedule_interval=None,  # Запуск только вручную
         catchup=False) as dag:
    upload_task = PythonOperator(
        task_id='upload_raw_data',
        python_callable=upload_files_to_s3
    )
