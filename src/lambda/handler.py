import json
import boto3
import pandas as pd
import io
import os
import logging
from datetime import datetime
from decimal import Decimal

# Настройка логирования
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Конфигурация через переменные окружения (Best Practice)
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'BikeMetrics')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
ENDPOINT_URL = os.environ.get('AWS_ENDPOINT_URL', 'http://localstack:4566')

# Инициализация клиентов (вне хендлера для переиспользования)
s3_client = boto3.client('s3', region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)
table = dynamodb.Table(DYNAMODB_TABLE)


class MetricsCalculator:
    """
    Service class responsible for calculating daily aggregation metrics.
    Follows Single Responsibility Principle.
    """

    @staticmethod
    def calculate_daily_stats(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates AVG distance, duration, speed, and temperature per day.
        """
        # Преобразование типов
        df['Departure'] = pd.to_datetime(df['Departure'])

        # Расчет скорости (км/ч), если её нет (Distance m / Duration sec * 3.6)
        # Предполагаем колонки: 'Covered distance (m)', 'Duration (sec)', 'Air temperature (degC)'
        # В реальном файле имена могут отличаться, используем стандартизированные
        df['speed_kmh'] = (df['Covered distance (m)'] / df['Duration (sec)']) * 3.6

        # Группировка по дням
        daily_stats = df.groupby(df['Departure'].dt.date).agg({
            'Covered distance (m)': 'mean',
            'Duration (sec)': 'mean',
            'speed_kmh': 'mean',
            'Air temperature (degC)': 'mean'
        }).reset_index()

        daily_stats.columns = ['date', 'avg_distance', 'avg_duration', 'avg_speed', 'avg_temp']
        return daily_stats


def lambda_handler(event, context):
    """
    Main entry point for AWS Lambda.
    Triggered by SNS notification containing S3 event details.
    """
    logger.info("Received event: %s", json.dumps(event))

    try:
        # 1. Парсинг события (SNS -> S3)
        # SNS оборачивает сообщение S3 в поле 'Message'
        sns_message = json.loads(event['Records'][0]['Sns']['Message'])
        s3_record = sns_message['Records'][0]
        bucket_name = s3_record['s3']['bucket']['name']
        file_key = s3_record['s3']['object']['key']

        logger.info(f"Processing file: s3://{bucket_name}/{file_key}")

        # 2. Чтение файла из S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read()
        df = pd.read_csv(io.BytesIO(content))

        # 3. Расчет метрик
        stats_df = MetricsCalculator.calculate_daily_stats(df)

        # 4. Запись в DynamoDB
        with table.batch_writer() as batch:
            for _, row in stats_df.iterrows():
                item = {
                    'Date': str(row['date']),  # Partition Key
                    'Type': 'DailyAgg',  # Sort Key (для удобства выборки)
                    'AvgDistance': Decimal(str(round(row['avg_distance'], 2))),
                    'AvgDuration': Decimal(str(round(row['avg_duration'], 2))),
                    'AvgSpeed': Decimal(str(round(row['avg_speed'], 2))),
                    'AvgTemp': Decimal(str(round(row['avg_temp'], 2))),
                    'ProcessedAt': datetime.utcnow().isoformat()
                }
                batch.put_item(Item=item)

        logger.info(f"Successfully processed {len(stats_df)} records.")
        return {"statusCode": 200, "body": "Success"}

    except Exception as e:
        logger.error(f"Error processing lambda: {str(e)}")
        raise e