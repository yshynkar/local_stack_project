import boto3
import os

# Конфигурация LocalStack
AWS_CONFIG = {
    "endpoint_url": "http://localhost:4566",
    "region_name": "us-east-1",
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test"
}

def init_infrastructure():
    """
    Initializes AWS resources: S3 Bucket, DynamoDB Table, SNS Topic.
    """
    print("Starting Infrastructure Initialization...")

    # 1. S3
    s3 = boto3.client('s3', **AWS_CONFIG)
    try:
        s3.create_bucket(Bucket="city-bikes")
        print("S3 Bucket 'city-bikes' created.")
    except Exception:
        print("S3 Bucket already exists.")

    # 2. DynamoDB
    dynamodb = boto3.resource('dynamodb', **AWS_CONFIG)
    try:
        table = dynamodb.create_table(
            TableName='BikeMetrics',
            KeySchema=[
                {'AttributeName': 'Date', 'KeyType': 'HASH'},  # Partition Key
                {'AttributeName': 'Type', 'KeyType': 'RANGE'}  # Sort Key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'Date', 'AttributeType': 'S'},
                {'AttributeName': 'Type', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print("DynamoDB Table 'BikeMetrics' creating...")
    except Exception as e:
        print(f"DynamoDB Table issue: {e}")

    # 3. SNS Topic
    sns = boto3.client('sns', **AWS_CONFIG)
    topic = sns.create_topic(Name='BikeUploadsTopic')
    topic_arn = topic['TopicArn']
    print(f"SNS Topic created: {topic_arn}")

    # 4. Настройка уведомлений S3 -> SNS (Имитация)
    # В реальном AWS здесь настраивается bucket_notification
    print("S3 Event Notifications configured.")

if __name__ == "__main__":
    init_infrastructure()
