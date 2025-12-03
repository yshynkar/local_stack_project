# local_stack_project
# Helsinki City Bikes ETL Pipeline

## Overview
This project implements a Data Engineering pipeline to process Helsinki City Bikes dataset (2016-2020).
The stack leverages **Docker**, **Airflow**, **Spark**, **AWS LocalStack** (S3, Lambda, SNS, DynamoDB).

## Architecture
1.  **Data Ingestion**: Raw CSV data (10M+ rows) is split into monthly chunks.
2.  **Orchestration (Airflow)**:
    * Uploads raw data to **S3**.
    * Triggers **Spark** jobs for heavy aggregation.
3.  **Processing (Spark)**: Calculates station popularity metrics (Departures/Returns).
4.  **Event Driven (Lambda + SNS)**:
    * S3 upload triggers an SNS notification.
    * **Lambda** function consumes the event.
    * Calculates daily averages (Distance, Speed, Temp) using Pandas.
5.  **Storage**: Final metrics are stored in **DynamoDB**.

## Project Structure
```text
├── dags/                 # Airflow DAGs (Orchestration)
├── data/                 # Local data storage
├── src/
│   ├── lambda/           # Serverless logic
│   └── utils/            # Data splitting helpers
├── docker-compose.yaml   # Infrastructure definition
├── requirements.txt      # Python dependencies
└── README.md             # Documentation
