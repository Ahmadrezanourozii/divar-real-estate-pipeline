from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
from datetime import datetime
from dags.real_estate_dag import read_data, preprocess_data, send_to_kafka

# Kafka Configuration
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'migrate_already_crawled_data_topic'

# Set up logging
logger = logging.getLogger(__name__)

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
}

with DAG(
        "migrate_already_crawled_data_pipeline",
        default_args=default_args,
        description="send already crawled data to kafka and clickhouse",
        catchup=False,
) as dag:
    task_read = PythonOperator(
        task_id='read_data',
        python_callable=read_data,
        op_kwargs={'file_paths': [
            "/home/airflow/data/already_crawled_real_estate_tehran.xlsx",
            "/home/airflow/data/already_crawled_real_estate_mashhad.xlsx",
            "/home/airflow/data/already_crawled_real_estate_isfahan.xlsx"
        ]},
    )

    task_preprocess = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
    )

    task_send_to_kafka = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka,
        op_kwargs={
            'topic': KAFKA_TOPIC,
        },
    )

    task_read >> task_preprocess >> task_send_to_kafka
