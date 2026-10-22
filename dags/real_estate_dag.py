from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from kafka_producer.producer import Producer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_TOPIC = 'real_estate_topic'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def read_data(file_paths):
    try:
        logger.info("Reading Excel files...")
        dataframes = [pd.read_excel(file) for file in file_paths]
        df = pd.concat(dataframes, ignore_index=True)
        df.to_csv('/home/airflow/data/combined_rentals.csv', index=False)
        logger.info("Data successfully saved to /home/airflow/data/combined_rentals.csv")
    except Exception as e:
        logger.error(f"Error reading data: {e}")
        raise


def preprocess_data():
    try:
        logger.info("Preprocessing data...")
        df = pd.read_csv('/home/airflow/data/combined_rentals.csv')
        df = df.dropna()
        df['price'] = df['price'].apply(persian_to_uint)
        df['price_per_meter'] = df['price_per_meter'].apply(persian_to_uint)
        df['total_price'] = df['total_price'].apply(persian_to_uint)
        df['rooms'] = df['rooms'].apply(persian_to_uint)
        df['metrage'] = df['metrage'].astype(int)
        df['build_year'] = df['build_year'].apply(persian_to_uint).astype(int)

        df.to_csv('/home/airflow/data/processed_rentals.csv', index=False)
        logger.info(f"Data successfully preprocessed and saved to /home/airflow/data/processed_rentals.csv")
    except Exception as e:
        logger.error(f"Error preprocessing data: {e}")
        raise


def send_to_kafka(topic):
    try:
        logger.info("Sending data to Kafka...")
        df = pd.read_csv('/home/airflow/data/processed_rentals.csv')
        producer = Producer()
        for _, row in df.iterrows():
            message = row.to_json()
            producer.send(topic, key=str(row['title']), value=message)
        logger.info("All data successfully sent to Kafka")
    except Exception as e:
        logger.error(f"Error sending data to Kafka: {e}")
        raise


def persian_to_uint(persian_str):
    persian_to_english_map = {
        '۰': '0', '۱': '1', '۲': '2', '۳': '3', '۴': '4',
        '۵': '5', '۶': '6', '۷': '7', '۸': '8', '۹': '9'
    }

    # Remove the 'تومان' or any other currency symbol
    clean_str = persian_str.replace('تومان', '').strip()
    clean_str = clean_str.replace('قبل از', '').strip()
    clean_str = clean_str.replace('بدون اتاق', '0').strip()
    clean_str = clean_str.replace('رایگان', '0').strip()
    clean_str = clean_str.replace('مجانی', '0').strip()

    # Replace Persian numerals with English numerals
    for persian_num, english_num in persian_to_english_map.items():
        clean_str = clean_str.replace(persian_num, english_num)

    # Remove commas
    clean_str = clean_str.replace(',', '')
    clean_str = clean_str.replace('٬', '')

    # Convert to unsigned integer
    return int(clean_str)


with DAG(
        "real_estate_pipeline",
        default_args=default_args,
        description="Complete pipeline for real estate data processing",
        schedule_interval="*/30 * * * *",
        catchup=False,
) as dag:
    scrap_task = BashOperator(
        task_id="scrap_real_estate",
        bash_command="python /opt/airflow/scraper/divar_scraper.py",
    )

    task_read = PythonOperator(
        task_id='read_data',
        python_callable=read_data,
        op_kwargs={'file_paths': [
            "/home/airflow/data/real_estate_tehran.xlsx",
            "/home/airflow/data/real_estate_mashhad.xlsx",
            "/home/airflow/data/real_estate_isfahan.xlsx"
        ]},
    )

    task_preprocess = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
    )

    task_send_to_kafka = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka,
        op_kwargs={'topic': KAFKA_TOPIC},
    )

    scrap_task >> task_read >> task_preprocess >> task_send_to_kafka
