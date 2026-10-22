from kafka import KafkaConsumer
import json
import logging
from clickhouse_driver import Client
from typing import Dict, Any
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickhouseWriter:
    def __init__(self):
        self.client = Client(
            host='clickhouse',
            port=9000,
            user='airflow',
            password='airflow',
            database='airflow'
        )

    def insert_data(self, data: Dict[str, Any]):
        try:
            insert_query = """
                        INSERT INTO divar_real_estate (title, metrage, build_year, rooms, total_price, price_per_meter, floor, location, price, url, city, created_at, source)
                        VALUES
                    """

            # Convert dictionary to a tuple (order matters!)
            row = (
                data['title'],
                data['metrage'],
                data['build_year'],
                data['rooms'],
                data['total_price'],
                data['price_per_meter'],
                data['floor'],
                data['location'],
                data['price'],
                data['url'],
                data['city'],
                data.get('created_at', datetime.utcnow()),
                data.get('source', 'divar')
            )
            self.client.execute(insert_query, [row])  # A list of tuples
            logger.info(f"Successfully inserted data for property: {data.get('title', 'Unknown')}")
        except Exception as e:
            logger.error(f"Error inserting data into Clickhouse: {e}")
            logger.error(f"Problematic data: {data}")


class KafkaClickhouseConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'migrate_already_crawled_data_topic',
            "real_estate_topic",
            bootstrap_servers='kafka:9092',
            group_id='clickhouse_consumer',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.clickhouse_writer = ClickhouseWriter()

    def start_consuming(self):
        try:
            logger.info("Starting to consume messages from Kafka")
            for message in self.consumer:
                try:
                    data = message.value
                    self.clickhouse_writer.insert_data(data)
                    self.consumer.commit()
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    consumer = KafkaClickhouseConsumer()
    while True:
        try:
            consumer.start_consuming()
        except Exception as e:
            logger.error(f"Consumer crashed: {e}")
            time.sleep(5)  # Wait before retrying
