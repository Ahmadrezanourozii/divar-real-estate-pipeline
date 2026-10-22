from confluent_kafka import Producer as KafkaProducer
import logging
from typing import Any, Dict
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Producer:
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {
            'bootstrap.servers': 'kafka:9092',
            'client.id': 'python-producer',
            'acks': 'all',
            'retries': 3,
        }
        self.kafka_producer = self._create_producer()

    def _create_producer(self) -> KafkaProducer:
        try:
            return KafkaProducer(self.config)
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def send(self, topic: str, value: str, key: str = None) -> None:
        """
        Send message to Kafka topic with retry mechanism.

        Args:
            topic: Kafka topic name
            value: Message value (string)
            key: Message key (optional)
        """
        try:
            # Produce message
            self.kafka_producer.produce(topic, value=value.encode('utf-8'), key=key.encode('utf-8') if key else None)
            logger.info(f"Successfully sent message to topic {topic}")
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise
        finally:
            self.kafka_producer.flush()  # Ensure all messages are flushed to Kafka

    def cleanup(self) -> None:
        """Ensure cleanup of producer"""
        if self.kafka_producer:
            self.kafka_producer.flush()  # Ensure all pending messages are sent
            logger.info("Kafka producer flushed")
