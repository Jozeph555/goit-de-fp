import json
import time
from kafka import KafkaConsumer
import sys
import os

# Add path to project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import KAFKA_CONFIG

def test_kafka_consumer(topic_name, max_messages=5):
    """Test consumer to verify data in Kafka topic"""

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanism=KAFKA_CONFIG["sasl_mechanism"],
        sasl_plain_username=KAFKA_CONFIG["username"],
        sasl_plain_password=KAFKA_CONFIG["password"],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k is not None else None,
        auto_offset_reset='earliest',
        consumer_timeout_ms=15000,
        group_id=f"test_consumer_{int(time.time())}",
        enable_auto_commit=False
    )

    print(f"Testing Kafka topic: {topic_name}")
    print(f"Reading first {max_messages} messages...")

    message_count = 0
    try:
        for message in consumer:
            print(f"\nMessage #{message_count + 1}:")
            print(f"Value: {json.dumps(message.value, indent=2, ensure_ascii=False)}")
            print("-" * 50)

            message_count += 1
            if message_count >= max_messages:
                break

    except Exception as e:
        print(f"Error while reading: {str(e)}")
    finally:
        consumer.close()

    print(f"Read {message_count} messages")


if __name__ == "__main__":
    # Test input topic
    test_kafka_consumer(KAFKA_CONFIG["input_topic"])
