import json
import time
from kafka import KafkaConsumer
import sys
import os

# Add path to project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import KAFKA_CONFIG

def test_output_kafka_consumer(topic_name, max_messages=10):
    """Test consumer to verify enriched data in output Kafka topic"""

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
        group_id=f"test_output_consumer_{int(time.time())}",
        enable_auto_commit=False
    )

    print(f"Testing OUTPUT Kafka topic: {topic_name}")
    print(f"Reading first {max_messages} messages...")

    message_count = 0
    try:
        for message in consumer:
            print(f"\nEnriched Message #{message_count + 1}:")
            enriched_data = message.value
            print(f"Sport: {enriched_data.get('sport')}")
            print(f"Medal: {enriched_data.get('medal')}")
            print(f"Sex: {enriched_data.get('sex')}")
            print(f"Country: {enriched_data.get('country_noc')}")
            print(f"Avg Height: {enriched_data.get('avg_height'):.2f}")
            print(f"Avg Weight: {enriched_data.get('avg_weight'):.2f}")
            print(f"Timestamp: {enriched_data.get('timestamp')}")
            print("-" * 60)

            message_count += 1
            if message_count >= max_messages:
                break

    except Exception as e:
        print(f"Error while reading: {str(e)}")
    finally:
        consumer.close()

    print(f"Read {message_count} enriched messages from output topic")


if __name__ == "__main__":
    # Test output topic
    test_output_kafka_consumer(KAFKA_CONFIG["output_topic"])