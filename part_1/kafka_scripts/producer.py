import json
import time
import sys
import os
from kafka import KafkaProducer
from pyspark.sql import SparkSession

# Add path to project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import MYSQL_CONFIG, KAFKA_CONFIG, JDBC_URL


def load_mysql_data_to_kafka():
    """Load data from MySQL athlete_event_results to Kafka"""

    spark = SparkSession.builder \
        .appName("MySQLToKafka") \
        .config("spark.jars", "jars/mysql-connector-j-8.0.32.jar") \
        .getOrCreate()

    try:
        print("Connecting to MySQL and reading athlete_event_results...")

        df = spark.read.format('jdbc').options(
            url=JDBC_URL,
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='athlete_event_results',
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password']
        ).load()

        total_records = df.count()
        print(f"Read {total_records} records from athlete_event_results")

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            security_protocol=KAFKA_CONFIG["security_protocol"],
            sasl_mechanism=KAFKA_CONFIG["sasl_mechanism"],
            sasl_plain_username=KAFKA_CONFIG["username"],
            sasl_plain_password=KAFKA_CONFIG["password"],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k is not None else b''
        )

        print(f"Sending data to Kafka topic: {KAFKA_CONFIG['input_topic']}")

        all_records = df.collect()
        sent_count = 0

        for record in all_records:
            record_dict = record.asDict()

            athlete_id = record_dict.get('athlete_id')

            cleaned_record = {}
            for k, v in record_dict.items():
                if v is None or str(v).lower() == 'nan':
                    cleaned_record[k] = None
                else:
                    cleaned_record[k] = v

            producer.send(KAFKA_CONFIG['input_topic'], value=cleaned_record)
            sent_count += 1

            if sent_count % 10000 == 0:
                print(f"Sent {sent_count} records...")

        producer.flush()
        producer.close()
        print(f"Successfully sent {sent_count} records to Kafka")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    print("Starting MySQL to Kafka data loading...")

    start_time = time.time()
    load_mysql_data_to_kafka()
    end_time = time.time()

    print(f"Data loading completed in {end_time - start_time:.2f} seconds!")
