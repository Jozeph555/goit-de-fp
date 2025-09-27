import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg, current_timestamp, 
    when, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType
)
from config import MYSQL_CONFIG, KAFKA_CONFIG, JDBC_URL


os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.13:4.0.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 pyspark-shell'


def create_spark_session():
    """Create Spark session for streaming"""
    return SparkSession.builder \
        .appName("AthleteStreamingPipeline") \
        .config("spark.jars", "jars/mysql-connector-j-8.0.32.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .config("spark.sql.streaming.checkpointLocation", "checkpoints/") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()


def read_athlete_bio_data(spark):
    """Step 1: Read athlete bio data from MySQL"""
    print("Step 1: Reading athlete bio data from MySQL...")

    athlete_bio_df = spark.read.format('jdbc').options(
        url=JDBC_URL,
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='athlete_bio',
        user=MYSQL_CONFIG['user'],
        password=MYSQL_CONFIG['password']
    ).load()

    print(f"Read {athlete_bio_df.count()} athlete bio records")
    print("Athlete bio schema:")
    athlete_bio_df.printSchema()

    return athlete_bio_df

def filter_athlete_bio_data(athlete_bio_df):
    """Step 2: Filter bio data - remove records with empty height/weight or non-numeric values"""
    print("Step 2: Filtering athlete bio data...")

    # Clean and convert height/weight to numeric
    cleaned_df = athlete_bio_df.withColumn(
        "height_numeric", 
        when(col("height").rlike("^[0-9]+(\\.[0-9]+)?$"), col("height").cast(FloatType()))
        .otherwise(None)
    ).withColumn(
        "weight_numeric",
        when(col("weight").rlike("^[0-9]+(\\.[0-9]+)?$"), col("weight").cast(FloatType()))
        .otherwise(None)
    )

    # Filter out records where height or weight are null or invalid
    filtered_df = cleaned_df.filter(
        (col("height_numeric").isNotNull()) & 
        (col("weight_numeric").isNotNull()) &
        (col("height_numeric") > 0) &
        (col("weight_numeric") > 0)
    ).select(
        col("athlete_id"),
        col("name"),
        col("sex"),
        col("born"),
        col("height_numeric").alias("height"),
        col("weight_numeric").alias("weight"),
        col("country"),
        col("country_noc"),
        col("description"),
        col("special_notes")
    )

    original_count = athlete_bio_df.count()
    filtered_count = filtered_df.count()
    print(f"Filtered: {original_count} -> {filtered_count} records ({filtered_count/original_count*100:.1f}% kept)")

    return filtered_df


def read_kafka_stream(spark):
    """Step 3: Read streaming data from Kafka topic"""
    print("Step 3: Reading streaming data from Kafka...")

    event_schema = StructType([
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("event", StringType(), True)
    ])

    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"][0]) \
        .option("kafka.security.protocol", KAFKA_CONFIG["security_protocol"]) \
        .option("kafka.sasl.mechanism", KAFKA_CONFIG["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="{KAFKA_CONFIG["username"]}" '
                f'password="{KAFKA_CONFIG["password"]}";') \
        .option("subscribe", KAFKA_CONFIG["input_topic"]) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "1000") \
        .load()

    # Parse JSON from Kafka value
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")

    return parsed_df


def join_streams(event_stream_df, athlete_bio_df):
    """Step 4: Join streaming event data with bio data"""
    print("Step 4: Joining event stream with athlete bio data...")

    # Join on athlete_id
    joined_df = event_stream_df.join(
        athlete_bio_df,
        event_stream_df.athlete_id == athlete_bio_df.athlete_id,
        "inner"
    ).select(
        event_stream_df["*"],  # All event columns
        athlete_bio_df["height"],
        athlete_bio_df["weight"],
        athlete_bio_df["sex"]
    )

    return joined_df


def calculate_aggregations(joined_df):
    """Step 5: Calculate average height and weight by sport, medal, sex, country"""
    print("Step 5: Calculating aggregations...")

    # Clean medal field - replace null/nan with "No Medal"
    cleaned_df = joined_df.withColumn(
        "medal_clean",
        when((col("medal").isNull()) | (col("medal") == "nan") | (col("medal") == ""), "No Medal")
        .otherwise(col("medal"))
    )

    # Calculate aggregations
    aggregated_df = cleaned_df.groupBy(
        "sport",
        "medal_clean",
        "sex", 
        "country_noc"
    ).agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ).withColumn(
        "timestamp", current_timestamp()
    ).withColumnRenamed("medal_clean", "medal")

    return aggregated_df


def write_to_kafka_and_mysql(aggregated_df):
    """Step 6: Write results to both Kafka and MySQL using forEachBatch"""
    print("Step 6: Setting up output streams...")

    def foreach_batch_function(batch_df, batch_id):
        """Process each batch - write to both Kafka and MySQL"""
        print(f"Processing batch {batch_id} with {batch_df.count()} records...")

        if batch_df.count() > 0:
            try:
                # Step 6a: writing to kafka topic
                kafka_output_df = batch_df.select(
                    col("sport"),
                    col("medal"),
                    col("sex"),
                    col("country_noc"),
                    col("avg_height"),
                    col("avg_weight"),
                    col("timestamp")
                )

                # Convert to JSON for Kafka
                kafka_json_df = kafka_output_df.select(
                    to_json(struct([col(c) for c in kafka_output_df.columns])).alias("value")
                )

                kafka_json_df.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"][0]) \
                    .option("kafka.security.protocol", KAFKA_CONFIG["security_protocol"]) \
                    .option("kafka.sasl.mechanism", KAFKA_CONFIG["sasl_mechanism"]) \
                    .option("kafka.sasl.jaas.config",
                            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                            f'username="{KAFKA_CONFIG["username"]}" '
                            f'password="{KAFKA_CONFIG["password"]}";') \
                    .option("topic", KAFKA_CONFIG["output_topic"]) \
                    .save()

                print(f"Batch {batch_id}: Sent {kafka_output_df.count()} records to Kafka")

                # Step 6b: writing to MySQL database
                batch_df.write \
                    .format("jdbc") \
                    .option("url", JDBC_URL) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("dbtable", "enriched_athlete_stats") \
                    .option("user", MYSQL_CONFIG['user']) \
                    .option("password", MYSQL_CONFIG['password']) \
                    .mode("append") \
                    .save()

                print(f"Batch {batch_id}: Saved {batch_df.count()} records to MySQL")

                # Show sample data
                print(f"Batch {batch_id} sample data:")
                batch_df.show(5, truncate=False)

            except Exception as e:
                print(f"Error processing batch {batch_id}: {str(e)}")

    # Start streaming with forEachBatch
    query = aggregated_df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("update") \
        .trigger(processingTime='10 seconds') \
        .start()

    return query


def main():
    """Main streaming pipeline"""
    print("Starting Athlete Streaming Pipeline...")

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Step 1: Read athlete bio data from MySQL
        athlete_bio_df = read_athlete_bio_data(spark)

        # Step 2: Filter bio data
        filtered_bio_df = filter_athlete_bio_data(athlete_bio_df)

        # Step 3: Read streaming data from Kafka
        event_stream_df = read_kafka_stream(spark)

        # Step 4: Join streaming data with bio data
        joined_df = join_streams(event_stream_df, filtered_bio_df)

        # Step 5: Calculate aggregations
        aggregated_df = calculate_aggregations(joined_df)

        # Step 6: Write to Kafka and MySQL
        query = write_to_kafka_and_mysql(aggregated_df)

        print("Streaming pipeline started successfully!")
        print("Waiting for data... (Press Ctrl+C to stop)")

        # Wait for termination
        query.awaitTermination()

    except KeyboardInterrupt:
        print("Pipeline stopped by user")
    except Exception as e:
        print(f"Error in streaming pipeline: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
