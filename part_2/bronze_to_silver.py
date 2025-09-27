import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from config import Config

def clean_text(text):
    """Function to clean text - keeps only letters, digits and basic punctuation"""
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\\"\'\s]', '', str(text))

def process_table(spark, table_name):
    """Processes one table: text cleaning + deduplication"""
    print(f"Processing table: {table_name}")

    # Read data from bronze layer
    bronze_path = Config.get_table_path(Config.BRONZE_DIR, table_name)
    df = spark.read.parquet(bronze_path)

    print(f"Loaded {df.count()} rows from bronze/{table_name}")

    # Create UDF for text cleaning
    clean_text_udf = udf(clean_text, StringType())

    # Get list of string columns
    string_columns = [field.name for field in df.schema.fields if field.dataType == StringType()]
    print(f"Text columns to clean: {string_columns}")

    # Apply text cleaning to all string columns
    cleaned_df = df
    for col_name in string_columns:
        cleaned_df = cleaned_df.withColumn(col_name, clean_text_udf(col(col_name)))

    print("Text cleaning applied to all string columns")

    # Deduplication
    initial_count = cleaned_df.count()
    deduplicated_df = cleaned_df.dropDuplicates()
    final_count = deduplicated_df.count()

    duplicates_removed = initial_count - final_count
    print(f"Deduplication completed: {duplicates_removed} duplicates removed")
    print(f"Final count: {final_count} rows")

    # Show sample of cleaned data
    print(f"Sample cleaned data from {table_name}:")
    deduplicated_df.show(5, truncate=False)

    # Save to silver layer
    silver_path = Config.get_table_path(Config.SILVER_DIR, table_name)
    deduplicated_df.write.mode("overwrite").parquet(silver_path)
    print(f"Cleaned data saved to {silver_path}")

    return deduplicated_df

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    try:
        # Create silver directory
        os.makedirs(Config.SILVER_DIR, exist_ok=True)

        print("Starting Bronze to Silver ETL process...")

        for table in Config.TABLES:
            # Check if bronze table exists
            bronze_path = Config.get_table_path(Config.BRONZE_DIR, table)
            if not os.path.exists(bronze_path):
                print(f"⚠️ Bronze table {table} not found at {bronze_path}")
                continue

            process_table(spark, table)
            print(f"Successfully processed {table}")
            print("-" * 50)

        print("Bronze to Silver process completed successfully!")

    except Exception as e:
        print(f"Error in Bronze to Silver process: {str(e)}")
        raise e

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
