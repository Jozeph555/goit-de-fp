import os
import requests
from pyspark.sql import SparkSession
from config import Config

def download_data(table_name):
    """Downloads data from FTP server"""
    url = Config.get_ftp_url(table_name)
    local_file_path = f"{table_name}.csv"

    print(f"Downloading from {url}")
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
        return local_file_path
    else:
        raise Exception(f"Failed to download the file. Status code: {response.status_code}")

def process_table(spark, table_name):
    """Processes one table: download -> convert to parquet"""
    print(f"Processing table: {table_name}")

    # Download CSV file from FTP
    csv_file = download_data(table_name)

    # Read CSV file using Spark
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(csv_file)

    print(f"Loaded {df.count()} rows for table {table_name}")
    print(f"Schema for {table_name}:")
    df.printSchema()

    # Show sample data
    print(f"Sample data from {table_name}:")
    df.show(5, truncate=False)

    # Save in Parquet format
    output_path = Config.get_table_path(Config.BRONZE_DIR, table_name)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")

    # Clean up temporary CSV file
    if os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"Temporary file {csv_file} removed")

    return df

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("LandingToBronze") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    try:
        # Create bronze directory
        os.makedirs(Config.BRONZE_DIR, exist_ok=True)

        print("Starting Landing to Bronze ETL process...")

        for table in Config.TABLES:
            process_table(spark, table)
            print(f"Successfully processed {table}")
            print("-" * 50)

        print("Landing to Bronze process completed successfully!")

    except Exception as e:
        print(f"Error in Landing to Bronze process: {str(e)}")
        raise e

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
