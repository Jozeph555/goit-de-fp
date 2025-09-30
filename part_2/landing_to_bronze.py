import os
import requests
from pyspark.sql import SparkSession

# Configuration constants
FTP_BASE_URL = "https://ftp.goit.study/neoversity/"
TABLES = ["athlete_bio", "athlete_event_results"]
BRONZE_DIR = "bronze"

def download_data(table_name):
    """Downloads data from FTP server"""
    url = f"{FTP_BASE_URL}{table_name}.csv"
    local_file_path = f"{table_name}.csv"

    print(f"Downloading from {url}")
    response = requests.get(url)

    if response.status_code == 200:
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
        return local_file_path
    else:
        raise Exception(f"Failed to download the file. Status code: {response.status_code}")

def process_table(spark, table_name):
    """Processes one table: download -> convert to parquet"""
    print(f"Processing table: {table_name}")

    csv_file = download_data(table_name)

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(csv_file)

    print(f"Loaded {df.count()} rows for table {table_name}")
    print(f"Schema for {table_name}:")
    df.printSchema()

    print(f"Sample data from {table_name}:")
    df.show(5, truncate=False)

    output_path = os.path.join(BRONZE_DIR, table_name)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")

    if os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"Temporary file {csv_file} removed")

    return df

def main():
    spark = SparkSession.builder \
        .appName("LandingToBronze") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    try:
        os.makedirs(BRONZE_DIR, exist_ok=True)

        print("Starting Landing to Bronze ETL process...")

        for table in TABLES:
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
