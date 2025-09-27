import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, regexp_extract, when
from pyspark.sql.types import FloatType
from config import Config


def convert_numeric_columns(df, columns):
    """Converts specified columns to numeric format with data cleaning"""
    converted_df = df
    for column in columns:
        if column in df.columns:
            # Clean the data: extract first number from strings like "78, 80" or "78-80"
            # This regex extracts the first sequence of digits (with optional decimal point)
            cleaned_col = regexp_extract(col(column), r"(\d+\.?\d*)", 1)
            
            # Convert to float, if extraction fails or is empty, it becomes null
            converted_df = converted_df.withColumn(
                column, 
                when(cleaned_col != "", cleaned_col.cast(FloatType())).otherwise(None)
            )
            print(f"Converted column '{column}' to FloatType with data cleaning")
    return converted_df

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    try:
        print("Starting Silver to Gold ETL process...")

        # Create gold directory
        os.makedirs(Config.GOLD_DIR, exist_ok=True)

        # Check if silver tables exist
        athlete_bio_path = Config.get_table_path(Config.SILVER_DIR, "athlete_bio")
        athlete_results_path = Config.get_table_path(Config.SILVER_DIR, "athlete_event_results")

        if not os.path.exists(athlete_bio_path):
            raise FileNotFoundError(f"Silver table not found: {athlete_bio_path}")
        if not os.path.exists(athlete_results_path):
            raise FileNotFoundError(f"Silver table not found: {athlete_results_path}")

        # Read data from silver layer
        print("Loading data from silver layer...")
        athlete_bio_df = spark.read.parquet(athlete_bio_path)
        athlete_results_df = spark.read.parquet(athlete_results_path)

        print(f"Loaded athlete_bio: {athlete_bio_df.count()} rows")
        print(f"Loaded athlete_event_results: {athlete_results_df.count()} rows")

        # Show schemas
        print("athlete_bio schema:")
        athlete_bio_df.printSchema()
        print("athlete_event_results schema:")
        athlete_results_df.printSchema()

        # Convert weight and height to numeric format
        print("Converting weight and height to numeric format...")
        athlete_bio_df = convert_numeric_columns(athlete_bio_df, Config.NUMERIC_COLUMNS)

        # Join tables on athlete_id
        print("Joining tables on athlete_id...")
        joined_df = athlete_results_df.join(
            athlete_bio_df,
            on="athlete_id",
            how="inner"
        )

        joined_count = joined_df.count()
        print(f"Joined dataset contains {joined_count} rows")

        # Show sample of joined data - використовуємо конкретну таблицю для country_noc
        print("Sample of joined data:")
        joined_df.select("athlete_id", "sport", "medal", "sex", athlete_results_df["country_noc"], "weight", "height").show(5, truncate=False)

        # Filter rows with non-null values
        filtered_df = joined_df.filter(
            (col("weight").isNotNull()) &
            (col("height").isNotNull()) &
            (col("sport").isNotNull()) &
            (col("medal").isNotNull()) &
            (col("sex").isNotNull()) &
            (athlete_results_df["country_noc"].isNotNull())
        )

        filtered_count = filtered_df.count()
        print(f"Filtered dataset (non-null values): {filtered_count} rows")

        # Group and calculate average values
        print("Calculating average statistics...")
        avg_stats_df = filtered_df.groupBy("sport", "medal", "sex", athlete_results_df["country_noc"])\
            .agg(
                avg("weight").alias("avg_weight"),
                avg("height").alias("avg_height")
            )

        # Add timestamp
        final_df = avg_stats_df.withColumn("timestamp", current_timestamp())

        final_count = final_df.count()
        print(f"Final aggregated dataset: {final_count} unique combinations")

        # Show final result
        print("Final gold dataset sample:")
        final_df.show(10, truncate=False)

        # Save to gold layer
        gold_path = Config.get_table_path(Config.GOLD_DIR, "avg_stats")
        final_df.write.mode("overwrite").parquet(gold_path)
        print(f"Gold data saved to {gold_path}")

        # Statistics for verification
        print("\nFinal Statistics:")
        print(f"Total unique combinations: {final_count}")
        print("Distribution by medal type:")
        final_df.groupBy("medal").count().show()

        print("Silver to Gold process completed successfully!")

    except Exception as e:
        print(f"Error in Silver to Gold process: {str(e)}")
        raise e

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
