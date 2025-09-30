import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, regexp_extract, when
from pyspark.sql.types import FloatType

# Configuration constants
SILVER_DIR = "silver"
GOLD_DIR = "gold"
NUMERIC_COLUMNS = ["weight", "height"]

def convert_numeric_columns(df, columns):
    """Converts specified columns to numeric format with data cleaning"""
    converted_df = df
    for column in columns:
        if column in df.columns:
            cleaned_col = regexp_extract(col(column), r"(\d+\.?\d*)", 1)
            converted_df = converted_df.withColumn(
                column, 
                when(cleaned_col != "", cleaned_col.cast(FloatType())).otherwise(None)
            )
            print(f"Converted column '{column}' to FloatType with data cleaning")
    return converted_df

def main():
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    try:
        print("Starting Silver to Gold ETL process...")

        os.makedirs(GOLD_DIR, exist_ok=True)

        athlete_bio_path = os.path.join(SILVER_DIR, "athlete_bio")
        athlete_results_path = os.path.join(SILVER_DIR, "athlete_event_results")

        if not os.path.exists(athlete_bio_path):
            raise FileNotFoundError(f"Silver table not found: {athlete_bio_path}")
        if not os.path.exists(athlete_results_path):
            raise FileNotFoundError(f"Silver table not found: {athlete_results_path}")

        print("Loading data from silver layer...")
        athlete_bio_df = spark.read.parquet(athlete_bio_path)
        athlete_results_df = spark.read.parquet(athlete_results_path)

        print(f"Loaded athlete_bio: {athlete_bio_df.count()} rows")
        print(f"Loaded athlete_event_results: {athlete_results_df.count()} rows")

        print("athlete_bio schema:")
        athlete_bio_df.printSchema()
        print("athlete_event_results schema:")
        athlete_results_df.printSchema()

        print("Converting weight and height to numeric format...")
        athlete_bio_df = convert_numeric_columns(athlete_bio_df, NUMERIC_COLUMNS)

        print("Joining tables on athlete_id...")
        joined_df = athlete_results_df.join(
            athlete_bio_df,
            on="athlete_id",
            how="inner"
        )

        joined_count = joined_df.count()
        print(f"Joined dataset contains {joined_count} rows")

        print("Sample of joined data:")
        joined_df.select("athlete_id", "sport", "medal", "sex", athlete_results_df["country_noc"], "weight", "height").show(5, truncate=False)

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

        print("Calculating average statistics...")
        avg_stats_df = filtered_df.groupBy("sport", "medal", "sex", athlete_results_df["country_noc"])\
            .agg(
                avg("weight").alias("avg_weight"),
                avg("height").alias("avg_height")
            )

        final_df = avg_stats_df.withColumn("timestamp", current_timestamp())

        final_count = final_df.count()
        print(f"Final aggregated dataset: {final_count} unique combinations")

        print("Final gold dataset sample:")
        final_df.show(10, truncate=False)

        gold_path = os.path.join(GOLD_DIR, "avg_stats")
        final_df.write.mode("overwrite").parquet(gold_path)
        print(f"Gold data saved to {gold_path}")

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
