import sys
import os
from pyspark.sql import SparkSession

# Add path to project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import MYSQL_CONFIG, JDBC_URL

def check_mysql_results():
    """Check if enriched data was written to MySQL"""
    
    spark = SparkSession.builder \
        .appName("CheckMySQLResults") \
        .config("spark.jars", "jars/mysql-connector-j-8.0.32.jar") \
        .getOrCreate()
    
    try:
        print("Connecting to MySQL to check enriched_athlete_stats table...")
        
        # Read from enriched table
        df = spark.read.format('jdbc').options(
            url=JDBC_URL,
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='enriched_athlete_stats',
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password']
        ).load()
        
        total_records = df.count()
        print(f"Total records in enriched_athlete_stats: {total_records}")
        
        if total_records > 0:
            print("\nSample enriched data:")
            df.show(10, truncate=False)
            
            print("\nStats by sport:")
            df.groupBy("sport").count().orderBy("count", ascending=False).show(10)
            
            print("\nStats by medal:")
            df.groupBy("medal").count().orderBy("count", ascending=False).show()
            
            print("\nLatest records:")
            df.orderBy("timestamp", ascending=False).show(5, truncate=False)
        else:
            print("No data found in enriched_athlete_stats table")
            
    except Exception as e:
        print(f"Error checking MySQL: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    check_mysql_results()