from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Paths
bronze_path = "/opt/spark-data/bronze/payments"
silver_path = "/opt/spark-data/silver/payments"

# Check if the bronze directory exists and is not empty to avoid "Path does not exist"
# In a real environment, you'd check for the existence of _SUCCESS or actual files
if os.path.exists(bronze_path) and len(os.listdir(bronze_path)) > 0:
    print(f"Reading data from {bronze_path}...")

    # 1. Read Bronze data (Batch mode)
    df = spark.read.parquet(bronze_path)

    # 2. Transform: Deduplicate, Clean, and Audit
    # We drop duplicates based on transaction_id to ensure exact-once processing
    silver_df = (
        df.dropDuplicates(["transaction_id"])
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("processed_at", current_timestamp())
        .filter(col("amount") >= 0)
    )

    # 3. Write to Silver
    # Using 'overwrite' replaces the table; 'append' adds to it.
    print(f"Writing {silver_df.count()} cleaned records to {silver_path}...")

    silver_df.write \
        .mode("overwrite") \
        .parquet(silver_path)

    print("Silver layer update complete.")
else:
    print("Bronze path is empty or does not exist. Please run the streaming job first.")

spark.stop()
