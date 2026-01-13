from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BronzeToSilver_Incremental") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# Define Paths
bronze_path = "/opt/spark-data/bronze/payments"
silver_path = "/opt/spark-data/silver/payments"
checkpoint_path = "/opt/spark-data/chk/silver_incremental"

# 1. Read from Bronze using readStream (Incremental)
# Spark will monitor the directory for new Parquet files automatically
bronze_stream = (
    spark.readStream
    .format("parquet")
    # Inherit schema from existing bronze
    .schema(spark.read.parquet(bronze_path).schema)
    .load(bronze_path)
)

# 2. Transform & Deduplicate
# Note: dropDuplicates in streaming requires a watermark or global state.
# For File-to-File streams, it works on the current micro-batch.
silver_df = (
    bronze_stream
    .withWatermark("event_ts","24 hours") # keep id in state for 24 hous
    .dropDuplicates(["transaction_id","event_ts"])
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("processed_at", current_timestamp())
    .filter(col("amount") >= 0)
)

# 3. Write to Silver (Stream)
# We use 'append' so we don't delete old data, and 'checkpoint' to track progress
query = (
    silver_df.writeStream
    .format("parquet")
    .outputMode("append")
    .partitionBy("date")
    .option("path", silver_path)
    .option("checkpointLocation", checkpoint_path)
    # This makes it run like a batch job but with stream logic
    .trigger(availableNow=True)
    .start()
)

print(f"Starting incremental update from {bronze_path} to {silver_path}...")
query.awaitTermination()
print("Silver layer update complete.")
