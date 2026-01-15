from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("SilverToWarehouse_Incremental") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

# Paths & JDBC Configuration
silver_path = "/opt/spark-data/silver/payments"
checkpoint_path = "/opt/spark-data/chk/warehouse_incremental"
jdbc_url = "jdbc:postgresql://postgres:5432/payments_dw"

db_properties = {
    "user": "warehouse_user",
    "password": "warehouse_password",
    "driver": "org.postgresql.Driver",
    "batchsize": "10000",  # Optimization for faster DB inserts
    "rewriteBatchedInserts": "true"
}

def write_to_warehouse(batch_df, batch_id):
    """
    Function to handle the writing of each micro-batch to Postgres.
    """
    row_count = batch_df.count()
    if row_count > 0:
        print(f"Batch {batch_id}: Processing {row_count} records...")
        
        # Coalesce to control number of parallel connections to Postgres
        # (Too many partitions = too many open DB connections)
        (batch_df.coalesce(2) 
            .write.jdbc(
                url=jdbc_url,
                table="transactions",
                mode="append",
                properties=db_properties
            ))
    else:
        print(f"Batch {batch_id}: No new data found.")

# 1. Load the Silver Schema dynamically
silver_schema = spark.read.parquet(silver_path).schema

# 2. Setup the Incremental Stream
silver_stream = (
    spark.readStream
    .format("parquet")
    .schema(silver_schema)
    .option("maxFilesPerTrigger", 10) # Control flow so you don't overwhelm Postgres
    .load(silver_path)
)

# 3. Data Selection & Final Casting for Warehouse
# Ensure transaction_id is not null to maintain DB integrity
warehouse_df = silver_stream.select(
    "transaction_id",
    "ts_event",
    "card_hash",
    "merchant_id",
    col("amount").cast("decimal(18,2)"), # Common DW practice for currency
    "currency",
    "mcc",
    "channel",
    "auth_result",
    "location",
    "event_ts",
    "rule_high_amount",
    "rule_blacklist",
    "is_fraud",
    "fraud_reason",
    "processed_at",
    "date"
).filter(col("transaction_id").isNotNull())

# 4. Trigger the process
query = (
    warehouse_df.writeStream
    .foreachBatch(write_to_warehouse)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True) # Runs as a batch but uses streaming checkpoints
    .start()
)

print(f"Incremental Sync: Silver -> Warehouse started...")
query.awaitTermination()
print("Warehouse sync complete.")