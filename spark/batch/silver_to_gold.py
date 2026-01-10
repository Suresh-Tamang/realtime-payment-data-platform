from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

silver_path = "/opt/spark-data/silver/payments"
jdbc_url = "jdbc:postgresql://postgres:5432/payments_dw"
properties = {
    "user": "warehouse_user",
    "password": "warehouse_password",
    "driver": "org.postgresql.Driver"
}

# 1. Safety Check: Only proceed if Silver data exists
if os.path.exists(silver_path):
    print(f"Reading from {silver_path}...")
    df = spark.read.parquet(silver_path)

    # 2. Select columns for the Fact Table
    # We ensure column names are clean for Postgres
    fact_transactions = df.select(
        "transaction_id",
        "card_hash",
        "merchant_id",
        "amount",
        "currency",
        "auth_result",
        "event_ts"
    ).coalesce(1)  # Optimization: Single connection to Postgres

    # 3. Write to PostgreSQL
    print("Writing to PostgreSQL table: fact_transactions...")
    fact_transactions.write.jdbc(
        url=jdbc_url,
        table="fact_transactions",
        mode="overwrite",
        properties=properties
    )
    print("Gold layer successfully updated.")
else:
    print(f"Path {silver_path} not found. Ensure the Silver job has run.")

spark.stop()
