from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

payment_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("ts_event", StringType()),
    StructField("card_hash", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("mcc", StringType()),
    StructField("channel", StringType()),
    StructField("auth_result", StringType()),
    StructField("location", StringType())
])

spark = SparkSession.builder.appName("PaymentsFraudDetection") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "4")

# 1. Source (Kafka)
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "payments.raw")
    .option("startingOffsets", "earliest")  # Ensure we don't miss data
    .load()
)

# 2. Refine & Validate
# We extract data and add a 'date' column for partitioning
payments = (
    raw_df
    .select(from_json(col("value").cast("string"), payment_schema).alias("p"))
    .select("p.*")
    .filter(col("transaction_id").isNotNull())  # Basic schema validation
    .withColumn("event_ts", to_timestamp("ts_event"))
    .withColumn("date", to_date(col("event_ts")))  # Required for partitioning
    .withWatermark("event_ts", "15 minutes")
)

# 3. Apply Fraud Rules (High Amount & Blacklist)
blacklist_df = spark.createDataFrame(
    [("M1001",), ("M2002",), ("M9999",)], ["merchant_id"])

payments = (
    payments
    .withColumn("rule_high_amount", col("amount") > 10000)
    .join(broadcast(blacklist_df.withColumn("rule_blacklist", lit(True))), on="merchant_id", how="left")
    .withColumn("rule_blacklist", coalesce(col("rule_blacklist"), lit(False)))
    .withColumn("is_fraud", col("rule_high_amount") | col("rule_blacklist"))
    .withColumn("fraud_reason", concat_ws(",",
                when(col("rule_high_amount"), "HIGH_AMOUNT"),
                when(col("rule_blacklist"), "BLACKLISTED_MERCHANT")))
)

# --- SINKS ---


# 4. Dead Letter Queue: Route ONLY flagged events back to Kafka
fraud_txns = payments.filter(col("is_fraud") == True)

deadletter_query = (
    fraud_txns
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("topic", "payments.deadletter")
    .option("checkpointLocation", "/opt/spark-data/chk/deadletter")
    .start()
)
# 5. Bronze Layer: Write ALL processed events to Parquet
# This acts as your historical record (Refined Bronze)
bronze_query = (
    payments.writeStream
    .format("parquet")
    .partitionBy("date")
    .outputMode("append")
    .trigger(processingTime='10 seconds')
    .option("path", "/opt/spark-data/bronze/payments")
    .option("checkpointLocation", "/opt/spark-data/chk/bronze")
    .start()
)

spark.streams.awaitAnyTermination()
