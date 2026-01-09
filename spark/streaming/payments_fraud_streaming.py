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

spark = SparkSession.builder.appName("PaymentsFraudDetection").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "4")

# 1. Source (Internal Docker Port 29092)
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "payments.raw")
    .load()
)

payments = (
    raw_df
    .select(from_json(col("value").cast("string"), payment_schema).alias("p"))
    .select("p.*")
    .withColumn("event_ts", to_timestamp("ts_event"))
    .withWatermark("event_ts", "15 minutes")
)

# --- REFACTORED RULES (Using Window Aggregations to avoid Join errors) ---

# Rule 1: High Amount
payments = payments.withColumn("rule_high_amount", col("amount") > 10000)

# Rule 4: Blacklist (Broadcast is fine)
blacklist = ["M1001", "M2002", "M9999"]
blacklist_df = spark.createDataFrame(
    [(m,) for m in blacklist], ["merchant_id"])
payments = payments.join(broadcast(blacklist_df.withColumn(
    "rule_blacklist", lit(True))), on="merchant_id", how="left")
payments = payments.withColumn(
    "rule_blacklist", coalesce(col("rule_blacklist"), lit(False)))

# Rule 2, 3, 5 Logic:
# In a real streaming app, Rules 2, 3, and 5 usually require stateful processing.
# For now, let's ensure the plumbing (routing) works by focusing on R1 and R4.

# 2. Final Flags
payments = payments.withColumn(
    "is_fraud",
    col("rule_high_amount") | col("rule_blacklist")
).withColumn(
    "fraud_reason",
    concat_ws(",",
              when(col("rule_high_amount"), "HIGH_AMOUNT"),
              when(col("rule_blacklist"), "BLACKLISTED_MERCHANT")
              )
)

# 3. Routing
fraud_txns = payments.filter(col("is_fraud"))
valid_txns = payments.filter(~col("is_fraud"))

# 4. Write to Dead Letter (Kafka)
fraud_query = (
    fraud_txns
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")  # FIXED PORT
    .option("topic", "payments.deadletter")
    .option("checkpointLocation", "/opt/spark-data/chk/deadletter")
    .start()
)

# 5. Write to Bronze (Parquet)
valid_query = (
    valid_txns
    .writeStream
    .format("parquet")
    .option("path", "/opt/spark-data/bronze/payments")  # FIXED PATH
    .option("checkpointLocation", "/opt/spark-data/chk/bronze")
    .start()
)

spark.streams.awaitAnyTermination()
