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

spark = (
    SparkSession.builder
    .appName("PaymentsFraudDetection")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "4")

# ----------------------------
# Kafka Source
# ----------------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "payments.raw")
    .option("startingOffsets", "latest")
    .load()
)

payments = (
    raw_df
    .select(from_json(col("value").cast("string"), payment_schema).alias("p"))
    .select("p.*")
    .withColumn("event_ts", to_timestamp("ts_event"))
    .withWatermark("event_ts", "15 minutes")
)

# ----------------------------
# RULE 1: HIGH AMOUNT
# ----------------------------
payments = payments.withColumn(
    "rule_high_amount",
    col("amount") > 10000
)

# ----------------------------
# RULE 2: VELOCITY ( >5 txns / 1 min / card )
# ----------------------------
velocity_counts = (
    payments
    .groupBy(
        window(col("event_ts"), "1 minute"),
        col("card_hash")
    )
    .agg(count("*").alias("txn_count"))
    .filter(col("txn_count") > 5)
)

payments = (
    payments
    .join(
        velocity_counts.select("card_hash").withColumn(
            "rule_velocity", lit(True)),
        on="card_hash",
        how="left"
    )
    .withColumn("rule_velocity", coalesce(col("rule_velocity"), lit(False)))
)

# ----------------------------
# RULE 3: CROSS-BORDER (2 countries / 10 min)
# ----------------------------
country_check = (
    payments
    .groupBy(
        window(col("event_ts"), "10 minutes"),
        col("card_hash")
    )
    .agg(countDistinct("location").alias("country_count"))
    .filter(col("country_count") > 1)
)

payments = (
    payments
    .join(
        country_check.select("card_hash").withColumn(
            "rule_cross_border", lit(True)),
        on="card_hash",
        how="left"
    )
    .withColumn("rule_cross_border", coalesce(col("rule_cross_border"), lit(False)))
)

# ----------------------------
# RULE 4: BLACKLISTED MERCHANT
# ----------------------------
blacklist = ["M1001", "M2002", "M9999"]
blacklist_df = spark.createDataFrame(
    [(m,) for m in blacklist],
    ["merchant_id"]
)

payments = (
    payments
    .join(
        broadcast(blacklist_df.withColumn("rule_blacklist", lit(True))),
        on="merchant_id",
        how="left"
    )
    .withColumn("rule_blacklist", coalesce(col("rule_blacklist"), lit(False)))
)

# ----------------------------
# RULE 5: DECLINE RATE (>50% in last 10 txns)
# ----------------------------
decline_window = Window.partitionBy("card_hash").orderBy(
    col("event_ts")).rowsBetween(-9, 0)

payments = payments.withColumn(
    "decline_rate",
    avg(when(col("auth_result") == "DECLINED", 1).otherwise(0)).over(decline_window)
)

payments = payments.withColumn(
    "rule_decline_rate",
    col("decline_rate") > 0.5
)

# ----------------------------
# FINAL FRAUD FLAG
# ----------------------------
payments = payments.withColumn(
    "is_fraud",
    col("rule_high_amount") |
    col("rule_velocity") |
    col("rule_cross_border") |
    col("rule_blacklist") |
    col("rule_decline_rate")
)

payments = payments.withColumn(
    "fraud_reason",
    concat_ws(
        ",",
        when(col("rule_high_amount"), "HIGH_AMOUNT"),
        when(col("rule_velocity"), "VELOCITY"),
        when(col("rule_cross_border"), "CROSS_BORDER"),
        when(col("rule_blacklist"), "BLACKLISTED_MERCHANT"),
        when(col("rule_decline_rate"), "DECLINE_RATE")
    )
)

# ----------------------------
# ROUTING
# ----------------------------
valid_txns = payments.filter(~col("is_fraud"))
fraud_txns = payments.filter(col("is_fraud"))

# ----------------------------
# BRONZE WRITE (VALID)
# ----------------------------
valid_query = (
    valid_txns
    .writeStream
    .format("parquet")
    .option("path", "/data/bronze/payments")
    .option("checkpointLocation", "/chk/bronze/payments")
    .partitionBy("currency")
    .outputMode("append")
    .start()
)

# ----------------------------
# DEAD LETTER (FRAUD)
# ----------------------------
fraud_query = (
    fraud_txns
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "payments.deadletter")
    .option("checkpointLocation", "/chk/deadletter/payments")
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()
