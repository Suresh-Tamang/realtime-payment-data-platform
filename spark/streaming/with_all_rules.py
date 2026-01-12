from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming.state import GroupStateTimeout
import pandas as pd
from datetime import timedelta

# ===================================================
# 1. Spark Session
# ===================================================
spark = (
    SparkSession.builder
    .appName("PaymentsFraudDetection")
    .master("spark://spark-master:7077")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===================================================
# 2. Input Schema
# ===================================================
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

# ===================================================
# 3. Kafka Source
# ===================================================
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "payments.raw")
    .load()
)

# ===================================================
# 4. Parse JSON + Invalid Schema DLQ
# ===================================================
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), payment_schema).alias("p")
)

valid_df = parsed_df.filter(col("p").isNotNull()).select("p.*")

invalid_schema_df = (
    parsed_df.filter(col("p").isNull())
    .withColumn("error_reason", lit("INVALID_JSON_SCHEMA"))
    .withColumn("dlq_ts", current_timestamp())
)

# ===================================================
# 5. Stateless Rules (High Amount + Blacklist)
# ===================================================
blacklist = ["M1001", "M2002", "M9999"]
blacklist_df = spark.createDataFrame(
    [(m,) for m in blacklist], ["merchant_id"])

payments = (
    valid_df
    .withColumn("event_ts", to_timestamp("ts_event"))
    .withColumn("event_date", to_date("event_ts"))
    .withWatermark("event_ts", "15 minutes")
    .withColumn("rule_high_amount", col("amount") > 10000)
    .join(
        broadcast(blacklist_df.withColumn("rule_blacklist_tmp", lit(True))),
        "merchant_id",
        "left"
    )
    .withColumn(
        "rule_blacklist",
        coalesce(col("rule_blacklist_tmp"), lit(False))
    )
    .drop("rule_blacklist_tmp")
)

# ===================================================
# 6. Output & State Schemas (CRITICAL)
# ===================================================
output_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("ts_event", StringType()),
    StructField("card_hash", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("mcc", StringType()),
    StructField("channel", StringType()),
    StructField("auth_result", StringType()),
    StructField("location", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("event_date", DateType()),
    StructField("rule_high_amount", BooleanType()),
    StructField("rule_blacklist", BooleanType()),
    StructField("rule_velocity", BooleanType()),
    StructField("rule_cross_border", BooleanType()),
    StructField("rule_decline_rate", BooleanType())
])

state_schema = StructType([
    StructField(
        "events",
        ArrayType(
            StructType([
                StructField("ts", TimestampType()),
                StructField("location", StringType()),
                StructField("auth_result", StringType())
            ])
        )
    )
])

# ===================================================
# 7. Stateful Fraud Logic (PANDAS STATE)
# ===================================================


def fraud_state_fn(card_hash, pdf, state):
    if state.exists:
        events = state.get["events"]
    else:
        events = []

    results = []
    pdf = pdf.sort_values("event_ts")

    for _, row in pdf.iterrows():
        now = row.event_ts

        events.append({
            "ts": now,
            "location": row.location,
            "auth_result": row.auth_result
        })

        # Keep last 10 minutes
        events = [e for e in events if e["ts"] >= now - timedelta(minutes=10)]

        # Rule 2: Velocity (>5 txns in 1 min)
        rule_velocity = sum(
            1 for e in events if e["ts"] >= now - timedelta(minutes=1)
        ) > 5

        # Rule 3: Cross-border
        rule_cross_border = len(set(e["location"] for e in events)) > 1

        # Rule 5: Decline rate
        last_10 = events[-10:]
        declines = sum(1 for e in last_10 if e["auth_result"] == "DECLINED")
        rule_decline_rate = (
            len(last_10) >= 10 and (declines / len(last_10)) > 0.5
        )

        results.append({
            "transaction_id": row.transaction_id,
            "ts_event": row.ts_event,
            "card_hash": row.card_hash,
            "merchant_id": row.merchant_id,
            "amount": row.amount,
            "currency": row.currency,
            "mcc": row.mcc,
            "channel": row.channel,
            "auth_result": row.auth_result,
            "location": row.location,
            "event_ts": row.event_ts,
            "event_date": row.event_date,
            "rule_high_amount": row.rule_high_amount,
            "rule_blacklist": row.rule_blacklist,
            "rule_velocity": rule_velocity,
            "rule_cross_border": rule_cross_border,
            "rule_decline_rate": rule_decline_rate
        })

    state.update({"events": events})
    state.setTimeoutDuration("15 minutes")

    return pd.DataFrame(results)


# ===================================================
# 8. Apply Stateful Processing (CORRECT SIGNATURE)
# ===================================================
stateful_df = (
    payments
    .groupBy("card_hash")
    .applyInPandasWithState(
        func=fraud_state_fn,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
    )
)

# ===================================================
# 9. Final Fraud Decision
# ===================================================
final_df = (
    stateful_df
    .withColumn(
        "is_fraud",
        col("rule_high_amount") |
        col("rule_blacklist") |
        col("rule_velocity") |
        col("rule_cross_border") |
        col("rule_decline_rate")
    )
    .withColumn(
        "fraud_reason",
        concat_ws(",",
                  when(col("rule_high_amount"), "HIGH_AMOUNT"),
                  when(col("rule_blacklist"), "BLACKLISTED_MERCHANT"),
                  when(col("rule_velocity"), "VELOCITY"),
                  when(col("rule_cross_border"), "CROSS_BORDER"),
                  when(col("rule_decline_rate"), "DECLINE_RATE")
                  )
    )
)

fraud_txns = final_df.filter(col("is_fraud"))
valid_txns = final_df.filter(~col("is_fraud"))

# ===================================================
# 10. Kafka DLQ Sink
# ===================================================
deadletter_query = (
    fraud_txns
    .withColumn("error_reason", lit("FRAUD_DETECTED"))
    .withColumn("dlq_ts", current_timestamp())
    .unionByName(invalid_schema_df, allowMissingColumns=True)
    .select(
        col("card_hash").cast("string").alias("key"),
        to_json(struct("*")).alias("value")
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("topic", "payments.deadletter")
    .option("checkpointLocation", "/opt/spark-data/test/chk/deadletter")
    .start()
)

# ===================================================
# 11. Bronze Tables
# ===================================================
valid_query = (
    valid_txns.writeStream
    .format("parquet")
    .partitionBy("event_date")
    .option("path", "/opt/spark-data/test/bronze/payments")
    .option("checkpointLocation", "/opt/spark-data/test/chk/bronze")
    .trigger(processingTime="1 minute")
    .start()
)

fraud_history_query = (
    fraud_txns.writeStream
    .format("parquet")
    .partitionBy("event_date")
    .option("path", "/opt/spark-data/test/bronze/payments_fraud")
    .option("checkpointLocation", "/opt/spark-data/test/chk/bronze_fraud")
    .trigger(processingTime="1 minute")
    .start()
)

spark.streams.awaitAnyTermination()
