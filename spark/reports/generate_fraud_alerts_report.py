from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, date_sub, count, sum, round

spark = SparkSession.builder \
    .appName("FraudSettlementReport") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres-server:5432/payments_dw"
properties = {
    "user": "warehouse_user",
    "password": "warehouse_password",
    "driver": "org.postgresql.Driver"
}

# 1. Read Data
fraud = spark.read.jdbc(
    jdbc_url, "fact_fraud_transactions", properties=properties)

# 2. Apply Logic (Filter for Yesterday + Aggregations)
# We use date_sub(current_date(), 1) to target exactly yesterday
fraud_report = (
    fraud
    .filter(col("ts_event").cast("date") == date_sub(current_date(), 1))
    .groupBy("merchant_id", "currency")
    .agg(
        count("*").alias("fraud_count"),
        round(sum("amount"), 2).alias("total_fraud_amount")
    )
    .withColumn("risk_score", round(col("total_fraud_amount") * 0.15, 2))
)

# 3. Write directly to the final directory
# This follows your settlement_report pattern
output_path = "/opt/spark-data/reports/fraud_alerts_report"
fraud_report.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("sep", ",") \
    .csv(output_path)

print(f"Report successfully written to : {output_path}")

spark.stop()
