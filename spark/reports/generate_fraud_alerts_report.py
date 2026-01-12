from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, date_sub, count, sum, round,to_date, when
from datetime import datetime

spark = SparkSession.builder \
    .appName("FraudSettlementReport") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/payments_dw"
properties = {
    "user": "warehouse_user",
    "password": "warehouse_password",
    "driver": "org.postgresql.Driver"
}

# 1. Read Data
fraud = spark.read.jdbc(
    jdbc_url, "fact_fraud_signals", properties=properties)

# 2. Apply Logic (Filter for Yesterday + Aggregations)
# We use date_sub(current_date(), 1) to target exactly yesterday
fraud_report = (
    fraud
    .filter(to_date(col("ts_event")) == current_date())
    .groupBy("merchant_id")
    .agg(
        count("*").alias("fraud_count"),
        round(sum("amount"), 2).alias("total_fraud_amount")
    )
    .withColumn("risk_score", round(col("total_fraud_amount") * 0.1, 2))
    .withColumn("risk_level",
                when(col("risk_score") >=10000,"CRITICAL")
                .when(col("risk_score") >= 5000,"HIGH")
                .when(col("risk_score") >= 1000, "MEDIUM")
                .otherwise("LOW")                    
                
    )
)

# 3. Write directly to the final directory
# This follows your settlement_report pattern
today_str = datetime.now().strftime("%Y-%m-%d")
output_path = f"/opt/spark-data/reports/daily/fraud/fraud_alerts_{today_str}"
fraud_report.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"Report successfully written to : {output_path}")

spark.stop()
