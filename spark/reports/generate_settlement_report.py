from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, current_date, round

spark = SparkSession.builder \
    .appName("DailySettlementReport") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/payments_dw"
properties = {
    "user": "warehouse_user",
    "password": "warehouse_password",
    "driver": "org.postgresql.Driver"
}

fact_txn = spark.read.jdbc(
    jdbc_url,
    "fact_transactions",
    properties=properties
)

settlement = (
    fact_txn
    .filter(col("txn_date").cast("date") == current_date())
    .groupBy("merchant_id", "currency")
    .agg(
        count("*").alias("total_transactions"),
        round(sum("amount"), 2).alias("gross_amount")
    )
    .withColumn("processing_fee", round(col("gross_amount") * 0.02, 2))
    .withColumn(
        "net_settlement",
        round(col("gross_amount") - col("processing_fee"), 2)
    )
)

settlement.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark-apps/reports/settlement_daily")

spark.stop()
