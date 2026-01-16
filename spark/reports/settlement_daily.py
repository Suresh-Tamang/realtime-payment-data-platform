from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date

spark = SparkSession.builder \
    .appName("GoldDailySettlement") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/payments_dw"
props = {
    "user": "warehouse_user",
    "password": "warehouse_password",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="fact_transactions",
    properties=props
)

daily_settlement = (
    df.filter(col("auth_result") == "APPROVED")
      .groupBy(to_date("event_ts").alias("settlement_date"))
      .agg(
          sum("amount").alias("total_amount"),
          count("*").alias("txn_count")
    )
)

daily_settlement.write.jdbc(
    url=jdbc_url,
    table="fact_settlement_daily",
    mode="overwrite",
    properties=props
)

spark.stop()
