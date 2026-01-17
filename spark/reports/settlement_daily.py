from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, date_sub
from datetime import datetime

spark = SparkSession.builder \
    .appName("DailySettlementExport") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

jdbc_url = "jdbc:postgresql://postgres:5432/payments_dw"
properties = {
    "user": "warehouse_user",
    "password": "warehouse_password",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="_analytics.fact_settlement_daily",
    properties=properties
)


today_df = df.filter(col("settlement_date") == current_date())

today_str = datetime.utcnow().strftime("%Y-%m-%d")

output_path = f"/opt/spark-data/reports/settlement_daily/settlement_{today_str}"

(
    today_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(output_path)
)

print(f"Settlement report generated for {today_str}")

spark.stop()
