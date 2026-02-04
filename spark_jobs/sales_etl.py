import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth,
    to_date, lit, when, current_timestamp, concat_ws
)

# ----------------------------
# Date input
# ----------------------------
if len(sys.argv) != 2:
    print("Usage: spark-submit sales_etl.py YYYY-MM-DD")
    sys.exit(1)

run_date = sys.argv[1]
run_date_obj = datetime.strptime(run_date, "%Y-%m-%d")

year_p = run_date_obj.strftime("%Y")
month_p = run_date_obj.strftime("%m")
day_p = run_date_obj.strftime("%d")

# ----------------------------
# Azure credentials
# ----------------------------
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

# ----------------------------
# Spark session with Delta
# ----------------------------
spark = (
    SparkSession.builder
    .appName("Sales_ETL")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # --- CONCURRENCY FIXES ---
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    .config("spark.databricks.delta.autoCompact.enabled", "true")
    # Hardcode the protocol so concurrent writers don't fight over initialization
    .config("spark.databricks.delta.minReaderVersion", "1")
    .config("spark.databricks.delta.minWriterVersion", "2")
    # Allow multiple writers to commit to the log simultaneously
    .config("spark.databricks.delta.snapshotIsolation.enabled", "true")
    .getOrCreate()
)

spark.conf.set(
    f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
    account_key
)

# ----------------------------
# Paths
# ----------------------------
raw_path = f"abfss://raw@{account_name}.dfs.core.windows.net/sales/{year_p}/{month_p}/{day_p}/sales_raw.csv"
processed_path = f"abfss://processed@{account_name}.dfs.core.windows.net/sales/"

# ----------------------------
# Read RAW data
# ----------------------------
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_path)
)

# ----------------------------
# Transformations
# ----------------------------
df = df.withColumn("order_date", to_date(col("order_date")))

df = (
    df
    .withColumn("order_year", year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
    .withColumn("order_day", dayofmonth(col("order_date")))
    .withColumn("order_year_month", concat_ws("-", col("order_year"), col("order_month")))
    .withColumn("total_amount", col("quantity") * col("price"))
    .withColumn(
        "price_bucket",
        when(col("price") < 500, "LOW")
        .when(col("price") < 2000, "MEDIUM")
        .otherwise("HIGH")
    )
    .withColumn("processing_date", lit(run_date))
    .withColumn("ingestion_timestamp", current_timestamp())
)

# ----------------------------
# Write PROCESSED Delta table
# ----------------------------
(
    df.write
    .format("delta")
    .mode("append")
    .partitionBy("order_year", "order_month")
    .save(processed_path)
)

spark.stop()
print("SUCCESS: Processed data written to Delta Lake")
