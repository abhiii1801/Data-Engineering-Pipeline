import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ----------------------------
# Date input
# ----------------------------
if len(sys.argv) != 2:
    print("Usage: spark-submit expectations.py YYYY-MM-DD")
    sys.exit(1)

run_date = sys.argv[1]
run_date_obj = datetime.strptime(run_date, "%Y-%m-%d")

year_p = int(run_date_obj.strftime("%Y"))
month_p = int(run_date_obj.strftime("%m"))

# ----------------------------
# Azure credentials
# ----------------------------
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

# ----------------------------
# Spark session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("Data_Quality_Checks")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)
spark.conf.set(
    f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
    account_key
)

# ----------------------------
# Read processed Delta data
# ----------------------------
processed_path = f"abfss://processed@{account_name}.dfs.core.windows.net/sales/"

df = spark.read.format("delta").load(processed_path)

# Filter only current partition
df = df.filter(
    (col("order_year") == year_p) &
    (col("order_month") == month_p)
)

# ----------------------------
# Data Quality Checks
# ----------------------------
errors = []

if df.count() == 0:
    errors.append("No records found for given date")

if df.filter(col("order_id").isNull()).count() > 0:
    errors.append("order_id contains NULL values")

if df.filter(col("quantity") <= 0).count() > 0:
    errors.append("quantity must be > 0")

if df.filter(col("price") <= 0).count() > 0:
    errors.append("price must be > 0")

if df.filter(col("total_amount") <= 0).count() > 0:
    errors.append("total_amount must be > 0")

# ----------------------------
# Final result
# ----------------------------
if errors:
    print("DATA QUALITY CHECK FAILED")
    for e in errors:
        print(f"- {e}")
    spark.stop()
    sys.exit(1)

print("DATA QUALITY CHECK PASSED")

spark.stop()
