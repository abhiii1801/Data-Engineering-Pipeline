import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from delta.tables import DeltaTable

# ----------------------------
# Date input
# ----------------------------
if len(sys.argv) != 2:
    print("Usage: spark-submit load_to_sql.py YYYY-MM-DD")
    sys.exit(1)

run_date = sys.argv[1]
dt = datetime.strptime(run_date, "%Y-%m-%d")
year_p = dt.year
month_p = dt.month

# ----------------------------
# Env vars
# ----------------------------
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

sql_server = os.getenv("AZURE_SQL_SERVER")
sql_db = os.getenv("AZURE_SQL_DB")
sql_user = os.getenv("AZURE_SQL_USER")
sql_password = os.getenv("AZURE_SQL_PASSWORD")

jdbc_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_db};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "loginTimeout=30;"
)

# ----------------------------
# Spark session (Delta enabled)
# ----------------------------
spark = (
    SparkSession.builder
    .appName("Load_To_Azure_SQL")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.conf.set(
    f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
    account_key
)

# ----------------------------
# Read processed Delta
# ----------------------------
processed_path = f"abfss://processed@{account_name}.dfs.core.windows.net/sales/"

# ---- IMPORTANT SAFETY CONFIG ----
spark.catalog.clearCache()

df = spark.read.format("delta").load(processed_path)

df = df.persist()
rows = df.count()
print("ROWS TO LOAD:", rows)

if rows == 0:
    raise Exception("No data found for SQL load")

df_sql = df.select(
    "order_id",
    "product_id",
    "customer_id",
    "quantity",
    "price",
    "total_amount",
    "price_bucket",
    "order_date",
    "order_year",
    "order_month",
    "processing_date"
)

df_sql.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.fact_sales") \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("overwrite") \
    .save()


print("ROWS TO LOAD:", df_sql.count())
df_sql.show(5, truncate=False)


print("SUCCESS: Data loaded into Azure SQL")

spark.stop()
