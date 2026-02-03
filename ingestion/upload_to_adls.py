import os
import sys
from datetime import datetime
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

if not ACCOUNT_NAME or not ACCOUNT_KEY:
    print("ERROR: Azure credentials not found in .env file")
    sys.exit(1)

# ----------------------------
# Required schema
# ----------------------------
REQUIRED_COLUMNS = [
    "order_id",
    "order_date",
    "customer_id",
    "customer_name",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "price",
    "city",
    "payment_type"
]

# ----------------------------
# Get date input
# ----------------------------
if len(sys.argv) != 2:
    print("Usage: python upload_to_adls.py YYYY-MM-DD")
    sys.exit(1)

run_date = sys.argv[1]

try:
    run_date_obj = datetime.strptime(run_date, "%Y-%m-%d")
except ValueError:
    print("ERROR: Date format must be YYYY-MM-DD")
    sys.exit(1)

year = run_date_obj.strftime("%Y")
month = run_date_obj.strftime("%m")
day = run_date_obj.strftime("%d")

# ----------------------------
# Local file path
# ----------------------------
PROJECT_ROOT = "/opt/project"

local_file_path = (
    f"{PROJECT_ROOT}/data/{year}/{month}/{day}/sales_raw.csv"
)

if not os.path.exists(local_file_path):
    print(f"ERROR: File not found at {local_file_path}")
    sys.exit(1)

print(f"Reading file: {local_file_path}")

# ----------------------------
# Read CSV
# ----------------------------
df = pd.read_csv(local_file_path)

if df.empty:
    print("ERROR: CSV file is empty")
    sys.exit(1)

# ----------------------------
# Schema validation
# ----------------------------
missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]

if missing_cols:
    print(f"ERROR: Missing columns: {missing_cols}")
    sys.exit(1)

# ----------------------------
# Basic data quality checks
# ----------------------------
if df["order_id"].isnull().any():
    print("ERROR: order_id contains NULL values")
    sys.exit(1)

if (df["quantity"] <= 0).any():
    print("ERROR: quantity must be greater than 0")
    sys.exit(1)

if (df["price"] <= 0).any():
    print("ERROR: price must be greater than 0")
    sys.exit(1)

# ----------------------------
# Add ingestion metadata
# ----------------------------
df["ingestion_date"] = datetime.now().strftime("%Y-%m-%d")
df["source_system"] = "local_csv"

# ----------------------------
# Connect to Azure Data Lake
# ----------------------------
service_client = DataLakeServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
    credential=ACCOUNT_KEY
)

file_system_name = "raw"
directory_path = f"sales/{year}/{month}/{day}"
file_name = "sales_raw.csv"

file_system_client = service_client.get_file_system_client(
    file_system=file_system_name
)

directory_client = file_system_client.get_directory_client(directory_path)
file_client = directory_client.create_file(file_name)

# ----------------------------
# Upload data
# ----------------------------
csv_data = df.to_csv(index=False)

file_client.append_data(data=csv_data, offset=0, length=len(csv_data))
file_client.flush_data(len(csv_data))

print("SUCCESS: File uploaded to Azure Data Lake")
print(f"Path: raw/{directory_path}/{file_name}")
