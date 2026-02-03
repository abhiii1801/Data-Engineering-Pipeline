import pandas as pd
import random
from faker import Faker
from datetime import date, datetime
import os
import json
import sys

fake = Faker("en_IN")

# ---------- READ DATE FROM ARG ----------
if len(sys.argv) != 2:
    print("Usage: python generate_sales.py YYYY-MM-DD")
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

order_date = run_date_obj.date()

# CRITICAL: Define project root relative to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
# This points to: /opt/project/data/ (assuming script is in /opt/project/data/)

PRODUCT_DB_PATH = os.path.join(BASE_DIR, "product_db.json")
CUSTOMER_DB_PATH = os.path.join(BASE_DIR, "customer_db.json")
ORDER_SEQ_PATH = os.path.join(BASE_DIR, "order_seq.json")

# ---------- LOAD PRODUCT DIMENSION ----------
with open(PRODUCT_DB_PATH, "r", encoding="utf-8") as f:
    product_db = json.load(f)

products_by_category = {}
for p in product_db:
    products_by_category.setdefault(p["category"], []).append(p)

# ---------- LOAD CUSTOMER DIMENSION ----------
with open(CUSTOMER_DB_PATH, "r", encoding="utf-8") as f:
    customer_db = json.load(f)

# ---------- LOAD ORDER SEQUENCE ----------
with open(ORDER_SEQ_PATH, "r") as f:
    seq = json.load(f)
    last_order_id = seq["last_order_id"]

# ---------- CONFIG ----------
NUM_ROWS = random.randint(5000, 7000)
PAYMENT_TYPES = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery"]

# ---------- DATA GENERATION ----------
data = []
for _ in range(NUM_ROWS):
    product_category = random.choice(list(products_by_category.keys()))
    product = random.choice(products_by_category[product_category])
    customer = random.choice(customer_db)
    quantity = random.randint(1, 5)
    price = round(random.uniform(150, 80000), 2)
    last_order_id += 1

    data.append({
        "order_id": f"ORD{last_order_id:09d}",
        "order_date": order_date,
        "customer_id": customer["customer_id"],
        "customer_name": customer["customer_name"],
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity": quantity,
        "price": price,
        "city": customer["city"],
        "payment_type": random.choice(PAYMENT_TYPES)
    })

# ---------- WRITE CSV ----------
df = pd.DataFrame(data)

# FIX: Build absolute path for the output directory
# BASE_DIR is /opt/project/data/
output_dir = os.path.join(BASE_DIR, year, month, day)
os.makedirs(output_dir, exist_ok=True)

file_name = os.path.join(output_dir, "sales_raw.csv")
df.to_csv(file_name, index=False, date_format="%Y-%m-%d")

# ---------- UPDATE ORDER SEQ ----------
seq["last_order_id"] = last_order_id
# Use the absolute path defined earlier
with open(ORDER_SEQ_PATH, "w") as f:
    json.dump(seq, f, indent=2)

print(f"SUCCESS: CSV generated at {file_name}")
print(f"Rows generated: {len(df)}")