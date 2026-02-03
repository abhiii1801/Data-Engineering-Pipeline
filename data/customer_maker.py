# Generates customer_db.json
# One-time execution

import json
from faker import Faker

fake = Faker("en_IN")

TOTAL_CUSTOMERS = 50000
START_ID = 100000

customers = []

for i in range(TOTAL_CUSTOMERS):
    customers.append({
        "customer_id": f"CUST{START_ID + i}",
        "customer_name": fake.name(),
        "city": fake.city()
    })

with open("customer_db.json", "w", encoding="utf-8") as f:
    json.dump(customers, f, indent=2)

print("customer_db.json created")
print(f"Total customers generated: {len(customers)}")
