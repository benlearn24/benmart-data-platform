"""
generate_sample_data.py — Generate REALISTIC sample data for BenMart.

Creates messy, real-world-like test data:
  - orders.csv      (CSV — whitespace, empty fields, mixed case, bad dates)
  - customers.json  (JSON — nested address, null fields, phone as list)
  - products.parquet (Parquet — clean, product team pipeline output)

Purpose: Test that bronze_processor handles real-world data correctly.
"""

import csv
import json
import random
import os
from datetime import datetime, timedelta

random.seed(42)

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
raw_data_dir = os.path.join(project_root, "data", "raw")


# ===== PRODUCTS (Parquet — clean, typed) =====
# Product team's own pipeline output — already clean
products = [
    (201, "Basmati Rice", "Groceries", 120.00, True),
    (202, "Tomatoes", "Vegetables", 40.00, True),
    (203, "Milk", "Dairy", 28.00, True),
    (204, "Chicken", "Meat", 220.00, True),
    (205, "Mangoes", "Fruits", 150.00, False),
    (206, "Onions", "Vegetables", 35.00, True),
    (207, "Paneer", "Dairy", 90.00, True),
    (208, "Eggs", "Dairy", 72.00, True),
    (209, "Atta", "Groceries", 55.00, True),
    (210, "Bananas", "Fruits", 45.00, True),
    (211, "Potatoes", "Vegetables", 30.00, True),
    (212, "Curd", "Dairy", 25.00, True),
    (213, "Sugar", "Groceries", 48.00, True),
    (214, "Oil", "Groceries", 180.00, True),
    (215, "Dal", "Groceries", 95.00, True),
]

products_dir = os.path.join(raw_data_dir, "products")
os.makedirs(products_dir, exist_ok=True)
products_path = os.path.join(products_dir, "products.parquet")

try:
    import pandas as pd

    products_df = pd.DataFrame(products, columns=["product_id", "product_name", "category", "price", "is_active"])
    dupes = products_df.sample(n=3, random_state=42)
    products_df = pd.concat([products_df, dupes], ignore_index=True)
    products_df.to_parquet(products_path, index=False)
    print(f"Products: {len(products_df)} rows (3 duplicates) → Parquet ✅")

except ImportError:
    print("WARNING: pandas not installed. Run: pip install pandas pyarrow")


# ===== CUSTOMERS (JSON — nested, nulls, messy) =====
# Web app API export — real-world messiness
cities_states = {
    "Hyderabad": "Telangana",
    "Vijayawada": "Andhra Pradesh",
    "Chennai": "Tamil Nadu",
    "Bangalore": "Karnataka",
    "Mumbai": "Maharashtra",
    "Visakhapatnam": "Andhra Pradesh"
}
pincodes = {
    "Hyderabad": "500001",
    "Vijayawada": "520001",
    "Chennai": "600001",
    "Bangalore": "560001",
    "Mumbai": "400001",
    "Visakhapatnam": "530001"
}
first_names = ["Raju", "Sita", "Venkat", "Lakshmi", "Arjun", "Priya", "Kiran", "Deepa",
               "Suresh", "Anitha", "Ramesh", "Kavitha", "Manoj", "Swathi", "Prasad",
               "Divya", "Harish", "Mounika", "Srinivas", "Padma"]
last_names = ["Kumar", "Devi", "Rao", "Reddy", "Sharma", "Naidu", "Prasad", "Gupta"]

customers = []
for i in range(100):
    cid = 101 + i
    city = random.choice(list(cities_states.keys()))

    # Name — sometimes with whitespace (frontend doesn't trim)
    name = f"{random.choice(first_names)} {random.choice(last_names)}"
    if random.random() < 0.08:
        name = f"  {name}  "       # 8% chance — leading/trailing whitespace

    # Email — sometimes null, sometimes uppercase
    if random.random() < 0.12:
        email = None               # 12% — no email
    elif random.random() < 0.1:
        email = f"{name.strip().split()[0].upper()}{cid}@EMAIL.COM"  # 10% — uppercase
    else:
        email = f"{name.strip().split()[0].lower()}{cid}@email.com"

    # Phone — sometimes list, sometimes null, sometimes single string
    rand = random.random()
    if rand < 0.1:
        phone = None               # 10% — no phone at all
    elif rand < 0.25:
        phone = [f"98{random.randint(10000000, 99999999)}",
                 f"91{random.randint(10000000, 99999999)}"]   # 15% — two phones
    elif rand < 0.35:
        phone = []                  # 10% — empty list
    else:
        phone = [f"98{random.randint(10000000, 99999999)}"]   # 65% — single phone in list

    # Address — nested object, sometimes partial, sometimes null
    if random.random() < 0.08:
        address = None             # 8% — no address at all
    elif random.random() < 0.1:
        address = {                # 10% — partial address (no pincode)
            "city": city,
            "state": cities_states[city],
            "pincode": None
        }
    else:
        address = {                # 82% — full address
            "city": city,
            "state": cities_states[city],
            "pincode": pincodes[city]
        }

    # Registered date — sometimes null
    if random.random() < 0.05:
        reg_date = None            # 5% — registration date missing
    else:
        reg_date = (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 500))).strftime("%Y-%m-%d")

    customers.append({
        "customer_id": cid,
        "customer_name": name,
        "email": email,
        "phone": phone,
        "address": address,
        "registered_date": reg_date
    })

# Add 10 duplicates
for c in random.sample(customers, 10):
    customers.append(c.copy())

customers_dir = os.path.join(raw_data_dir, "customers")
os.makedirs(customers_dir, exist_ok=True)
customers_path = os.path.join(customers_dir, "customers.json")

with open(customers_path, "w", encoding="utf-8") as f:
    json.dump(customers, f, indent=2, ensure_ascii=False)

print(f"Customers: {len(customers)} rows (10 duplicates) → JSON ✅")
print(f"  Realistic: nested address, null emails, phone as list, whitespace names")


# ===== ORDERS (CSV — messy, real POS system output) =====
# POS system export — whitespace, empty fields, mixed case, bad data
statuses_messy = [
    "completed", "completed", "completed", "completed",
    "pending", "cancelled",
    "Completed",           # mixed case — some branches
    "CANCELLED",           # uppercase — old software
    " completed ",         # whitespace — data entry issue
    "",                    # empty string — cashier forgot
]
start_date = datetime(2024, 1, 1)

orders = []
for i in range(500):
    oid = i + 1
    cid = random.choice([c["customer_id"] for c in customers[:100]])
    pid = random.choice(products)[0]
    status = random.choice(statuses_messy)

    # Date — mostly correct, sometimes bad format
    if random.random() < 0.03:
        odate = "bad-date"                    # 3% — corrupted date
    elif random.random() < 0.05:
        odate = (start_date + timedelta(days=random.randint(0, 180))).strftime("%d-%m-%Y")  # 5% — wrong format DD-MM-YYYY
    else:
        odate = (start_date + timedelta(days=random.randint(0, 180))).strftime("%Y-%m-%d")  # 92% — correct

    # Amount — mostly correct, sometimes empty or zero
    if random.random() < 0.04:
        amount = ""                           # 4% — empty amount
    elif random.random() < 0.02:
        amount = "0.00"                       # 2% — zero amount
    else:
        amount = str(round(random.uniform(50, 2000), 2))  # 94% — normal

    orders.append((oid, cid, pid, odate, amount, status))

orders_dir = os.path.join(raw_data_dir, "orders")
os.makedirs(orders_dir, exist_ok=True)
orders_path = os.path.join(orders_dir, "orders.csv")

with open(orders_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer_id", "product_id", "order_date", "total_amount", "status"])
    for o in orders:
        writer.writerow(o)
    for o in random.sample(orders, 30):
        writer.writerow(o)

print(f"Orders: {len(orders) + 30} rows (30 duplicates) → CSV ✅")
print(f"  Realistic: whitespace status, empty amounts, bad dates, mixed case")
print(f"\nSample data generated! 🎉")
