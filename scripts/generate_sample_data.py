"""Generate realistic sample data (~1GB) for BenMart Data Platform."""

import os
import csv
import json
import random
import pandas as pd
from datetime import datetime, timedelta

random.seed(42)
BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

# ─── Realistic Indian data pools ────────────────────────────

FIRST_NAMES = [
    "Raju", "Venkat", "Suresh", "Priya", "Lakshmi", "Anil", "Deepa", "Kiran",
    "Sanjay", "Meena", "Arjun", "Divya", "Rahul", "Sneha", "Vikram", "Anjali",
    "Ramesh", "Pooja", "Manoj", "Kavitha", "Naveen", "Swathi", "Ganesh", "Radha",
    "Prasad", "Bhavani", "Harish", "Rekha", "Chandra", "Padma", "Srinivas", "Uma",
    "Naresh", "Sunitha", "Rajesh", "Asha", "Mohan", "Vani", "Satish", "Madhavi"
]

LAST_NAMES = [
    "Kumar", "Reddy", "Sharma", "Patel", "Rao", "Singh", "Naidu", "Verma",
    "Gupta", "Yadav", "Pillai", "Nair", "Iyer", "Joshi", "Mishra", "Choudhary",
    "Das", "Patil", "Kulkarni", "Deshpande", "Menon", "Bhat", "Shetty", "Hegde"
]

CITIES = {
    "Hyderabad": ("Telangana", ["500001", "500032", "500072", "500081"]),
    "Bangalore": ("Karnataka", ["560001", "560034", "560078", "560102"]),
    "Chennai": ("Tamil Nadu", ["600001", "600028", "600042", "600096"]),
    "Mumbai": ("Maharashtra", ["400001", "400050", "400070", "400093"]),
    "Delhi": ("Delhi", ["110001", "110025", "110044", "110085"]),
    "Pune": ("Maharashtra", ["411001", "411038", "411057"]),
    "Kolkata": ("West Bengal", ["700001", "700020", "700064"]),
    "Ahmedabad": ("Gujarat", ["380001", "380015", "380054"]),
    "Jaipur": ("Rajasthan", ["302001", "302015", "302021"]),
    "Vizag": ("Andhra Pradesh", ["530001", "530016", "530045"]),
}

DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "rediffmail.com", "hotmail.com"]

CATEGORIES = {
    "Electronics":  {"prefix": ["Samsung", "Boat", "Realme", "JBL", "Sony", "Mi", "OnePlus", "Noise"],
                     "suffix": ["Earbuds", "Speaker", "Charger", "Power Bank", "Smartwatch", "Cable", "Adapter", "Headphones"]},
    "Clothing":     {"prefix": ["Cotton", "Silk", "Linen", "Denim", "Woolen", "Printed", "Plain", "Checked"],
                     "suffix": ["T-Shirt", "Shirt", "Kurta", "Jeans", "Saree", "Jacket", "Shorts", "Dress"]},
    "Groceries":    {"prefix": ["Organic", "Fresh", "Premium", "Fortune", "Aashirvaad", "Tata", "MTR", "Amul"],
                     "suffix": ["Rice 5kg", "Atta 10kg", "Oil 1L", "Dal 1kg", "Sugar 5kg", "Tea 500g", "Ghee 1L", "Milk 1L"]},
    "Home & Kitchen": {"prefix": ["Milton", "Prestige", "Pigeon", "Borosil", "Cello", "Bajaj", "Philips", "Crompton"],
                       "suffix": ["Mixer", "Cooker", "Pan", "Bottle Set", "Container", "Iron", "Fan", "Kettle"]},
    "Beauty":       {"prefix": ["Lakme", "Nivea", "Dove", "Himalaya", "Biotique", "Mamaearth", "Ponds", "Garnier"],
                     "suffix": ["Face Wash", "Shampoo", "Cream", "Serum", "Sunscreen", "Lipstick", "Lotion", "Hair Oil"]},
}

STATUSES = ["completed", "pending", "shipped", "cancelled", "returned"]
MESSY_STATUSES = [" completed ", "COMPLETED", "Completed", " pending", "SHIPPED ", "cancelled", " returned "]

DATE_START = datetime(2023, 1, 1)
DATE_END = datetime(2026, 9, 30)
DATE_RANGE_DAYS = (DATE_END - DATE_START).days


def random_date():
    return (DATE_START + timedelta(days=random.randint(0, DATE_RANGE_DAYS))).strftime("%Y-%m-%d")

def messy_date():
    """10% chance of bad date format."""
    if random.random() < 0.10:
        d = DATE_START + timedelta(days=random.randint(0, DATE_RANGE_DAYS))
        formats = [
            d.strftime("%d-%m-%Y"),        # wrong format
            d.strftime("%m/%d/%Y"),         # US format
            "bad-date",                      # garbage
            "",                              # empty
            d.strftime("%Y/%m/%d"),          # slash instead of dash
        ]
        return random.choice(formats)
    return random_date()

def random_phone():
    return f"{random.choice(['98', '97', '96', '95', '91', '90', '88', '87', '70', '63'])}{random.randint(10000000, 99999999)}"


# ═══════════════════════════════════════════════════════════════
#  PRODUCTS — 5,000 rows, Parquet
# ═══════════════════════════════════════════════════════════════

def generate_products():
    print("📦 Generating products...")
    products = []
    pid = 1

    for category, data in CATEGORIES.items():
        combos = [(p, s) for p in data["prefix"] for s in data["suffix"]]
        for prefix, suffix in combos:
            products.append({
                "product_id": pid,
                "product_name": f"{prefix} {suffix}",
                "category": category,
                "price": round(random.uniform(50, 25000), 2),
                "is_active": random.choice([True, True, True, True, False])  # 20% inactive
            })
            pid += 1

    # Pad to 5000 with variations
    while len(products) < 5000:
        cat = random.choice(list(CATEGORIES.keys()))
        prefix = random.choice(CATEGORIES[cat]["prefix"])
        suffix = random.choice(CATEGORIES[cat]["suffix"])
        variant = random.choice(["Pro", "Lite", "Max", "Mini", "Plus", "V2", "XL", "SE"])
        products.append({
            "product_id": pid,
            "product_name": f"{prefix} {suffix} {variant}",
            "category": cat,
            "price": round(random.uniform(50, 25000), 2),
            "is_active": random.choice([True, True, True, True, False])
        })
        pid += 1

    # Add 200 duplicates
    for _ in range(200):
        products.append(random.choice(products[:5000]))

    random.shuffle(products)

    path = os.path.join(BASE_DIR, "products")
    os.makedirs(path, exist_ok=True)
    df = pd.DataFrame(products)
    df.to_parquet(os.path.join(path, "products.parquet"), index=False)
    print(f"  ✅ Products: {len(products)} rows ({len(products) - 200} unique + 200 dupes)")


# ═══════════════════════════════════════════════════════════════
#  CUSTOMERS — 200,000 rows, JSON (chunked)
# ═══════════════════════════════════════════════════════════════

def generate_customers():
    print("👤 Generating customers...")
    customers = []

    for cid in range(1, 200_001):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)

        # 8% messy names (whitespace)
        name = f"  {first} {last}  " if random.random() < 0.08 else f"{first} {last}"

        # 12% null email
        email = None if random.random() < 0.12 else f"{first.lower()}.{last.lower()}{cid}@{random.choice(DOMAINS)}"

        # 8% null address
        if random.random() < 0.08:
            address = None
        else:
            city = random.choice(list(CITIES.keys()))
            state, pincodes = CITIES[city]
            address = {"city": city, "state": state, "pincode": random.choice(pincodes)}

        # Phone: array with 1-2 numbers, 5% null, 3% empty array
        if random.random() < 0.05:
            phone = None
        elif random.random() < 0.03:
            phone = []
        elif random.random() < 0.3:
            phone = [random_phone(), random_phone()]
        else:
            phone = [random_phone()]

        # 5% null registered_date
        reg_date = None if random.random() < 0.05 else random_date()

        customers.append({
            "customer_id": cid,
            "customer_name": name,
            "email": email,
            "phone": phone,
            "address": address,
            "registered_date": reg_date
        })

    # Add 5000 duplicates
    for _ in range(5000):
        customers.append(random.choice(customers[:200_000]))

    random.shuffle(customers)

    path = os.path.join(BASE_DIR, "customers")
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "customers.json"), "w") as f:
        json.dump(customers, f)

    print(f"  ✅ Customers: {len(customers)} rows ({len(customers) - 5000} unique + 5000 dupes)")


# ═══════════════════════════════════════════════════════════════
#  ORDERS — 5,000,000 rows, CSV (chunked write)
# ═══════════════════════════════════════════════════════════════

def generate_orders():
    print("🛒 Generating orders (5M rows, chunked)...")

    path = os.path.join(BASE_DIR, "orders")
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, "orders.csv")

    headers = ["order_id", "customer_id", "product_id", "order_date", "status", "total_amount"]
    chunk_size = 500_000
    total_rows = 5_000_000
    dupe_count = 50_000

    # Pre-generate duplicate order_ids
    dupe_ids = set(random.sample(range(1, total_rows + 1), dupe_count))

    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        oid = 1
        written = 0
        chunk_num = 0

        while written < total_rows + dupe_count:
            chunk = []
            for _ in range(min(chunk_size, total_rows + dupe_count - written)):
                customer_id = random.randint(1, 200_000)
                product_id = random.randint(1, 5000)
                order_date = messy_date()

                # 15% messy status
                if random.random() < 0.15:
                    status = random.choice(MESSY_STATUSES)
                else:
                    status = random.choice(STATUSES)

                # 8% bad amount (empty or negative)
                if random.random() < 0.06:
                    amount = ""
                elif random.random() < 0.02:
                    amount = str(round(-random.uniform(1, 500), 2))
                else:
                    amount = str(round(random.uniform(49.99, 49999.99), 2))

                chunk.append([oid, customer_id, product_id, order_date, status, amount])

                # Duplicate: write same order_id again
                if oid in dupe_ids:
                    chunk.append([oid, customer_id, product_id, order_date, status, amount])

                oid += 1

            writer.writerows(chunk)
            written += len(chunk)
            chunk_num += 1
            print(f"  📝 Chunk {chunk_num}: {written:,} rows written...")

    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  ✅ Orders: {written:,} rows | {file_size_mb:.1f} MB")


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("🚀 BenMart Sample Data Generator (1GB)")
    print("=" * 50)

    os.makedirs(BASE_DIR, exist_ok=True)

    generate_products()
    generate_customers()
    generate_orders()

    print("=" * 50)
    print("🎉 All data generated!")
    print(f"📁 Location: {os.path.abspath(BASE_DIR)}")
