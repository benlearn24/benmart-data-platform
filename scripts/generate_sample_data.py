"""Generate production-realistic sample data for BenMart Data Platform.
10M+ orders (15 cols), 500K customers (15 cols), 50K products (14 cols).
Seasonal patterns, regional distribution, customer behavior, realistic Indian data.
"""

import os
import csv
import json
import random
import pandas as pd
from datetime import datetime, timedelta

random.seed(42)
BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

# ─── Indian Names ────────────────────────────────────────────

FIRST_NAMES = [
    "Raju", "Venkat", "Suresh", "Priya", "Lakshmi", "Anil", "Deepa", "Kiran",
    "Sanjay", "Meena", "Arjun", "Divya", "Rahul", "Sneha", "Vikram", "Anjali",
    "Ramesh", "Pooja", "Manoj", "Kavitha", "Naveen", "Swathi", "Ganesh", "Radha",
    "Prasad", "Bhavani", "Harish", "Rekha", "Chandra", "Padma", "Srinivas", "Uma",
    "Naresh", "Sunitha", "Rajesh", "Asha", "Mohan", "Vani", "Satish", "Madhavi",
    "Sunil", "Jyothi", "Rajendra", "Saritha", "Pavan", "Lavanya", "Mahesh", "Keerthi",
    "Dinesh", "Revathi", "Siddharth", "Nandini", "Ajay", "Sowmya", "Krishna", "Aparna",
    "Ravi", "Mythili", "Surya", "Gayathri", "Varun", "Tejaswi", "Ashok", "Sirisha",
]

LAST_NAMES = [
    "Kumar", "Reddy", "Sharma", "Patel", "Rao", "Singh", "Naidu", "Verma",
    "Gupta", "Yadav", "Pillai", "Nair", "Iyer", "Joshi", "Mishra", "Choudhary",
    "Das", "Patil", "Kulkarni", "Deshpande", "Menon", "Bhat", "Shetty", "Hegde",
    "Chauhan", "Thakur", "Tiwari", "Pandey", "Saxena", "Agarwal", "Mehta", "Shah",
    "Rajan", "Varma", "Swamy", "Gowda", "Sethi", "Bose", "Sen", "Roy",
]

GENDERS = ["Male", "Female", "Other"]
GENDER_WEIGHTS = [0.52, 0.46, 0.02]

# ─── Cities — 32 cities, tiered ─────────────────────────────

CITIES = {
    "Hyderabad":    ("Telangana",        ["500001", "500032", "500072", "500081", "500034"]),
    "Bangalore":    ("Karnataka",        ["560001", "560034", "560078", "560102", "560043"]),
    "Chennai":      ("Tamil Nadu",       ["600001", "600028", "600042", "600096", "600017"]),
    "Mumbai":       ("Maharashtra",      ["400001", "400050", "400070", "400093", "400028"]),
    "Delhi":        ("Delhi",            ["110001", "110025", "110044", "110085", "110019"]),
    "Kolkata":      ("West Bengal",      ["700001", "700020", "700064", "700091", "700032"]),
    "Pune":         ("Maharashtra",      ["411001", "411038", "411057", "411014"]),
    "Ahmedabad":    ("Gujarat",          ["380001", "380015", "380054", "380009"]),
    "Jaipur":       ("Rajasthan",        ["302001", "302015", "302021"]),
    "Vizag":        ("Andhra Pradesh",   ["530001", "530016", "530045"]),
    "Lucknow":      ("Uttar Pradesh",    ["226001", "226010", "226020"]),
    "Chandigarh":   ("Chandigarh",       ["160001", "160017", "160022"]),
    "Bhopal":       ("Madhya Pradesh",   ["462001", "462011", "462023"]),
    "Kochi":        ("Kerala",           ["682001", "682016", "682024"]),
    "Indore":       ("Madhya Pradesh",   ["452001", "452010", "452018"]),
    "Coimbatore":   ("Tamil Nadu",       ["641001", "641012", "641018"]),
    "Nagpur":       ("Maharashtra",      ["440001", "440010", "440022"]),
    "Patna":        ("Bihar",            ["800001", "800010", "800020"]),
    "Thiruvananthapuram": ("Kerala",     ["695001", "695010", "695014"]),
    "Vadodara":     ("Gujarat",          ["390001", "390007", "390015"]),
    "Vijayawada":   ("Andhra Pradesh",   ["520001", "520010"]),
    "Warangal":     ("Telangana",        ["506001", "506002"]),
    "Mysore":       ("Karnataka",        ["570001", "570010"]),
    "Mangalore":    ("Karnataka",        ["575001", "575003"]),
    "Hubli":        ("Karnataka",        ["580001", "580020"]),
    "Tirupati":     ("Andhra Pradesh",   ["517501", "517502"]),
    "Salem":        ("Tamil Nadu",       ["636001", "636007"]),
    "Ranchi":       ("Jharkhand",        ["834001", "834002"]),
    "Guwahati":     ("Assam",            ["781001", "781005"]),
    "Dehradun":     ("Uttarakhand",      ["248001", "248005"]),
    "Raipur":       ("Chhattisgarh",     ["492001", "492007"]),
    "Bhubaneswar":  ("Odisha",           ["751001", "751010"]),
}

METRO_CITIES = ["Hyderabad", "Bangalore", "Chennai", "Mumbai", "Delhi", "Kolkata", "Pune", "Ahmedabad"]
TIER2_CITIES = ["Jaipur", "Vizag", "Lucknow", "Chandigarh", "Bhopal", "Kochi", "Indore",
                "Coimbatore", "Nagpur", "Patna", "Thiruvananthapuram", "Vadodara"]
TIER3_CITIES = ["Vijayawada", "Warangal", "Mysore", "Mangalore", "Hubli", "Tirupati",
                "Salem", "Ranchi", "Guwahati", "Dehradun", "Raipur", "Bhubaneswar"]

DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "rediffmail.com", "hotmail.com", "icloud.com"]

# ─── Products — 10 categories with sub-categories ───────────

CATEGORIES = {
    "Electronics": {
        "brands": ["Samsung", "boAt", "Realme", "JBL", "Sony", "Mi", "OnePlus", "Noise", "Philips", "LG"],
        "sub_categories": {
            "Earbuds":     (499, 5999),
            "Speakers":    (799, 12999),
            "Chargers":    (299, 2499),
            "Power Banks": (599, 3999),
            "Smartwatches": (999, 14999),
            "Cables":      (99, 999),
            "Headphones":  (499, 7999),
            "TWS":         (699, 9999),
        },
    },
    "Clothing": {
        "brands": ["Allen Solly", "Peter England", "Van Heusen", "Levi's", "US Polo",
                    "Raymond", "Arrow", "Fabindia", "W", "Biba"],
        "sub_categories": {
            "T-Shirts":  (199, 2499),
            "Shirts":    (499, 3999),
            "Kurtas":    (399, 4999),
            "Jeans":     (699, 3499),
            "Sarees":    (499, 14999),
            "Jackets":   (799, 5999),
            "Dresses":   (599, 4999),
            "Trousers":  (599, 3499),
        },
    },
    "Groceries": {
        "brands": ["Aashirvaad", "Fortune", "Tata", "MTR", "Amul",
                    "Mother Dairy", "Patanjali", "24 Mantra", "Organic Tattva", "Daawat"],
        "sub_categories": {
            "Rice & Atta":  (120, 899),
            "Oils":         (99, 499),
            "Pulses":       (80, 350),
            "Spices":       (30, 299),
            "Dairy":        (25, 599),
            "Tea & Coffee": (99, 799),
            "Snacks":       (20, 399),
            "Beverages":    (15, 249),
        },
    },
    "Home & Kitchen": {
        "brands": ["Milton", "Prestige", "Pigeon", "Borosil", "Cello",
                    "Bajaj", "Crompton", "Havells", "Butterfly", "Wonderchef"],
        "sub_categories": {
            "Cookware":    (299, 5999),
            "Cookers":     (699, 4999),
            "Storage":     (199, 2999),
            "Bottles":     (149, 999),
            "Kitchen Tools": (99, 1999),
            "Fans":        (999, 4999),
            "Kettles":     (499, 2999),
        },
    },
    "Beauty": {
        "brands": ["Lakme", "Nivea", "Dove", "Himalaya", "Biotique",
                    "Mamaearth", "Ponds", "Garnier", "L'Oreal", "Maybelline"],
        "sub_categories": {
            "Face Care":  (99, 1499),
            "Hair Care":  (99, 999),
            "Skin Care":  (149, 2499),
            "Makeup":     (199, 2999),
            "Fragrances": (299, 4999),
            "Bath & Body": (49, 799),
        },
    },
    "Sports & Fitness": {
        "brands": ["Nike", "Adidas", "Puma", "Decathlon", "Yonex",
                    "Cosco", "Nivia", "Vector X", "SG", "SS"],
        "sub_categories": {
            "Shoes":       (999, 12999),
            "Cricket":     (299, 8999),
            "Fitness":     (199, 5999),
            "Badminton":   (299, 6999),
            "Football":    (199, 4999),
            "Yoga":        (149, 2999),
        },
    },
    "Books & Stationery": {
        "brands": ["Classmate", "Navneet", "Apsara", "Camlin", "Parker",
                    "Faber Castell", "Staedtler", "Pilot", "Reynolds", "Cello"],
        "sub_categories": {
            "Notebooks":   (25, 599),
            "Pens":        (10, 1499),
            "Art Supplies": (49, 1999),
            "Diaries":     (99, 799),
            "Office":      (49, 999),
        },
    },
    "Mobiles & Tablets": {
        "brands": ["Samsung", "Redmi", "Realme", "OnePlus", "Vivo",
                    "Oppo", "Apple", "Nothing", "Motorola", "iQOO"],
        "sub_categories": {
            "Budget Phones":  (6999, 14999),
            "Mid Range":      (15000, 29999),
            "Flagship":       (30000, 89999),
            "Tablets":        (8999, 49999),
            "Accessories":    (99, 2999),
        },
    },
    "Toys & Baby": {
        "brands": ["Fisher Price", "Funskool", "Lego", "Hot Wheels", "Barbie",
                    "Nerf", "Play-Doh", "Chicco", "Mee Mee", "LuvLap"],
        "sub_categories": {
            "Building Toys": (199, 4999),
            "Board Games":   (149, 2999),
            "Action Figures": (199, 3999),
            "Baby Care":     (99, 2999),
            "Baby Gear":     (499, 9999),
        },
    },
    "Appliances": {
        "brands": ["Samsung", "LG", "Whirlpool", "Godrej", "Haier",
                    "Voltas", "Blue Star", "Daikin", "IFB", "Bosch"],
        "sub_categories": {
            "Washing Machines": (8999, 45999),
            "Refrigerators":    (9999, 65999),
            "ACs":              (19999, 64999),
            "Microwaves":       (3999, 19999),
            "Water Purifiers":  (2999, 14999),
            "Geysers":          (2999, 12999),
        },
    },
}

STATUSES = ["completed", "pending", "shipped", "cancelled", "returned"]
MESSY_STATUSES = [" completed ", "COMPLETED", "Completed", " pending", "SHIPPED ",
                  "cancelled", " returned ", "Pending", "CANCELLED"]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "COD", "Net Banking", "Wallet", "EMI"]
PAYMENT_WEIGHTS = [0.40, 0.15, 0.15, 0.15, 0.07, 0.05, 0.03]

PAYMENT_STATUSES = ["paid", "pending", "refunded", "failed"]
ORDER_SOURCES = ["mobile_app", "website", "mobile_web"]
ORDER_SOURCE_WEIGHTS = [0.55, 0.30, 0.15]

REG_SOURCES = ["organic", "referral", "google_ads", "facebook_ads", "instagram", "youtube"]
REG_SOURCE_WEIGHTS = [0.35, 0.20, 0.20, 0.10, 0.10, 0.05]

LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]

VARIANTS = ["", "Pro", "Lite", "Max", "Mini", "Plus", "V2", "XL", "SE", "Neo", "Ultra"]

DATE_START = datetime(2023, 1, 1)
DATE_END = datetime(2026, 9, 30)
DATE_RANGE_DAYS = (DATE_END - DATE_START).days


# ─── Helper Functions ────────────────────────────────────────

def get_seasonal_weight(date):
    month = date.month
    if month in (10, 11): return 2.5    # Diwali
    if month == 12: return 2.0           # Year-end
    if month == 8: return 1.8            # Independence Day
    if month in (1, 7): return 1.5       # Republic Day / Summer sale
    if month == 2: return 1.3            # Wedding season
    if month in (4, 5): return 0.7       # Summer heat — low
    return 1.0

def weighted_random_date():
    while True:
        d = DATE_START + timedelta(days=random.randint(0, DATE_RANGE_DAYS))
        if random.random() < get_seasonal_weight(d) / 2.5:
            return d

def messy_date(d):
    if random.random() < 0.10:
        return random.choice([
            d.strftime("%d-%m-%Y"), d.strftime("%m/%d/%Y"),
            "bad-date", "", d.strftime("%Y/%m/%d"),
        ])
    return d.strftime("%Y-%m-%d")

def weighted_city():
    r = random.random()
    if r < 0.60: return random.choice(METRO_CITIES)
    if r < 0.90: return random.choice(TIER2_CITIES)
    return random.choice(TIER3_CITIES)

def random_phone():
    prefix = random.choice(['98', '97', '96', '95', '91', '90', '88', '87', '70', '63'])
    return f"{prefix}{random.randint(10000000, 99999999)}"

def weighted_payment():
    return random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]

def random_dob():
    """Generate DOB for age 18-70."""
    age = random.randint(18, 70)
    base = datetime(2026, 1, 1) - timedelta(days=age * 365 + random.randint(0, 364))
    return base.strftime("%Y-%m-%d")


# ═════════════════════════════════════════════════════════════
#  PRODUCTS — 50,000 rows, 14 columns, Parquet
# ═════════════════════════════════════════════════════════════

def generate_products():
    print("📦 Generating products (50K, 14 columns)...")
    products = []
    pid = 1

    for category, cat_data in CATEGORIES.items():
        brands = cat_data["brands"]
        for sub_cat, (price_lo, price_hi) in cat_data["sub_categories"].items():
            for brand in brands:
                for variant in random.sample(VARIANTS, k=min(4, len(VARIANTS))):
                    name = f"{brand} {sub_cat} {variant}".strip()
                    mrp = round(random.uniform(price_lo, price_hi), 2)
                    discount_pct = random.choice([0, 0, 5, 10, 15, 20, 25, 30, 40, 50])
                    selling_price = round(mrp * (1 - discount_pct / 100), 2)

                    products.append({
                        "product_id": pid,
                        "product_name": name,
                        "brand": brand,
                        "category": category,
                        "sub_category": sub_cat,
                        "mrp": mrp,
                        "selling_price": selling_price,
                        "discount_percentage": discount_pct,
                        "stock_quantity": random.randint(0, 5000),
                        "weight_kg": round(random.uniform(0.05, 30.0), 2),
                        "avg_rating": round(random.uniform(1.0, 5.0), 1),
                        "review_count": random.randint(0, 25000),
                        "is_active": random.choices([True, False], weights=[0.82, 0.18])[0],
                        "launch_date": (DATE_START + timedelta(days=random.randint(0, DATE_RANGE_DAYS))).strftime("%Y-%m-%d"),
                    })
                    pid += 1
                    if pid > 50000:
                        break
                if pid > 50000: break
            if pid > 50000: break
        if pid > 50000: break

    # Pad to 50K
    while len(products) < 50000:
        cat = random.choice(list(CATEGORIES.keys()))
        brands = CATEGORIES[cat]["brands"]
        sub_cats = list(CATEGORIES[cat]["sub_categories"].keys())
        sub_cat = random.choice(sub_cats)
        price_lo, price_hi = CATEGORIES[cat]["sub_categories"][sub_cat]
        brand = random.choice(brands)
        variant = random.choice(VARIANTS)
        mrp = round(random.uniform(price_lo, price_hi), 2)
        discount_pct = random.choice([0, 5, 10, 15, 20, 25, 30])
        selling_price = round(mrp * (1 - discount_pct / 100), 2)

        products.append({
            "product_id": pid,
            "product_name": f"{brand} {sub_cat} {variant}".strip(),
            "brand": brand,
            "category": cat,
            "sub_category": sub_cat,
            "mrp": mrp,
            "selling_price": selling_price,
            "discount_percentage": discount_pct,
            "stock_quantity": random.randint(0, 5000),
            "weight_kg": round(random.uniform(0.05, 30.0), 2),
            "avg_rating": round(random.uniform(1.0, 5.0), 1),
            "review_count": random.randint(0, 25000),
            "is_active": random.choices([True, False], weights=[0.82, 0.18])[0],
            "launch_date": (DATE_START + timedelta(days=random.randint(0, DATE_RANGE_DAYS))).strftime("%Y-%m-%d"),
        })
        pid += 1

    # 1000 duplicates (2%)
    products.extend(random.choices(products[:50000], k=1000))
    random.shuffle(products)

    path = os.path.join(BASE_DIR, "products")
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, "products.parquet")
    pd.DataFrame(products).to_parquet(filepath, index=False)

    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  ✅ Products: {len(products):,} rows × 14 cols | {size_mb:.1f} MB")


# ═════════════════════════════════════════════════════════════
#  CUSTOMERS — 500,000 rows, 15 columns, JSON
# ═════════════════════════════════════════════════════════════

def generate_customers():
    print("👤 Generating customers (500K, 15 columns)...")
    customers = []

    for cid in range(1, 500_001):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)

        # 8% messy names
        name = f"  {first} {last}  " if random.random() < 0.08 else f"{first} {last}"

        # 10% null email
        email = None if random.random() < 0.10 else f"{first.lower()}.{last.lower()}{cid}@{random.choice(DOMAINS)}"

        # Gender
        gender = random.choices(GENDERS, weights=GENDER_WEIGHTS, k=1)[0]

        # DOB — 3% null
        dob = None if random.random() < 0.03 else random_dob()

        # Phone — 4% null, 2% empty array
        if random.random() < 0.04:
            phone = None
        elif random.random() < 0.02:
            phone = []
        elif random.random() < 0.25:
            phone = [random_phone(), random_phone()]
        else:
            phone = [random_phone()]

        # Address — 6% null, regional distribution
        if random.random() < 0.06:
            address = None
        else:
            city = weighted_city()
            state, pincodes = CITIES[city]
            address = {"city": city, "state": state, "pincode": random.choice(pincodes)}

        # Registration date
        reg_date = None if random.random() < 0.03 else weighted_random_date().strftime("%Y-%m-%d")

        # Registration source
        reg_source = random.choices(REG_SOURCES, weights=REG_SOURCE_WEIGHTS, k=1)[0]

        # Preferred payment
        pref_payment = weighted_payment()

        # Active + verified
        is_active = random.choices([True, False], weights=[0.85, 0.15])[0]
        is_verified = random.choices([True, False], weights=[0.70, 0.30])[0]

        # Loyalty tier — weighted (most are Bronze)
        loyalty = random.choices(LOYALTY_TIERS, weights=[0.50, 0.30, 0.15, 0.05], k=1)[0]

        customers.append({
            "customer_id": cid,
            "customer_name": name,
            "email": email,
            "phone": phone,
            "gender": gender,
            "date_of_birth": dob,
            "address": address,
            "registered_date": reg_date,
            "registration_source": reg_source,
            "preferred_payment": pref_payment,
            "is_active": is_active,
            "is_verified": is_verified,
            "loyalty_tier": loyalty,
        })

        if cid % 100_000 == 0:
            print(f"  📝 {cid:,} customers generated...")

    # 10K duplicates
    customers.extend(random.choices(customers[:500_000], k=10_000))
    random.shuffle(customers)

    path = os.path.join(BASE_DIR, "customers")
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, "customers.json")
    with open(filepath, "w") as f:
        json.dump(customers, f)

    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  ✅ Customers: {len(customers):,} rows × 15 cols | {size_mb:.1f} MB")


# ═════════════════════════════════════════════════════════════
#  ORDERS — 10,000,000 rows, 15 columns, CSV (chunked write)
# ═════════════════════════════════════════════════════════════

def generate_orders():
    print("🛒 Generating orders (10M rows, 15 columns, chunked)...")

    path = os.path.join(BASE_DIR, "orders")
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, "orders.csv")

    headers = [
        "order_id", "customer_id", "product_id", "order_date",
        "quantity", "unit_price", "discount_amount", "total_amount",
        "status", "payment_method", "payment_status",
        "shipping_city", "shipping_state", "shipping_pincode",
        "order_source",
    ]
    chunk_size = 500_000
    total_unique = 30_000_000
    dupe_count = 200_000

    dupe_ids = set(random.sample(range(1, total_unique + 1), dupe_count))

    # Power buyers — top 5% customers place 30% orders
    power_buyers = list(range(1, 25_001))
    normal_buyers = list(range(25_001, 500_001))

    # Pre-load city data for shipping address
    all_cities = list(CITIES.keys())

    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        oid = 1
        written = 0
        chunk_num = 0

        while oid <= total_unique:
            chunk = []
            batch_end = min(oid + chunk_size, total_unique + 1)

            for current_oid in range(oid, batch_end):
                # Customer — power buyer bias
                if random.random() < 0.30:
                    customer_id = random.choice(power_buyers)
                else:
                    customer_id = random.choice(normal_buyers)

                product_id = random.randint(1, 50_000)

                # Seasonal date
                order_date_obj = weighted_random_date()
                order_date = messy_date(order_date_obj)

                # Quantity — most orders 1-2 items
                quantity = random.choices([1, 2, 3, 4, 5], weights=[0.55, 0.25, 0.12, 0.05, 0.03], k=1)[0]

                # Unit price — realistic range
                unit_price = round(random.uniform(29.99, 49999.99), 2)

                # Discount — 40% no discount, rest various amounts
                if random.random() < 0.40:
                    discount_amount = 0.00
                else:
                    discount_amount = round(random.uniform(10, unit_price * quantity * 0.3), 2)

                # Total = quantity × unit_price - discount
                total_raw = quantity * unit_price - discount_amount
                total_amount = round(max(total_raw, 0), 2)

                # 6% empty amount, 2% negative (bad data)
                if random.random() < 0.06:
                    total_amount_str = ""
                elif random.random() < 0.02:
                    total_amount_str = str(round(-random.uniform(1, 500), 2))
                else:
                    total_amount_str = str(total_amount)

                # Status — 15% messy
                if random.random() < 0.15:
                    status = random.choice(MESSY_STATUSES)
                else:
                    status = random.choice(STATUSES)

                # Payment method + status
                payment_method = weighted_payment()

                # Payment status correlates with order status
                clean_status = status.strip().lower()
                if clean_status == "cancelled":
                    payment_status = random.choices(["refunded", "pending", "paid"], weights=[0.6, 0.3, 0.1], k=1)[0]
                elif clean_status == "returned":
                    payment_status = random.choices(["refunded", "paid"], weights=[0.7, 0.3], k=1)[0]
                elif clean_status in ("completed", "shipped"):
                    payment_status = random.choices(["paid", "pending"], weights=[0.95, 0.05], k=1)[0]
                else:
                    payment_status = random.choices(PAYMENT_STATUSES, weights=[0.60, 0.25, 0.10, 0.05], k=1)[0]

                # Shipping address — 85% same as customer city, 15% different (gift/other address)
                ship_city = weighted_city()
                ship_state, ship_pincodes = CITIES[ship_city]
                ship_pincode = random.choice(ship_pincodes)

                # Order source — mobile app dominant in India
                order_source = random.choices(ORDER_SOURCES, weights=ORDER_SOURCE_WEIGHTS, k=1)[0]

                row = [
                    current_oid, customer_id, product_id, order_date,
                    quantity, unit_price, discount_amount, total_amount_str,
                    status, payment_method, payment_status,
                    ship_city, ship_state, ship_pincode,
                    order_source,
                ]
                chunk.append(row)

                if current_oid in dupe_ids:
                    chunk.append(row)

            writer.writerows(chunk)
            written += len(chunk)
            oid = batch_end
            chunk_num += 1

            if chunk_num % 2 == 0 or oid > total_unique:
                print(f"  📝 Chunk {chunk_num}: {written:,} rows written...")

    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  ✅ Orders: {written:,} rows × 15 cols | {file_size_mb:.1f} MB")


# ═════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("🚀 BenMart Sample Data Generator (Production — 10M+)")
    print("=" * 60)

    os.makedirs(BASE_DIR, exist_ok=True)

    generate_products()    # ~51K rows × 14 cols, Parquet, ~30 sec
    generate_customers()   # ~510K rows × 15 cols, JSON, ~2-3 min
    generate_orders()      # ~10.1M rows × 15 cols, CSV, ~5-8 min

    print("=" * 60)
    print("🎉 All data generated!")
    print(f"📁 Location: {os.path.abspath(BASE_DIR)}")
