import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ===== PRODUCTS =====
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

products_path = os.path.join(project_root, "../data", "data/raw", "products", "products.csv")
with open(products_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["product_id", "product_name", "category", "price", "is_active"])
    for p in products:
        writer.writerow([p[0], p[1], p[2], p[3], str(p[4]).lower()])
    for p in random.sample(products, 3):
        writer.writerow([p[0], p[1], p[2], p[3], str(p[4]).lower()])

print(f"Products: {len(products) + 3} rows (3 duplicates)")


# ===== CUSTOMERS =====
cities = ["Hyderabad", "Vijayawada", "Chennai", "Bangalore", "Mumbai", "Visakhapatnam"]
first_names = ["Raju", "Sita", "Venkat", "Lakshmi", "Arjun", "Priya", "Kiran", "Deepa",
               "Suresh", "Anitha", "Ramesh", "Kavitha", "Manoj", "Swathi", "Prasad",
               "Divya", "Harish", "Mounika", "Srinivas", "Padma"]
last_names = ["Kumar", "Devi", "Rao", "Reddy", "Sharma", "Naidu", "Prasad", "Gupta"]

customers = []
for i in range(100):
    cid = 101 + i
    name = f"{random.choice(first_names)} {random.choice(last_names)}"
    email = f"{name.split()[0].lower()}{cid}@email.com" if random.random() > 0.1 else ""
    phone = f"98{random.randint(10000000, 99999999)}" if random.random() > 0.15 else ""
    city = random.choice(cities)
    reg_date = (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 500))).strftime("%Y-%m-%d")
    customers.append((cid, name, email, phone, city, reg_date))

customers_path = os.path.join(project_root, "../data", "data/raw", "customers", "customers.csv")
with open(customers_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["customer_id", "customer_name", "email", "phone", "city", "registered_date"])
    for c in customers:
        writer.writerow(c)
    for c in random.sample(customers, 10):
        writer.writerow(c)

print(f"Customers: {len(customers) + 10} rows (10 duplicates)")


# ===== ORDERS =====
statuses = ["completed", "completed", "completed", "completed", "pending", "cancelled"]
start_date = datetime(2024, 1, 1)

orders = []
for i in range(500):
    oid = i + 1
    cid = random.choice(customers)[0]
    pid = random.choice(products)[0]
    odate = (start_date + timedelta(days=random.randint(0, 180))).strftime("%Y-%m-%d")
    amount = round(random.uniform(50, 2000), 2)
    status = random.choice(statuses)
    orders.append((oid, cid, pid, odate, amount, status))


orders_path = os.path.join(project_root, "../data", "data/raw", "orders", "orders.csv")
with open(orders_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer_id", "product_id", "order_date", "total_amount", "status"])
    for o in orders:
        writer.writerow(o)
    for o in random.sample(orders, 30):
        writer.writerow(o)

print(f"Orders: {len(orders) + 30} rows (30 duplicates)")
print("\nSample data generated!")
