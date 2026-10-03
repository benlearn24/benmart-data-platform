"""BenMart API Ingestion — Fetches product pricing data and writes to S3."""

import json
import boto3
import random
from datetime import datetime, timezone

s3_client = boto3.client('s3')

BUCKET = "benmart-dev-raw"
PREFIX = "api-data/pricing"

PRODUCTS = [
    {"product_id": 1, "product_name": "Samsung Galaxy M14", "category": "Electronics"},
    {"product_id": 2, "product_name": "boAt Rockerz 450", "category": "Electronics"},
    {"product_id": 3, "product_name": "Allen Solly Shirt", "category": "Clothing"},
    {"product_id": 4, "product_name": "Aashirvaad Atta 5kg", "category": "Groceries"},
    {"product_id": 5, "product_name": "Prestige Cooker 3L", "category": "Home & Kitchen"},
    {"product_id": 6, "product_name": "Lakme Lipstick", "category": "Beauty"},
    {"product_id": 7, "product_name": "Redmi Note 13", "category": "Electronics"},
    {"product_id": 8, "product_name": "Levi's Jeans", "category": "Clothing"},
    {"product_id": 9, "product_name": "Tata Salt 1kg", "category": "Groceries"},
    {"product_id": 10, "product_name": "Philips Trimmer", "category": "Electronics"},
]


def fetch_pricing_data():
    records = []
    for product in PRODUCTS:
        base_price = random.uniform(100, 25000)
        discount_pct = random.choice([0, 5, 10, 15, 20, 25])
        final_price = round(base_price * (1 - discount_pct / 100), 2)

        records.append({
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "base_price": round(base_price, 2),
            "discount_percentage": discount_pct,
            "final_price": final_price,
            "currency": "INR",
            "price_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "fetched_at": datetime.now(timezone.utc).isoformat()
        })

    return records


def lambda_handler(event, context):
    print("Fetching pricing data...")
    records = fetch_pricing_data()
    print(f"Fetched {len(records)} product prices")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3_key = f"{PREFIX}/{today}/pricing.json"

    body = json.dumps(records, indent=2)
    s3_client.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=body,
        ContentType="application/json"
    )
    print(f"Written to s3://{BUCKET}/{s3_key} ({len(body)} bytes)")

    return {
        "statusCode": 200,
        "body": {
            "message": "Pricing data ingested successfully",
            "records": len(records),
            "s3_path": f"s3://{BUCKET}/{s3_key}",
            "date": today
        }
    }
