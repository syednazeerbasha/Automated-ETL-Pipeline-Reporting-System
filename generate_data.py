import pandas as pd
import json
import uuid
import random
import os
from datetime import datetime, timedelta

# Configuration
DATA_DIR = "data"
CSV_FILENAME = "sales_dump.csv"
JSON_FILENAME = "web_transactions.json"
NUM_CSV_RECORDS = 1000  # Number of CSV rows
NUM_JSON_RECORDS = 200  # Number of JSON objects

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Product Catalog (Product Name, Base Price)
PRODUCTS = {
    "Laptop Pro": 1200.00,
    "Wireless Mouse": 25.00,
    "HD Monitor": 300.00,
    "Mechanical Keyboard": 150.00,
    "USB-C Hub": 45.00,
    "Ergo Chair": 450.00,
    "Enterprise Server": 15000.00, # High value item
    "SaaS License (Yearly)": 999.00
}

def get_random_date(start_date=datetime(2023, 1, 1), end_date=datetime.now()):
    """Generates a random datetime between start and end dates."""
    delta = end_date - start_date
    random_days = random.randrange(delta.days)
    random_seconds = random.randrange(86400)
    return start_date + timedelta(days=random_days, seconds=random_seconds)

def generate_csv_data():
    """Generates a large CSV dataset mimicking bulk export."""
    print(f"Generating {NUM_CSV_RECORDS} CSV records...")
    
    data = []
    for _ in range(NUM_CSV_RECORDS):
        prod_name = random.choice(list(PRODUCTS.keys()))
        base_price = PRODUCTS[prod_name]
        
        # Add some random variance to price (e.g., discounts or tax)
        actual_price = round(base_price * random.uniform(0.9, 1.1), 2)
        
        # 1% Chance of a massive data entry error (Anomaly)
        if random.random() < 0.01:
            actual_price = actual_price * 100 

        row = {
            "transaction_id": str(uuid.uuid4()),
            "date": get_random_date().strftime("%Y-%m-%d %H:%M:%S"),
            "product": prod_name,
            "category": "Hardware" if "Server" in prod_name or "Laptop" in prod_name else "Accessory",
            "region": random.choice(["NA", "EU", "APAC", "LATAM"]),
            "amount": actual_price
        }
        data.append(row)

    df = pd.DataFrame(data)
    
    # Save to CSV
    csv_path = os.path.join(DATA_DIR, CSV_FILENAME)
    df.to_csv(csv_path, index=False)
    print(f"✅ CSV saved to: {csv_path}")

def generate_json_data():
    """Generates a JSON dataset mimicking web/app logs."""
    print(f"Generating {NUM_JSON_RECORDS} JSON records...")
    
    data = []
    for _ in range(NUM_JSON_RECORDS):
        prod_name = random.choice(list(PRODUCTS.keys()))
        base_price = PRODUCTS[prod_name]
        
        record = {
            "id": str(uuid.uuid4()),
            "timestamp": get_random_date(start_date=datetime.now() - timedelta(days=7)).isoformat(),
            "item": prod_name,
            "customer_type": random.choice(["Guest", "Premium", "Standard"]),
            "price": round(base_price, 2)
        }
        data.append(record)

    # Save to JSON
    json_path = os.path.join(DATA_DIR, JSON_FILENAME)
    with open(json_path, "w") as f:
        json.dump(data, f, indent=4)
    print(f"✅ JSON saved to: {json_path}")

if __name__ == "__main__":
    print("--- Starting Data Generation ---")
    generate_csv_data()
    generate_json_data()
    print("--- Data Generation Complete ---")