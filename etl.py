import pandas as pd
import json
import random
import uuid
from datetime import datetime
from sqlalchemy.orm import Session
from models import SalesRecord, SessionLocal, init_db
from config import DATA_DIR
import os

# --- MOCK DATA GENERATORS (To make the code runnable immediately) ---
def generate_mock_files():
    """Generates dummy CSV and JSON files in the data folder."""
    
    # Mock CSV Data
    csv_data = [
        {"transaction_id": str(uuid.uuid4()), "product": "Laptop", "amount": 1200.00},
        {"transaction_id": str(uuid.uuid4()), "product": "Mouse", "amount": 25.50},
        {"transaction_id": str(uuid.uuid4()), "product": "Server", "amount": 50000.00}, # Anomaly
    ]
    pd.DataFrame(csv_data).to_csv(os.path.join(DATA_DIR, "sales.csv"), index=False)

    # Mock JSON Data
    json_data = [
        {"id": str(uuid.uuid4()), "item": "Monitor", "price": 300.00},
        {"id": str(uuid.uuid4()), "item": "HDMI Cable", "price": 15.00},
    ]
    with open(os.path.join(DATA_DIR, "sales.json"), "w") as f:
        json.dump(json_data, f)

# --- ETL FUNCTIONS ---

# def extract():
#     """Reads data from CSV, JSON, and (Mock) API."""
#     print("--- [EXTRACT] Starting extraction... ---")
#     generate_mock_files() # Ensure files exist

#     # 1. Read CSV
#     df_csv = pd.read_csv(os.path.join(DATA_DIR, "sales.csv"))
#     df_csv['source'] = 'CSV'

#     # 2. Read JSON
#     with open(os.path.join(DATA_DIR, "sales.json")) as f:
#         data_json = json.load(f)
#     # Normalize JSON structure to match CSV
#     df_json = pd.DataFrame(data_json).rename(columns={"id": "transaction_id", "item": "product", "price": "amount"})
#     df_json['source'] = 'JSON'
def extract():
    print("--- [EXTRACT] Starting extraction... ---")

    # 1. Read CSV (Handling the new filename)
    try:
        df_csv = pd.read_csv(os.path.join(DATA_DIR, "sales_dump.csv"))
        # Keep only columns we need for the database model
        df_csv = df_csv[['transaction_id', 'product', 'amount']]
        df_csv['source'] = 'CSV'
    except FileNotFoundError:
        print("CSV file not found. Run generate_data.py first.")
        df_csv = pd.DataFrame()

    # 2. Read JSON (Handling the new filename)
    try:
        with open(os.path.join(DATA_DIR, "web_transactions.json")) as f:
            data_json = json.load(f)
        # Map JSON keys to Database columns
        df_json = pd.DataFrame(data_json).rename(columns={
            "id": "transaction_id", 
            "item": "product", 
            "price": "amount"
        })
        df_json = df_json[['transaction_id', 'product', 'amount']]
        df_json['source'] = 'JSON'
    except FileNotFoundError:
         print("JSON file not found. Run generate_data.py first.")
         df_json = pd.DataFrame()

    # 3. Mock API Call
    # In a real scenario: response = requests.get('https://api.example.com/sales')
    api_data = [
        {"transaction_id": str(uuid.uuid4()), "product": "SaaS Subscription", "amount": 99.00, "source": "API"}
    ]
    df_api = pd.DataFrame(api_data)

    # Combine all sources
    df_combined = pd.concat([df_csv, df_json, df_api], ignore_index=True)
    print(f"--- [EXTRACT] Extracted {len(df_combined)} records. ---")
    return df_combined

def transform(df: pd.DataFrame):
    """Cleans data, adds timestamps, and flags anomalies."""
    print("--- [TRANSFORM] Processing data... ---")
    
    # 1. Validation: Drop rows with missing amounts
    df = df.dropna(subset=['amount'])

    # 2. Transformation: Convert types
    df['amount'] = df['amount'].astype(float)
    df['timestamp'] = datetime.utcnow()

    # 3. Business Logic: Flag Anomalies (e.g., sales > $10,000)
    df['is_anomaly'] = df['amount'] > 10000

    print("--- [TRANSFORM] Transformation complete. ---")
    return df

def load(df: pd.DataFrame):
    """Loads transformed data into SQLite."""
    print("--- [LOAD] Saving to database... ---")
    db: Session = SessionLocal()
    
    try:
        count = 0
        for _, row in df.iterrows():
            # Check if transaction already exists to avoid duplicates
            exists = db.query(SalesRecord).filter(SalesRecord.transaction_id == row['transaction_id']).first()
            if not exists:
                record = SalesRecord(
                    source=row['source'],
                    transaction_id=row['transaction_id'],
                    product=row['product'],
                    amount=row['amount'],
                    timestamp=row['timestamp'],
                    is_anomaly=row['is_anomaly']
                )
                db.add(record)
                count += 1
        
        db.commit()
        print(f"--- [LOAD] Successfully loaded {count} new records. ---")
    except Exception as e:
        print(f"Error loading data: {e}")
        db.rollback()
    finally:
        db.close()

def run_pipeline():
    """Orchestrates the ETL process."""
    init_db() # Ensure DB tables exist
    raw_data = extract()
    clean_data = transform(raw_data)
    load(clean_data)