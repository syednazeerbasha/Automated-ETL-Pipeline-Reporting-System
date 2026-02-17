import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_NAME = "database.db"
DB_PATH = os.path.join(BASE_DIR, DB_NAME)
DB_URL = f"sqlite:///{DB_PATH}"

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)