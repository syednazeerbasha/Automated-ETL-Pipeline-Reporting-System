# ETL Pipeline with Reporting API

A complete automated data pipeline that extracts sales data from multiple sources (CSV, JSON, and API), transforms and cleans it, loads it into a SQLite database, and provides a REST API for reporting and manual ETL triggers.

---

## Features

- **Multi-Source Extraction**: Reads data from CSV files, JSON files, and simulated API endpoints
- **Data Transformation**: Cleans data, normalizes column names, converts data types, flags anomalies
- **SQLite Loading**: Persists transformed data with duplicate detection
- **Automated Scheduling**: Runs ETL pipeline automatically every minute
- **RESTful API**: FastAPI-based endpoints for health checks, manual triggers, and reporting
- **Anomaly Detection**: Automatically flags high-value transactions (>$10,000)
- **Reporting**: Get summary statistics, anomaly reports, and source trends

---

## Tech Stack

| Technology | Purpose |
|------------|---------|
| Python 3.12 | Primary language |
| Pandas | Data manipulation |
| SQLAlchemy | Database ORM |
| SQLite | Local database storage |
| FastAPI | REST API framework |
| Uvicorn | ASGI web server |
| Schedule | Job scheduling |

---

## Project Structure

```
.
├── main.py                 # Application entry point - starts scheduler and API
├── etl.py                  # Core ETL functions (extract, transform, load)
├── api.py                  # FastAPI REST endpoints
├── models.py               # SQLAlchemy database models
├── config.py               # Configuration settings
├── generate_data.py        # Mock data generation utility
├── requirements.txt        # Python dependencies
├── database.db             # SQLite database
└── data/
    ├── sales_dump.csv      # 1000 sample CSV records
    └── web_transactions.json # 200 sample JSON records
```

---

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Generate Sample Data (Optional)

```bash
python generate_data.py
```

This creates:
- `data/sales_dump.csv` - 1000 sales records
- `data/web_transactions.json` - 200 web transaction records

### 3. Run the Application

```bash
python main.py
```

This starts:
- **ETL Scheduler**: Runs every minute in the background
- **API Server**: Available at `http://127.0.0.1:8000`

---

## API Endpoints

### Health Check
```bash
GET /
```
Returns service status.

### Manual ETL Trigger
```bash
POST /trigger-etl
```
Manually runs the ETL pipeline and returns the number of new records loaded.

### Summary Report
```bash
GET /report/summary
```
Returns:
- Total revenue
- Average order value
- Total transaction count

### Anomaly Report
```bash
GET /report/anomalies
```
Returns all transactions flagged as anomalies (amount > $10,000).

### Source Trends
```bash
GET /report/trends
```
Returns transaction counts grouped by data source (CSV, JSON, API).

---

## ETL Pipeline Details

### Extract
Reads data from three sources:
- **CSV**: `data/sales_dump.csv` - Contains transaction_id, date, product, category, region, amount
- **JSON**: `data/web_transactions.json` - Contains id, timestamp, item, customer_type, price
- **API**: Generates mock API data for demonstration

### Transform
- Normalizes column names across sources
- Converts data types (dates, floats)
- Adds load timestamps
- Flags anomalies (transactions > $10,000)
- Handles missing values

### Load
- Inserts records into SQLite database
- Prevents duplicates using transaction_id
- Uses SQLAlchemy ORM for database operations

---

## Database Schema

**Table: `sales_records`**

| Column | Type | Description |
|--------|------|-------------|
| id | Integer | Primary key |
| source | String | Data source (CSV, JSON, API) |
| transaction_id | String | Unique transaction identifier |
| product | String | Product name |
| amount | Float | Transaction amount |
| timestamp | DateTime | Transaction timestamp |
| is_anomaly | Boolean | True if amount > $10,000 |

---

## Configuration

Edit `config.py` to customize:

```python
DATA_DIR = Path(__file__).parent / "data"    # Data files directory
DB_PATH = Path(__file__).parent / "database.db"  # SQLite database path
```

---

## Usage Examples

### Check API Health
```bash
curl http://127.0.0.1:8000/
```

### Trigger ETL Manually
```bash
curl -X POST http://127.0.0.1:8000/trigger-etl
```

### Get Summary Report
```bash
curl http://127.0.0.1:8000/report/summary
```

### View Anomalies
```bash
curl http://127.0.0.1:8000/report/anomalies
```

---

## Development

### Running ETL Only
```bash
python -c "from etl import run_etl; run_etl()"
```

### Running API Only
```bash
uvicorn api:app --reload
```

### View Database
Use any SQLite client to open `database.db` and query the `sales_records` table.

---

## 📫 Let’s Connect  

If you’re interested in my work or would like to discuss internship opportunities:  

[![LinkedIn](https://img.shields.io/badge/LinkedIn-blue?logo=linkedin)](https://www.linkedin.com/in/syed-nazeer-basha-508311250/)
