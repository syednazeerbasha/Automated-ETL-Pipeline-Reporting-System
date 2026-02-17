from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from models import SessionLocal, SalesRecord
from sqlalchemy import func
import etl

app = FastAPI(title="ETL Reporting API", description="Automated ETL Reports")

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def health_check():
    return {"status": "System Operational"}

@app.post("/trigger-etl")
def trigger_etl_job():
    """Manually trigger the ETL pipeline."""
    etl.run_pipeline()
    return {"message": "ETL Pipeline executed successfully."}

@app.get("/report/summary")
def get_summary_report(db: Session = Depends(get_db)):
    """Returns aggregated metrics: Total Sales, Average Order Value."""
    total_sales = db.query(func.sum(SalesRecord.amount)).scalar() or 0
    avg_sales = db.query(func.avg(SalesRecord.amount)).scalar() or 0
    total_tx = db.query(func.count(SalesRecord.id)).scalar() or 0

    return {
        "total_revenue": round(total_sales, 2),
        "average_order_value": round(avg_sales, 2),
        "transaction_count": total_tx
    }

@app.get("/report/anomalies")
def get_anomalies(db: Session = Depends(get_db)):
    """Returns a list of high-value transactions flagged as anomalies."""
    anomalies = db.query(SalesRecord).filter(SalesRecord.is_anomaly == True).all()
    return {"anomalies_count": len(anomalies), "records": anomalies}

@app.get("/report/trends")
def get_source_trends(db: Session = Depends(get_db)):
    """Returns sales breakdown by source (CSV vs JSON vs API)."""
    results = db.query(
        SalesRecord.source, func.sum(SalesRecord.amount)
    ).group_by(SalesRecord.source).all()
    
    return {source: round(amount, 2) for source, amount in results}