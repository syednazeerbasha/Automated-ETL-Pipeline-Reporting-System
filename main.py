import threading
import time
import schedule
import uvicorn
from etl import run_pipeline
from api import app

def run_schedule():
    """Background task to run ETL every minute (for demo purposes)."""
    print("Scheduler started. Running ETL every 1 minute...")
    # In production, this might be .every().day.at("10:30")
    schedule.every(1).minutes.do(run_pipeline)
    
    # Run once immediately on startup
    run_pipeline()

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    # Start the scheduler in a separate thread so it doesn't block the API
    scheduler_thread = threading.Thread(target=run_schedule, daemon=True)
    scheduler_thread.start()

    # Start the FastAPI Server
    print("Starting Web Server on http://127.0.0.1:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000)