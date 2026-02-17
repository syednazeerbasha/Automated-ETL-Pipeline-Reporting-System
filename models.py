from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import DB_URL
import datetime

Base = declarative_base()
engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class SalesRecord(Base):
    __tablename__ = "sales_records"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String)  # CSV, JSON, or API
    transaction_id = Column(String, unique=True, index=True)
    product = Column(String)
    amount = Column(Float)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    is_anomaly = Column(Boolean, default=False)  # Transformation logic flag

def init_db():
    Base.metadata.create_all(bind=engine)