import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Fallback to local development connection string if environment variable isn't injected yet
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://postgres_admin:SecureBankPassword2026!@localhost:5433/banking_db"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Dependency provider to inject database sessions into FastAPI endpoints safely
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()