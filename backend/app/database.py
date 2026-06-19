import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Uses the internal Docker container port 5432 or localhost fallback
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres_admin:SecureBankPassword2026!@localhost:5433/banking_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()