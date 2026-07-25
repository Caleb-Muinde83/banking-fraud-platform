import os
from pathlib import Path
from dotenv import load_dotenv

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

# Load .env
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

# Construct default connection string using postgresql+asyncpg:// driver
DEFAULT_DB_URL = (
    f"postgresql+asyncpg://{os.getenv('POSTGRES_USER', 'postgres_admin')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'SecureBankPassword2026!')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5433')}/"
    f"{os.getenv('POSTGRES_DB', 'banking_db')}"
)

DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

# Force driver conversion to asyncpg if DATABASE_URL was set to psycopg2 or standard postgresql
if "+psycopg2" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("+psycopg2", "+asyncpg")
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Initialize Asynchronous Engine
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True
)

# Create Async Session Factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

Base = declarative_base()


async def get_db():
    """Async dependency generator for FastAPI routes."""
    async with AsyncSessionLocal() as session:
        yield session