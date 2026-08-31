import os
from pathlib import Path
from dotenv import load_dotenv

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

# Load .env
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

# SWARM CREDENTIAL FIX: matches the _FILE convention postgres-init.sh already
# established for the postgres service itself. If POSTGRES_PASSWORD_FILE is
# set and points at a mounted Swarm secret (/run/secrets/postgres_password),
# read the password from there instead of requiring it as a plaintext env
# var. Falls back to POSTGRES_PASSWORD directly if no _FILE variant is set,
# so this is a no-op change for the single-host docker-compose.yml, which
# doesn't use secrets and continues to work unmodified.
_pg_password_file = os.getenv("POSTGRES_PASSWORD_FILE")
if _pg_password_file and Path(_pg_password_file).is_file():
    _pg_password = Path(_pg_password_file).read_text().strip()
else:
    _pg_password = os.getenv("POSTGRES_PASSWORD", "SecureBankPassword2026!")

# Construct default connection string using postgresql+asyncpg:// driver
DEFAULT_DB_URL = (
    f"postgresql+asyncpg://{os.getenv('POSTGRES_USER', 'postgres_admin')}:"
    f"{_pg_password}@"
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