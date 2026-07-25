import os
import sys
import socket
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Ensure the 'api/' directory is added to sys.path so 'app' can be imported
API_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API_DIR))

# Load .env variables from repository root
ROOT_DIR = API_DIR.parent
from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")

# Import Base from database configuration
from app.database import Base

# Import model definitions so Base.metadata is fully aware of all tables
try:
    from app.models.incidents import Incident, IncidentAlert, IncidentAuditLog
except ImportError:
    pass

# This is the Alembic Config object
config = context.config

# Interpret the config file for Python logging
if config.config_file_name:
    fileConfig(config.config_file_name)

# Set target metadata for Alembic
target_metadata = Base.metadata


def get_sync_db_url() -> str:
    """
    Constructs a synchronous DB connection string.
    Automatically rewrites Docker internal hostnames (bank_postgres:5432) 
    to local host bindings (localhost:5433) when executed outside Docker.
    """
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://postgres_admin:SecureBankPassword2026!@localhost:5433/banking_db"
    )

    # Check if running outside Docker (if host 'bank_postgres' is unresolvable)
    try:
        socket.gethostbyname("bank_postgres")
    except socket.gaierror:
        # Running on local host -> replace Docker host/port with localhost:5433
        db_url = db_url.replace("bank_postgres:5432", "localhost:5433").replace("bank_postgres", "localhost")

    # Convert asyncpg driver to psycopg2 for Alembic synchronous execution
    if "asyncpg" in db_url:
        db_url = db_url.replace("postgresql+asyncpg", "postgresql+psycopg2")

    return db_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = get_sync_db_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    db_url = get_sync_db_url()

    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = db_url

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, 
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()