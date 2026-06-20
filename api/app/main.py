import os
import time
import json
import uuid
import random
import decimal
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Request, Depends, HTTPException, Header
from pydantic import BaseModel

# Async Kafka Components
from aiokafka import AIOKafkaProducer
from app.middleware.kafka_logger import KafkaRequestLoggerMiddleware
import app.middleware.kafka_logger as kafka_logger

# SQLAlchemy Async Components
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert

# Import core structural routing endpoints and domains
from app.api import auth, accounts, transfers, employee, security, users, cards
import app.models.domain as models

# =========================================================================
# ASYNC DATABASE ENGINE & CONFIGURATION
# =========================================================================
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/fraud_db")

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def get_db():
    """Dependency injection yield loop providing async session execution."""
    async with AsyncSessionLocal() as session:
        yield session

# =========================================================================
# FASTAPI INITIALIZATION
# =========================================================================
app = FastAPI(
    title="Real-Time Core Banking & Fraud Platform Gateway",
    description="Backend Production API Gateway processing transactions with live streaming telemetry.",
    version="1.0.0"
)

# =========================================================================
# KAFKA EVENT-DRIVEN MIDDLEWARE REGISTRATION
# =========================================================================
app.add_middleware(KafkaRequestLoggerMiddleware)

@app.on_event("startup")
async def startup_kafka_producer():
    kafka_logger.kafka_producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await kafka_logger.kafka_producer.start()
    print("Kafka Producer connected successfully.")

@app.on_event("shutdown")
async def shutdown_kafka_producer():
    if kafka_logger.kafka_producer:
        await kafka_logger.kafka_producer.stop()
        print("Kafka Producer shut down cleanly.")

# =========================================================================
# SYSTEM DATA SEEDING ROUTINE (COMPATIBLE WITH ASYNCPG)
# =========================================================================
@app.on_event("startup")
async def startup_pipeline_provisioning():
    # Trigger DDL Generation on the target cluster asynchronously
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)

    # Core system demographic seeding engine
    async with AsyncSessionLocal() as db:
        try:
            # 1. ALWAYS ensure system fallback entities exist (Runs outside the count check)
            fallbacks = [
                {"id": "cust_external_mule", "first": "External", "last": "Mule", "risk": "HIGH", "country": "UA"},
                {"id": "cust_unknown_fallback", "first": "Unknown", "last": "Fallback", "risk": "LOW", "country": "US"},
                {"id": "SYSTEM_TELLER", "first": "System", "last": "Teller", "risk": "LOW", "country": "US"}
            ]
            for fb in fallbacks:
                fb_check = await db.execute(select(models.Customer).where(models.Customer.customer_id == fb["id"]))
                if not fb_check.scalar_one_or_none():
                    db.add(models.Customer(
                        customer_id=fb["id"], first_name=fb["first"], last_name=fb["last"],
                        email=f"{fb['id'].lower()}@simulation.net", country=fb["country"], risk_level=fb["risk"]
                    ))
            await db.flush()

            # 2. Seed main simulation demographic blocks if missing
            stmt = select(func.count(models.Customer.customer_id)).where(models.Customer.customer_id.like("cust_gen_%"))
            res = await db.execute(stmt)
            simulator_cust_count = res.scalar() or 0
            
            if simulator_cust_count == 0:
                print("[Startup] Seeding baseline actor profiles into database layout...")
                
                # Generate structural target clusters
                for i in range(20):
                    cust_id = f"cust_gen_{i}"
                    acc_id = f"ACC-GEN{i:05d}"
                    
                    new_cust = models.Customer(
                        customer_id=cust_id, first_name=f"UserFirst_{i}", last_name=f"UserLast_{i}",
                        email=f"{cust_id}@simulation.io", country="US" if i % 3 != 0 else "CA", risk_level="LOW"
                    )
                    db.add(new_cust)
                    await db.flush()
                    
                    new_acc = models.Account(
                        account_id=acc_id, customer_id=cust_id,
                        balance=float(round(random.uniform(5000.00, 65000.00), 2)), currency="USD", status="ACTIVE"
                    )
                    db.add(new_acc)
                
                # Seed Executive Target Vectors for targeted security exploits
                vips = [
                    {"id": "VIP-CEO-ACCOUNT-001", "first": "Executive", "last": "CEO"},
                    {"id": "VIP-CFO-ACCOUNT-002", "first": "Executive", "last": "CFO"}
                ]
                for vip in vips:
                    new_vip = models.Customer(
                        customer_id=vip["id"], first_name=vip["first"], last_name=vip["last"],
                        email=f"{vip['first'].lower()}@enterprise-vault.com", country="US", risk_level="HIGH"
                    )
                    db.add(new_vip)
                    await db.flush()
                    
                    new_vip_acc = models.Account(
                        account_id=vip["id"].replace("ACCOUNT", "ACC"), customer_id=vip["id"],
                        balance=2500000.00, currency="USD", status="ACTIVE"
                    )
                    db.add(new_vip_acc)

            await db.commit()
            print("[Startup] Seeding routine completed cleanly.")
        except Exception as e:
            print(f"[Startup Fail] Data bootstrapping routine crashed: {e}")
            await db.rollback()

# =========================================================================
# PYDANTIC VALIDATION INBOUND SCHEMAS
# =========================================================================
class LoginPayload(BaseModel):
    username: str
    password: str
    device_id: Optional[str] = "UNKNOWN_DEV"
    device_type: Optional[str] = "DESKTOP"
    ip_address: Optional[str] = "127.0.0.1"
    country: Optional[str] = "US"

class TransferPayload(BaseModel):
    from_account: str
    to_account: str
    amount: float
    currency: Optional[str] = "USD"
    transaction_type: Optional[str] = "TRANSFER"

class BeneficiaryPayload(BaseModel):
    account_number: str
    bank_name: str

class EmployeeActionPayload(BaseModel):
    customer_id: str

class ProfileUpdatePayload(BaseModel):
    customer_id: str
    phone: str
    email: str

class PasswordResetPayload(BaseModel):
    customer_id: str

class MfaRequestPayload(BaseModel):
    username: str

# =========================================================================
# LIVE CORE API PERMANENT PERSISTENCE ROUTING GATEWAYS
# =========================================================================

@app.post("/api/login")
async def api_login(payload: LoginPayload, db: AsyncSession = Depends(get_db)):
    cust_stmt = select(models.Customer).where(models.Customer.customer_id == payload.username)
    res = await db.execute(cust_stmt)
    customer = res.scalar_one_or_none()
    
    # Auto-provision customer profile context metrics dynamically if non-existent
    if not customer:
        customer = models.Customer(customer_id=payload.username, risk_level="LOW", country=payload.country)
        db.add(customer)
        await db.flush()
        
        acc_id = f"ACC-GEN{int(uuid.uuid4().int % 100000):05d}"
        new_account = models.Account(account_id=acc_id, customer_id=payload.username, balance=7500.00)
        db.add(new_account)
        await db.flush()

    is_success = "WrongPasswordAttempt" not in payload.password
    ev_id = str(uuid.uuid4())
    
    login_ev = models.LoginEvent(
        event_id=ev_id, customer_id=payload.username, device_id=payload.device_id,
        country=payload.country, ip_address=payload.ip_address, success=is_success,
        timestamp=datetime.utcnow()
    )
    db.add(login_ev)

    # ---> CONCURRENCY FIX: Safe PostgreSQL Upsert (Insert or Ignore) <---
    stmt = insert(models.Device).values(
        device_id=payload.device_id,
        customer_id=payload.username,
        device_type=payload.device_type,
        first_seen=datetime.utcnow()
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=['device_id'])
    await db.execute(stmt)

    session_id = str(uuid.uuid4())
    new_session = models.Session(
        session_id=session_id, customer_id=payload.username, device_id=payload.device_id,
        ip_address=payload.ip_address, created_at=datetime.utcnow(), expires_at=datetime.utcnow() + timedelta(hours=2)
    )
    db.add(new_session)
    await db.commit()

    if not is_success:
        raise HTTPException(status_code=401, detail="Authentication identity handshake rejected.")
        
    return {"status": "AUTHENTICATED", "session_id": session_id}

@app.get("/api/accounts/{id}")
async def get_account_details(id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(models.Account).where(models.Account.account_id == id)
    res = await db.execute(stmt)
    account = res.scalar_one_or_none()
    if not account:
        raise HTTPException(status_code=404, detail="Target ledger profile reference missing.")
    return {"account_id": account.account_id, "customer_id": account.customer_id, "balance": account.balance, "currency": account.currency}

@app.get("/api/accounts/{id}/balance")
async def get_account_balance(id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(models.Account).where(models.Account.account_id == id)
    res = await db.execute(stmt)
    account = res.scalar_one_or_none()
    if not account:
        # Prevent FK failures by ensuring parent customer exists before the account
        cust_check = await db.execute(select(models.Customer).where(models.Customer.customer_id == "cust_unknown_fallback"))
        if not cust_check.scalar_one_or_none():
            db.add(models.Customer(customer_id="cust_unknown_fallback", risk_level="LOW", country="US"))
            await db.flush()

        account = models.Account(account_id=id, customer_id="cust_unknown_fallback", balance=5000.00, status="ACTIVE")
        db.add(account)
        await db.commit()
    return {"account_id": account.account_id, "balance": account.balance}

@app.post("/api/transfers")
async def execute_transfer(payload: TransferPayload, x_user_id: Optional[str] = Header(None), db: AsyncSession = Depends(get_db)):
    for acc_id in [payload.from_account, payload.to_account]:
        stmt = select(models.Account).where(models.Account.account_id == acc_id)
        res = await db.execute(stmt)
        if not res.scalar_one_or_none():
            fallback_cust = x_user_id if acc_id == payload.from_account else "cust_external_mule"
            
            # ---> CRITICAL FIX: Ensure dynamic customer parent exists BEFORE account creation <---
            cust_check = await db.execute(select(models.Customer).where(models.Customer.customer_id == fallback_cust))
            if not cust_check.scalar_one_or_none():
                db.add(models.Customer(customer_id=fallback_cust, risk_level="HIGH", country="UNKNOWN"))
                await db.flush()

            new_acc = models.Account(account_id=acc_id, customer_id=fallback_cust, balance=15000.00, status="ACTIVE")
            db.add(new_acc)
    await db.flush()

    from_res = await db.execute(select(models.Account).where(models.Account.account_id == payload.from_account))
    to_res = await db.execute(select(models.Account).where(models.Account.account_id == payload.to_account))
    
    acc_from = from_res.scalar_one()
    acc_to = to_res.scalar_one()

    # ---> MATH FIX: Use standard floats to match the SQLAlchemy Float Column type <---
    amount_float = float(payload.amount)
    
    acc_from.balance = float(acc_from.balance) - amount_float
    acc_to.balance = float(acc_to.balance) + amount_float

    tx_id = str(uuid.uuid4())
    tx_record = models.Transaction(
        transaction_id=tx_id, from_account=payload.from_account, to_account=payload.to_account,
        amount=payload.amount, currency=payload.currency, transaction_type=payload.transaction_type,
        timestamp=datetime.utcnow()
    )
    db.add(tx_record)
    await db.commit()
    return {"status": "PROCESSED", "transaction_id": tx_id}

@app.post("/api/beneficiaries")
async def add_beneficiary(payload: BeneficiaryPayload, x_user_id: Optional[str] = Header(None), db: AsyncSession = Depends(get_db)):
    b_id = str(uuid.uuid4())
    
    # Secure parent customer check
    req_user = x_user_id or "UNKNOWN"
    cust_check = await db.execute(select(models.Customer).where(models.Customer.customer_id == req_user))
    if not cust_check.scalar_one_or_none():
        db.add(models.Customer(customer_id=req_user, risk_level="LOW", country="UNKNOWN"))
        await db.flush()

    beneficiary = models.Beneficiary(
        beneficiary_id=b_id, customer_id=req_user,
        account_number=payload.account_number, bank_name=payload.bank_name,
        created_at=datetime.utcnow()
    )
    db.add(beneficiary)
    await db.commit()
    return {"status": "ADDED", "beneficiary_id": b_id}

@app.post("/api/employee/view-account")
async def employee_view_account(payload: EmployeeActionPayload, x_employee_id: Optional[str] = Header(None), db: AsyncSession = Depends(get_db)):
    req_emp = x_employee_id or "SYSTEM_TELLER"
    
    # ---> CRITICAL FIX: Ensure dynamic employee parent exists BEFORE logging action <---
    emp_check = await db.execute(select(models.Employee).where(models.Employee.employee_id == req_emp))
    if not emp_check.scalar_one_or_none():
        db.add(models.Employee(
            employee_id=req_emp, 
            department="Retail Banking", 
            role="Teller"
        ))
        await db.flush()

    action_id = str(uuid.uuid4())
    record = models.EmployeeAction(
        action_id=action_id, employee_id=req_emp,
        customer_id=payload.customer_id, action_type="VIEW_ACCOUNT",
        timestamp=datetime.utcnow()
    )
    db.add(record)
    await db.commit()
    return {"status": "AUDITED", "action_id": action_id}
# =========================================================================
# ENHANCED ATTACK AND INTERACTION EMULATION INTERFACES
# =========================================================================
@app.post("/api/mfa/request")
async def request_mfa(payload: MfaRequestPayload):
    return {"status": "MFA_CHALLENGE_SENT", "username": payload.username}

@app.post("/api/user/update-profile")
async def update_profile(payload: ProfileUpdatePayload):
    return {"status": "PROFILE_MUTATED", "customer_id": payload.customer_id}

@app.post("/api/auth/reset-password")
async def reset_password(payload: PasswordResetPayload):
    return {"status": "PASSWORD_RESET_TOKEN_ISSUED", "customer_id": payload.customer_id}

@app.delete("/api/sessions/{session_id}")
async def drop_session(session_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(models.Session).where(models.Session.session_id == session_id)
    res = await db.execute(stmt)
    session_obj = res.scalar_one_or_none()
    if session_obj:
        await db.delete(session_obj)
        await db.commit()
    return {"status": "DELETED", "session_id": session_id}

# =========================================================================
# MODULAR SYSTEM SUBSYSTEM ROUTER REGISTRATIONS
# =========================================================================
app.include_router(auth.router)
app.include_router(accounts.router)
app.include_router(transfers.router)
app.include_router(employee.router)
app.include_router(security.router)
app.include_router(users.router)  
app.include_router(cards.router)

@app.get("/")
def read_root():
    return {"status": "ONLINE", "system": "Real-Time Core Banking Engine"}