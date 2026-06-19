from fastapi import FastAPI, Request
from app.core.database import engine, Base, SessionLocal
from app.models.domain import Account, Customer
from app.api import auth, accounts, transfers, employee, security, users, cards  # <-- ADDED cards HERE
import decimal
import time
import json
import uuid
import random
from datetime import datetime
from confluent_kafka import Producer

# Trigger DDL Generation
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Real-Time Core Banking Engine",
    description="Backend API Gateway processing baseline financial transactions.",
    version="1.0.0"
)

# --- START KAFKA MIDDLEWARE ---
producer_config = {'bootstrap.servers': 'localhost:9092'}
kafka_producer = Producer(producer_config)

@app.middleware("http")
async def log_api_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    
    forwarded_for = request.headers.get("X-Forwarded-For")
    client_ip = forwarded_for.split(",")[0].strip() if forwarded_for else (request.client.host if request.client else "127.0.0.1")
    
    log_payload = {
        "request_id": str(uuid.uuid4()),
        "user_id": request.headers.get("X-User-Id", "ANONYMOUS"),
        "endpoint": str(request.url.path),
        "method": str(request.method),
        "status_code": response.status_code,
        "latency_ms": round((time.time() - start_time) * 1000, 2),
        "timestamp": datetime.utcnow().isoformat(),
        "ip_address": client_ip,
        "proxy_chain": forwarded_for, 
        "device_id": request.headers.get("X-Device-Id") or request.headers.get("X-Device-ID"),
        "browser_fingerprint": request.headers.get("X-Browser-Fingerprint"),
        "employee_id": request.headers.get("X-Employee-Id"),
        "employee_role": request.headers.get("X-Employee-Role"),
        "user_agent": request.headers.get("user-agent", "Unknown")
    }
    
    try:
        kafka_producer.produce("api_requests", key=log_payload["request_id"], value=json.dumps(log_payload).encode('utf-8'))
        kafka_producer.poll(0)
    except Exception:
        pass 
        
    return response
# --- END KAFKA MIDDLEWARE ---


@app.on_event("startup")
def seed_development_data():
    db = SessionLocal()
    try:
        # Check specifically for simulator-generated customers
        simulator_cust_count = db.query(Customer).filter(Customer.customer_id.like("cust_gen_%")).count()
        if simulator_cust_count == 0:
            print("[Startup] Seeding generated users into database...")
            
            # Generate the 20 structural simulator profiles
            for i in range(20):
                cust_id = f"cust_gen_{i}"
                acc_id = f"ACC-GEN{i:05d}"
                
                new_cust = Customer(
                    customer_id=cust_id,
                    first_name=f"UserFirst_{i}",
                    last_name=f"UserLast_{i}",
                    email=f"{cust_id}@simulation.io",
                    country="US" if i % 3 != 0 else "CA",
                    risk_level="LOW"
                )
                db.add(new_cust)
                db.flush() # Stage changes to allow foreign key bindings immediately
                
                new_acc = Account(
                    account_id=acc_id,
                    customer_id=cust_id,
                    balance=decimal.Decimal(f"{random.uniform(5000.00, 65000.00):.2f}"),
                    currency="USD",
                    status="ACTIVE"
                )
                db.add(new_acc)
            
            # Seed Executive VIP targets for high-value scenarios
            vips = [
                {"id": "VIP-CEO-ACCOUNT-001", "first": "Executive", "last": "CEO"},
                {"id": "VIP-CFO-ACCOUNT-002", "first": "Executive", "last": "CFO"}
            ]
            for vip in vips:
                new_vip = Customer(
                    customer_id=vip["id"],
                    first_name=vip["first"],
                    last_name=vip["last"],
                    email=f"{vip['first'].lower()}@enterprise-vault.com",
                    country="US",
                    risk_level="HIGH"
                )
                db.add(new_vip)
                db.flush()
                
                new_vip_acc = Account(
                    account_id=vip["id"].replace("ACCOUNT", "ACC"),
                    customer_id=vip["id"],
                    balance=decimal.Decimal("2500000.00"),
                    currency="USD",
                    status="ACTIVE"
                )
                db.add(new_vip_acc)

            db.commit()
            print("[Startup] Seeding routine completed cleanly.")
    except Exception as e:
        print(f"[Startup Fail] Seeding crashed: {e}")
        db.rollback()
    finally:
        db.close()

app.include_router(auth.router)
app.include_router(accounts.router)
app.include_router(transfers.router)
app.include_router(employee.router)
app.include_router(security.router)
app.include_router(users.router)  
app.include_router(cards.router)  # <-- ADDED cards router HERE

@app.get("/")
def read_root():
    return {"status": "ONLINE", "system": "Banking Core"}