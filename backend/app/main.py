import time
import json
import uuid
from datetime import datetime
from fastapi import FastAPI, Request
from confluent_kafka import Producer

# Import routers from your sub-directory structure
from app.routers import core_banking, security, auth

app = FastAPI(title="Real-Time Banking Fraud & Security API Platform")

# Setup Kafka Producer configuration for request auditing
producer_config = {'bootstrap.servers': 'kafka:9092'} # Point to internal docker name or localhost depending on network
kafka_producer = Producer(producer_config)

# Global Audit Middleware: Section 6 Event Capture Requirement
@app.middleware("http")
async def log_api_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    
    log_payload = {
        "request_id": str(uuid.uuid4()),
        "user_id": request.headers.get("X-User-Id", "ANONYMOUS"),
        "ip_address": request.client.host if request.client else "127.0.0.1",
        "user_agent": request.headers.get("user-agent", "Unknown"),
        "endpoint": str(request.url.path),
        "method": str(request.method),
        "status_code": response.status_code,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    try:
        kafka_producer.produce(
            "api_requests", 
            key=log_payload["request_id"], 
            value=json.dumps(log_payload).encode('utf-8')
        )
        kafka_producer.poll(0)
    except Exception as e:
        print(f"[Kafka Logging Error] Failed to stream API audit log: {e}")
        
    return response

# Register routers with their respective file endpoints
app.include_router(core_banking.router)
app.include_router(security.router)
# app.include_router(auth.router) # Uncomment when auth.py has endpoints written