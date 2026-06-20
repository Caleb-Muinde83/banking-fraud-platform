import sys
import os
import uuid

# Dynamically resolve paths relative to the script location
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(CURRENT_DIR, "api"))

from app.middleware.kafka_producer import send_telemetry_event

print("--- Testing Valid Avro Event ---")
valid_event = {
    "request_id": str(uuid.uuid4()),
    "ip_address": "192.168.1.50",
    "user_agent": "Mozilla/5.0",
    "endpoint": "/api/v1/transfers",
    "method": "POST",
    "status_code": 200
}
send_telemetry_event(valid_event)

print("\n--- Testing Toxic Poison Pill (Should trigger DLQ routing) ---")
invalid_event = {
    "request_id": str(uuid.uuid4()),
    "ip_address": "10.0.0.99",
    "user_agent": "BadBot/1.0",
    "endpoint": "/api/v1/auth/login",
    "method": "POST",
    "status_code": "STR_POISON_PILL"  # <--- CRITICAL CONTRACT VIOLATION (Should be int)
}
send_telemetry_event(invalid_event)