import os
import time
from collections import defaultdict
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField

# 1. Infrastructure Setup
schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT_DIR = os.path.dirname(CURRENT_DIR)
SCHEMA_PATH = os.path.join(REPO_ROOT_DIR, "schemas", "api_request.avsc")

with open(SCHEMA_PATH, "r") as f:
    schema_str = f.read()

avro_deserializer = AvroDeserializer(
    schema_registry_client,
    schema_str,
    lambda dict_obj, ctx: dict_obj
)
string_deserializer = StringDeserializer('utf_8')

consumer = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'fraud-phase1-velocity-check-v2', # Bumped group ID to force a fresh read
    'auto.offset.reset': 'earliest'               # <-- CHANGED TO EARLIEST TO READ ALL PAST SIMULATIONS
})
consumer.subscribe(['api_requests'])

ip_activity_state = defaultdict(list)

# Rule Configuration
VELOCITY_THRESHOLD = 3      
TIME_WINDOW_SEC = 10        # <-- RELAXED TO 10 SECONDS TO ACCOUNT FOR NETWORK/HTTP LATENCY
SENSITIVE_ENDPOINTS = ["/transfers", "/login", "/beneficiaries"] # Shortened to catch partial matches

def evaluate_velocity_rule(ip_address: str, endpoint: str, current_time_ms: int):
    # Match if the sensitive word exists anywhere in the endpoint string
    if not any(sensitive in endpoint for sensitive in SENSITIVE_ENDPOINTS):
        return 
    
    current_time_sec = current_time_ms / 1000.0
    timestamps = ip_activity_state[ip_address]
    
    timestamps.append(current_time_sec)
    
    # Clean up old events outside the window
    ip_activity_state[ip_address] = [t for t in timestamps if current_time_sec - t <= TIME_WINDOW_SEC]
    current_count = len(ip_activity_state[ip_address])
    
    print(f"   [RULE CHECK] IP {ip_address} has hit sensitive endpoints {current_count}/{VELOCITY_THRESHOLD} times in last {TIME_WINDOW_SEC}s.")
    
    if current_count >= VELOCITY_THRESHOLD:
        print(f"\n[🚨 FRAUD ALERT] Rapid Sequential Actions detected!")
        print(f"   -> IP: {ip_address} hit {endpoint} {current_count} times inside a {TIME_WINDOW_SEC}s window.\n")

print("Phase 1 Rules Engine started. Listening for telemetry...")
ctx = SerializationContext("api_requests", MessageField.VALUE)

# 3. Continuous Stream Processing Loop
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # --- UPDATED REGION: Handle legacy un-serialized data safely ---
        try:
            payload = avro_deserializer(msg.value(), ctx)
        except Exception as serialization_error:
            # Safely skip plain text legacy payloads that lack the Avro magic byte
            print(f"[SKIPPING LEGACY RECORD] Found non-Avro or pre-migration payload: {serialization_error}")
            continue
        # --- END OF UPDATED REGION ---
        
        if payload:
            ip = payload.get("ip_address")
            ep = payload.get("endpoint")
            ts = payload.get("timestamp")
            
            print(f"[DEBUG RECEIVED] Telemetry Event from IP: {ip} | Endpoint: {ep}")
            
            evaluate_velocity_rule(ip, ep, ts)

except KeyboardInterrupt:
    print("Shutting down Rules Engine...")
finally:
    consumer.close()