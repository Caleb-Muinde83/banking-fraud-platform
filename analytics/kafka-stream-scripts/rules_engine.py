import os
import time
import json
from collections import defaultdict
from confluent_kafka import Consumer, Producer
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

# Set up both Consumer (Input) and Producer (Output to Risk Engine)
KAFKA_BROKER = 'localhost:29092'
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'fraud-phase1-velocity-check-v2', 
    'auto.offset.reset': 'earliest'               
})
consumer.subscribe(['api_requests'])

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

ip_activity_state = defaultdict(list)
alert_cooldown_state = {}  # NEW: Tracks when an IP was last alerted
ALERT_COOLDOWN_SEC = 30    # NEW: Keep them in timeout for 30 seconds

# Rule Configuration
VELOCITY_THRESHOLD = 3      
TIME_WINDOW_SEC = 10        
SENSITIVE_ENDPOINTS = ["/transfers", "/login", "/beneficiaries"]

def evaluate_velocity_rule(ip_address: str, endpoint: str, current_time_ms: int):
    if not any(sensitive in endpoint for sensitive in SENSITIVE_ENDPOINTS):
        return 
    
    current_time_sec = current_time_ms / 1000.0

    # =========================================================================
    # 🛡️ THE PENALTY BOX CHECK: Stop the Alert Storm!
    # =========================================================================
    if ip_address in alert_cooldown_state:
        if current_time_sec - alert_cooldown_state[ip_address] < ALERT_COOLDOWN_SEC:
            # The IP is still in timeout. Skip processing to prevent alert spam.
            return
    # =========================================================================

    timestamps = ip_activity_state[ip_address]
    timestamps.append(current_time_sec)
    
    # Clean up old events outside the window
    ip_activity_state[ip_address] = [t for t in timestamps if current_time_sec - t <= TIME_WINDOW_SEC]
    current_count = len(ip_activity_state[ip_address])
    
    print(f"   [RULE CHECK] IP {ip_address} has hit sensitive endpoints {current_count}/{VELOCITY_THRESHOLD} times in last {TIME_WINDOW_SEC}s.")
    
    if current_count >= VELOCITY_THRESHOLD:
        print(f"\n[🚨 FRAUD ALERT] Rapid Sequential Actions detected! Publishing to Risk Engine...")
        
        # Publish alert to the Risk Engine topic
        alert_payload = {
            "customer_id": ip_address, 
            "risk_indicator_type": "VELOCITY_VIOLATION",
            "endpoint": endpoint,
            "hits": current_count,
            "window_sec": TIME_WINDOW_SEC,
            "timestamp": current_time_ms
        }
        
        producer.produce(
            'fraud_alerts', 
            key=ip_address, 
            value=json.dumps(alert_payload)
        )
        producer.flush()

        # =========================================================================
        # 🛑 DROP THE IP INTO THE PENALTY BOX
        # =========================================================================
        alert_cooldown_state[ip_address] = current_time_sec
        # We also clear their activity state so they start fresh after cooldown
        ip_activity_state[ip_address] = []

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

        try:
            payload = avro_deserializer(msg.value(), ctx)
        except Exception as serialization_error:
            print(f"[SKIPPING LEGACY RECORD] Found non-Avro payload: {serialization_error}")
            continue
        
        if payload:
            ip = payload.get("ip_address")
            ep = payload.get("endpoint")
            ts = payload.get("timestamp")
            
            evaluate_velocity_rule(ip, ep, ts)

except KeyboardInterrupt:
    print("Shutting down Rules Engine...")
finally:
    consumer.close()