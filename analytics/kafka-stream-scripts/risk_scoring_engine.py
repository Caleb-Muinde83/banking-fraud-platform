import os
import time
import json
from collections import defaultdict
from confluent_kafka import Consumer, Producer

# Configuration
KAFKA_BROKER = 'localhost:29092'
ALERT_THRESHOLD = 80
SCORE_DECAY_WINDOW_SEC = 3600  # Scores reset after 1 hour of inactivity

# Risk Weight Definitions
RISK_WEIGHTS = {
    "NEW_DEVICE": 30,
    "COUNTRY_CHANGE": 40,
    "VELOCITY_VIOLATION": 30,
    "LARGE_TRANSFER": 25,
    "FAILED_LOGIN": 15,
    "CEP_ATO_MATCH": 100
}

# State Management: { customer_id: {"score": int, "indicators": list, "last_updated": float} }
risk_state = defaultdict(lambda: {"score": 0, "indicators": [], "last_updated": 0.0})

# Infrastructure Setup
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'risk-scoring-engine-v1',
    'auto.offset.reset': 'latest'
})

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Subscribe to all relevant upstream topics
consumer.subscribe(['api_requests', 'transactions', 'login_events', 'fraud_alerts'])

def process_event(event_type: str, customer_id: str, current_time: float):
    if event_type not in RISK_WEIGHTS:
        return

    profile = risk_state[customer_id]
    
    # Apply time-based decay: reset score if the last activity was outside the decay window
    if current_time - profile["last_updated"] > SCORE_DECAY_WINDOW_SEC:
        profile["score"] = 0
        profile["indicators"] = []

    # Update state
    weight = RISK_WEIGHTS[event_type]
    profile["score"] += weight
    profile["indicators"].append(event_type)
    profile["last_updated"] = current_time

    print(f"📊 [RISK UPDATE] {customer_id} triggered {event_type} (+{weight}). Current Score: {profile['score']}")

    # Check Threshold
    if profile["score"] >= ALERT_THRESHOLD:
        generate_structured_alert(customer_id, profile)
        # Reset score post-alert to prevent alert flooding
        profile["score"] = 0
        profile["indicators"] = []

def generate_structured_alert(customer_id: str, profile: dict):
    alert_payload = {
        "alert_id": f"ALT-{int(time.time())}",
        "customer_id": customer_id,
        "cumulative_score": profile["score"],
        "contributing_indicators": profile["indicators"],
        "timestamp": int(time.time() * 1000),
        "action_required": "FREEZE_ACCOUNT" if profile["score"] >= 100 else "REQUIRE_MFA"
    }
    
    print(f"\n🚨 [CRITICAL ALERT] Threshold Breached for {customer_id}!")
    print(json.dumps(alert_payload, indent=2))
    
    # Publish to downstream high-priority topic
    producer.produce('high_risk_alerts', key=customer_id, value=json.dumps(alert_payload))
    producer.flush()

print("Phase 6: Multi-Indicator Risk Engine Online. Aggregating telemetry...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        # Safely parse JSON (Assuming your upstream tools are outputting standard JSON for this layer)
        try:
            payload = json.loads(msg.value().decode('utf-8'))
        except Exception:
            continue

        # Extract normalized fields (Adapt these keys based on your actual topic schemas)
        cust_id = payload.get("customer_id")
        event_type = payload.get("risk_indicator_type") # e.g., "LARGE_TRANSFER"
        
        if cust_id and event_type:
            process_event(event_type, cust_id, time.time())

except KeyboardInterrupt:
    print("Shutting down Risk Engine...")
finally:
    consumer.close()