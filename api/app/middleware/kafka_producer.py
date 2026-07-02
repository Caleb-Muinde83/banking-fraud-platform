import os
import time
import json
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# 1. Infrastructure Initialization
schema_registry_conf = {'url': os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

kafka_conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
    'client.id': 'bank-telemetry-producer'
}
producer = Producer(kafka_conf)

# ---------------------------------------------------------
# Dynamic Path Resolution (Docker & Local Environment Safe)
# ---------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def find_schema_path():
    """Walks up the directory tree to find the schema file regardless of environment"""
    # 1. Check if it was explicitly passed via environment variables
    env_path = os.getenv("API_REQUEST_SCHEMA_PATH")
    if env_path and os.path.exists(env_path):
        return env_path
        
    search_dir = CURRENT_DIR
    # 2. Walk up the directory tree (max 4 levels) to find the schemas folder
    for _ in range(4):
        # Look in the config/schemas directory (Docker target)
        path_in_config = os.path.join(search_dir, "config", "schemas", "api_request.avsc")
        if os.path.exists(path_in_config):
            return path_in_config
            
        # Look in the root schemas directory (Common local dev target)
        path_in_root = os.path.join(search_dir, "schemas", "api_request.avsc")
        if os.path.exists(path_in_root):
            return path_in_root
            
        # Go up one directory level
        search_dir = os.path.dirname(search_dir)
        
    raise FileNotFoundError(f"Could not locate 'api_request.avsc' anywhere near {CURRENT_DIR}")

SCHEMA_PATH = find_schema_path()
print(f"✅ Successfully located Avro Schema at: {SCHEMA_PATH}")

# 2. Load and Register Avro Schema safely
with open(SCHEMA_PATH, "r") as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    lambda obj, ctx: obj 
)
string_serializer = StringSerializer('utf_8')

def send_telemetry_event(payload: dict):
    """
    Attempts to serialize and produce an event under strict Avro contract compliance.
    Fails safely to the Dead Letter Queue (DLQ) if structural validation falls apart.
    """
    primary_topic = "api_requests"
    dlq_topic = "dead_letter_queue"
    
    # Define a proper serialization context telling the engine this schema applies to the Message VALUE
    ctx = SerializationContext(primary_topic, MessageField.VALUE)
    
    try:
        if "timestamp" not in payload:
            payload["timestamp"] = int(time.time() * 1000)
            
        producer.produce(
            topic=primary_topic,
            key=string_serializer(payload["request_id"]),
            value=avro_serializer(payload, ctx),
            callback=delivery_report
        )
        producer.flush()
        
    except Exception as validation_error:
        print(f"[VALIDATION FAILURE] Poison pill detected. Routing to DLQ: {validation_error}")
        
        dlq_payload = {
            "error_message": str(validation_error),
            "failed_at": int(time.time() * 1000),
            "raw_payload": json.dumps(payload)
        }
        
        producer.produce(
            topic=dlq_topic,
            key=string_serializer(payload.get("request_id", "UNKNOWN")),
            value=string_serializer(json.dumps(dlq_payload)),
            callback=delivery_report
        )
        producer.flush()

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Successfully routed to {msg.topic()} [{msg.partition()}]")