import json
import os
from kafka import KafkaConsumer
from opensearchpy import OpenSearch

# -----------------------------------------------------------------------------
# 1. CONFIGURATION & CONNECTIONS
# -----------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
AUTH = (
    os.getenv("OPENSEARCH_USER", "admin"),
    os.getenv("OPENSEARCH_PASSWORD", "admin")
)

TOPIC_INDEX_MAP = {
    "api_requests": "api_requests",
    "login_events": "login_events",
    "critical_alerts": "alerts"
}

# Initialize OpenSearch Client
os_client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
    http_auth=AUTH,
    use_ssl=False,  # Set to True if testing an encrypted production cluster
    verify_certs=False,
    ssl_show_warn=False
)

# -----------------------------------------------------------------------------
# 2. EXPLICIT SECURITY INDEX PROVISIONING
# -----------------------------------------------------------------------------
def initialize_indices():
    """Defines structural schemas (mappings) to ensure exact security querying."""
    index_configs = {
        "api_requests": {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "customer_id": {"type": "keyword"},
                    "endpoint": {"type": "keyword"},
                    "action": {"type": "keyword"},
                    "amount": {"type": "float"},
                    "ip_address": {"type": "ip"}
                }
            }
        },
        "login_events": {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "customer_id": {"type": "keyword"},
                    "status": {"type": "keyword"},  # SUCCESS / FAILED
                    "ip_address": {"type": "ip"},
                    "location": {"type": "keyword"},
                    "device_id": {"type": "keyword"}
                }
            }
        },
        "alerts": {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "alert_id": {"type": "keyword"},
                    "customer_id": {"type": "keyword"},
                    "cumulative_score": {"type": "integer"},
                    "contributing_indicators": {"type": "keyword"},
                    "action_required": {"type": "keyword"}
                }
            }
        }
    }

    for name, config in index_configs.items():
        if not os_client.indices.exists(index=name):
            os_client.indices.create(index=name, body=config)
            print(f"📁 Created structured OpenSearch index: [{name}]")
        else:
            print(f"✅ OpenSearch Index [{name}] already exists.")


# -----------------------------------------------------------------------------
# 3. KAFKA TO OPENSEARCH CONSUMER LOOP
# -----------------------------------------------------------------------------
def run_opensearch_sink():
    initialize_indices()
    
    # Initialize Kafka consumer reading from all operational tracking topics
    consumer = KafkaConsumer(
        *TOPIC_INDEX_MAP.keys(),
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='opensearch-sink-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("📥 OpenSearch Sink Daemon listening to Kafka streams. Ready to index...")

    for message in consumer:
        try:
            topic = message.topic
            payload = message.value
            target_index = TOPIC_INDEX_MAP[topic]

            # Ingest payload as a document inside OpenSearch
            response = os_client.index(
                index=target_index,
                body=payload,
                refresh=True  # Force immediate indexing for near real-time threat hunting
            )
            print(f"⚡ Indexed doc into [{target_index}] | Doc ID: {response['_id']}")

        except Exception as e:
            print(f"❌ Error indexing message from topic {message.topic}: {str(e)}")


if __name__ == "__main__":
    run_opensearch_sink()
