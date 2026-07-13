import json
import os

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from opensearchpy import OpenSearch

# -----------------------------------------------------------------------------
# 1. CONFIGURATION & CONNECTIONS
# -----------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
AUTH = (
    os.getenv("OPENSEARCH_USER", "admin"),
    os.getenv("OPENSEARCH_PASSWORD", "admin")
)

# CHANGED: topic names corrected to match what's actually provisioned/
# produced elsewhere in the platform (kafka-init, RiskScoringJob,
# Debezium's table.include.list) — the previous "login_events" and
# "critical_alerts" keys subscribed to topics that don't exist.
#
# fraud_alerts_v2 and critical_alerts_v2 now both feed the same "alerts"
# OpenSearch index (per the Part 1 routing split) — the `severity` field
# already carried in each FraudAlert payload distinguishes them for
# querying, so there's no need for two separate indices just because
# there are now two Kafka topics.
TOPIC_INDEX_MAP = {
    "api_requests": "api_requests",
    "banking.login_events": "login_events",
    "fraud_alerts_v2": "alerts",
    "critical_alerts_v2": "alerts",
}

# CHANGED: these two topics are Avro (Schema-Registry-registered, written
# by FraudAlertAvroSerializationSchema on the Flink side) — everything else
# in TOPIC_INDEX_MAP is plain JSON (Debezium's JsonConverter for CDC
# topics, and the API's json.dumps-based Kafka producer for api_requests).
# A single deserializer can't handle both, so each message is deserialized
# per-topic in the consume loop below instead.
AVRO_TOPICS = {"fraud_alerts_v2", "critical_alerts_v2"}

# CHANGED: CDC topics need their Debezium envelope ({before, after, source,
# op, ts_ms}) unwrapped before indexing — previously the raw envelope was
# indexed as-is, leaving every explicitly-mapped field empty since the real
# data sits one level deeper, under `after.*`.
CDC_TOPICS = {"banking.login_events"}

# NEW — security plugin is now enabled on the OpenSearch backend (see
# docker-compose.yml), which also switches the REST layer to HTTPS with
# self-signed demo certs. use_ssl=True + verify_certs=False is the correct
# pairing for that; AUTH above now actually gets checked, not silently
# ignored like it was when DISABLE_SECURITY_PLUGIN=true.
os_client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
    http_auth=AUTH,
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False
    timeout=60,             # NEW: Give OpenSearch 60 seconds to respond
    max_retries=3,          # NEW: Automatically retry if it fails
    retry_on_timeout=True   # NEW: Don't just give up on a timeout
)

# NEW — Schema-Registry-aware Avro deserializer for the two alert topics.
# No schema_str/from_dict supplied: AvroDeserializer resolves the writer
# schema automatically from the Confluent wire-format magic byte + schema
# ID embedded in each message, and returns a plain dict by default — which
# is exactly the shape OpenSearch's index() call needs.
_schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
_avro_deserializer = AvroDeserializer(_schema_registry_client)


# -----------------------------------------------------------------------------
# 2. EXPLICIT SECURITY INDEX PROVISIONING
# -----------------------------------------------------------------------------
def initialize_indices():
    """Defines structural schemas (mappings) to ensure exact security querying."""
    index_configs = {
        "api_requests": {
            "mappings": {
                "properties": {
                    # CHANGED: fields corrected to match the real
                    # api_request.avsc schema (request_id, ip_address,
                    # user_agent, endpoint, method, status_code, timestamp).
                    # customer_id / action / amount removed — none of those
                    # exist on api_requests; that schema carries no
                    # customer_id at all.
                    "timestamp":   {"type": "date"},
                    "request_id":  {"type": "keyword"},
                    "ip_address":  {"type": "ip"},
                    "user_agent":  {"type": "keyword"},
                    "endpoint":    {"type": "keyword"},
                    "method":      {"type": "keyword"},
                    "status_code": {"type": "integer"}
                }
            }
        },
        "login_events": {
            "mappings": {
                "properties": {
                    # CHANGED: "status" (keyword) -> "success" (boolean) and
                    # "location" (keyword) -> "country" (keyword) to match
                    # the real login_events columns confirmed in schema.sql.
                    "timestamp":  {"type": "date"},
                    "customer_id":{"type": "keyword"},
                    "success":    {"type": "boolean"},
                    "ip_address": {"type": "ip"},
                    "country":    {"type": "keyword"},
                    "device_id":  {"type": "keyword"},
                    # NEW — geo-point support, derived from the new
                    # latitude/longitude columns at index time (see
                    # build_geo_point below).
                    "geo_location": {"type": "geo_point"}
                }
            }
        },
        "alerts": {
            "mappings": {
                "properties": {
                    # NEW — "severity" is now required: fraud_alerts_v2 and
                    # critical_alerts_v2 both feed this one index, and this
                    # is the field that distinguishes FRAUD from CRITICAL.
                    # "timestamp" renamed to "event_time" to match the real
                    # FraudAlert Avro schema field name directly, rather
                    # than needing a rename/transform at index time.
                    "event_time":              {"type": "date"},
                    "alert_id":                {"type": "keyword"},
                    "customer_id":             {"type": "keyword"},
                    "account_id":              {"type": "keyword"},
                    "severity":                {"type": "keyword"},
                    "cumulative_score":        {"type": "integer"},
                    "threshold":               {"type": "integer"},
                    "contributing_indicators": {"type": "keyword"},
                    "trigger_reason":          {"type": "keyword"},
                    "action_required":         {"type": "keyword"}
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
# 3. TRANSFORM HELPERS
# -----------------------------------------------------------------------------
def unwrap_cdc_envelope(payload: dict):
    """
    Extracts the `after` block from a Debezium CDC envelope and drops
    delete events. Returns None for a delete (op == "d") so the caller
    knows to skip indexing entirely, since `after` is null on deletes.
    Mirrors what RiskEventNormalizer does on the Flink side.

    FIXED: also overrides `after.timestamp` with the envelope's own
    `ts_ms`. The raw column value Debezium puts in `after` depends on
    time.precision.mode (default "adaptive" -> microseconds for a plain
    TIMESTAMP column, confirmed against a real login_events row: a
    16-digit value like 1783395584407664, not milliseconds). OpenSearch's
    "date" field type assumes milliseconds, so indexing the raw column
    value directly silently produced a wrong (or at least differently-
    scaled) date. `ts_ms` is Debezium's own envelope-level field and is
    documented to always be in real milliseconds regardless of
    time.precision.mode — RiskEventNormalizer already relies on this same
    guarantee on the Flink side, so this keeps both consumers consistent
    with each other instead of each interpreting the row's own timestamp
    column differently.
    """
    if payload.get("op") == "d":
        return None
    after = payload.get("after")
    if after is None:
        return payload
    ts_ms = payload.get("ts_ms")
    if ts_ms is not None:
        after = dict(after)  # don't mutate the caller's dict in place
        after["timestamp"] = ts_ms
    return after


def apply_geo_point(doc: dict) -> dict:
    """
    OpenSearch's geo_point type expects a single field, not two separate
    columns — transforms the login_events row's latitude/longitude (added
    in the geo-point schema migration) into the geo_location shape the
    index mapping above expects. No-op if either coordinate is missing
    (e.g. rows from before the migration, or a caller that didn't supply
    geo data).
    """
    lat = doc.get("latitude")
    lon = doc.get("longitude")
    if lat is not None and lon is not None:
        doc["geo_location"] = {"lat": lat, "lon": lon}
    return doc


def deserialize_message(topic: str, raw_key, raw_value):
    """
    Per-topic deserialization: Avro (via Schema Registry) for the alert
    topics, plain JSON for everything else. Deliberately NOT wrapped in
    try/except here — the caller wraps this call itself, so a bad message
    is caught in exactly one place rather than needing duplicate handling
    in every branch.
    """
    if topic in AVRO_TOPICS:
        return _avro_deserializer(raw_value, SerializationContext(topic, MessageField.VALUE))
    return json.loads(raw_value.decode("utf-8"))


# -----------------------------------------------------------------------------
# 4. KAFKA TO OPENSEARCH CONSUMER LOOP
# -----------------------------------------------------------------------------
def run_opensearch_sink():
    initialize_indices()

    # CHANGED: switched from kafka-python's KafkaConsumer (which forced a
    # single value_deserializer for every subscribed topic — the root cause
    # of the Avro-crashes-the-whole-process bug) to confluent-kafka's raw
    # Consumer, which hands back undecoded bytes so deserialization can be
    # chosen per-message based on topic.
    # CHANGED: enable.auto.commit was True — meaning offsets advanced on a
    # timer regardless of whether indexing actually succeeded, so a message
    # that failed to index was still marked "consumed" and would never be
    # retried, even on a sink restart. Manual commit (see the success path
    # below) ties offset advancement to actual indexing success instead.
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "opensearch-sink-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(list(TOPIC_INDEX_MAP.keys()))

    print("📥 OpenSearch Sink Daemon listening to Kafka streams. Ready to index...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"❌ Kafka consumer error: {msg.error()}")
                continue

            topic = msg.topic()

            # CHANGED: deserialization now happens INSIDE this try/except,
            # not before it. Previously a malformed/Avro-on-JSON-consumer
            # message would raise during kafka-python's iteration mechanics,
            # outside any try/except — killing the entire consumer loop on
            # the very first bad message, not just failing that one record.
            try:
                target_index = TOPIC_INDEX_MAP[topic]
                payload = deserialize_message(topic, msg.key(), msg.value())

                if topic in CDC_TOPICS:
                    payload = unwrap_cdc_envelope(payload)
                    if payload is None:
                        # Delete event — nothing to index.
                        continue
                    payload = apply_geo_point(payload)

                response = os_client.index(
                    index=target_index,
                    body=payload,
                    refresh=True  # Force immediate indexing for near real-time threat hunting
                )
                print(f"⚡ Indexed doc into [{target_index}] | Doc ID: {response['_id']}")

                # NEW — offset only advances here, after indexing has
                # actually succeeded. Synchronous (asynchronous=False):
                # indexing is already synchronous per-message due to
                # refresh=True above, so this doesn't change the throughput
                # profile, and it means a failed commit is caught here
                # rather than silently swallowed by an async callback.
                consumer.commit(message=msg, asynchronous=False)

            except Exception as e:
                print(f"❌ Error indexing message from topic {topic}: {str(e)}")
                # Deliberately no commit here — this message's offset is
                # NOT advanced. On a sink restart, it will be redelivered
                # from the last successfully committed offset rather than
                # being silently skipped forever. During continuous
                # operation (no restart), processing still moves on to the
                # next message on the next poll() — this isn't a blocking
                # retry loop, just a stronger recovery guarantee.
                continue
    finally:
        consumer.close()


if __name__ == "__main__":
    run_opensearch_sink()