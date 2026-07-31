"""
secondary_risk_engine.py
=========================

ACTIVE-ACTIVE REDUNDANCY ENGINE for the Java/Flink risk engine (RiskScoringJob).

WHY THIS EXISTS
----------------
Prior to this file, the platform had exactly one thing capable of turning raw
telemetry into a fraud alert: the Flink job in analytics/flink-risk-engine-java.
That is a single point of failure -- if the Flink job crashes, is mid-restart
(see RestartStrategies.fixedDelayRestart in RiskScoringJob), or is redeployed,
fraud scoring for the whole platform stops silently until it recovers.

This script is a second, independent, non-Flink consumer of the SAME raw
input topics the Flink job reads (api_requests, banking.transactions,
banking.login_events, banking.accounts). It re-implements a deliberately
simplified version of the same indicator taxonomy and scoring model
(RiskScoringProcessor / risk-scoring.properties) using plain in-memory
Python state instead of Flink's RocksDB-backed keyed state.

It is NOT a hot-standby that only activates when Flink is down. It runs
continuously, in parallel, all the time -- "active-active" rather than
"active-passive" -- because a standby that only starts consuming once a
failure is detected would itself have to catch up on backlog before it's
useful, and detecting "is Flink actually down" reliably is harder and slower
than just always having two independent opinions available. The Alert
Aggregator (api/app/workers/alert_aggregator.py) already consumes from BOTH
engines' output topics and de-duplicates/bundles by identity, so running
both engines all the time costs some redundant compute but buys real
survivability: if either engine goes down, incidents keep forming from
the other one's alerts with zero code change and zero manual failover step.

OUTPUT TOPICS
-------------
This intentionally reuses the `fraud_alerts` / `critical_alerts` topics --
provisioned by kafka-init from day one, but never written to by anything in
the original architecture (the Flink engine writes to the `_v2` topics
instead). Repurposing them here means no new topics need to be provisioned:
  - fraud_alerts_v2 / critical_alerts_v2  -> Flink engine (primary)
  - fraud_alerts    / critical_alerts     -> this script  (secondary)

WHAT IS DELIBERATELY SIMPLER THAN THE FLINK ENGINE
----------------------------------------------------
  - State lives in a plain dict, in one process's memory. No checkpointing,
    no exactly-once guarantee, no RocksDB spill-to-disk. State is lost on
    restart. This is an acceptable trade for a secondary/backstop engine:
    it trades Flink's durability guarantees for operational simplicity, and
    a short gap in a backstop engine's own memory is a much smaller risk
    than having no backstop at all.
  - No broadcast-state join subtlety: the account_id -> customer_id map is
    just a dict, rebuilt from banking.accounts CDC events as they arrive,
    with the same "acct:<id>" fallback identity Flink's AccountEnrichmentFunction
    uses when the mapping isn't resolved yet.
  - No async I/O enrichment step (OpenSearchLookupFunction has no analogue
    here) -- this engine only ever looks at what's in its own memory.

Every threshold below is intentionally copied from
analytics/flink-risk-engine-java/src/main/resources/risk-scoring.properties
so that the two engines' alerts are directly comparable in the incident
timeline, not just structurally similar.
"""

import os
import io
import json
import time
import threading
from collections import defaultdict, deque

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField
from prometheus_client import start_http_server, Counter, Gauge

# --------------------------------------------------------------------------
# Configuration -- mirrors RiskScoringConfig.java / risk-scoring.properties
# --------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
GROUP_ID = os.getenv("SECONDARY_ENGINE_GROUP_ID", "fraud-engine-secondary-v1")
METRICS_PORT = int(os.getenv("SECONDARY_ENGINE_METRICS_PORT", "9500"))

OUTPUT_FRAUD_TOPIC = os.getenv("SECONDARY_FRAUD_TOPIC", "fraud_alerts")
OUTPUT_CRITICAL_TOPIC = os.getenv("SECONDARY_CRITICAL_TOPIC", "critical_alerts")
ENGINE_SOURCE = "python-secondary"

ALERT_THRESHOLD = int(os.getenv("ALERT_THRESHOLD", "80"))
CRITICAL_THRESHOLD = int(os.getenv("CRITICAL_THRESHOLD", "100"))
DECAY_SEC = int(os.getenv("DECAY_SEC", str(30 * 60)))          # 30 min, matches DECAY_MS
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", str(15 * 60)))    # 15 min, matches COOLDOWN_MS
IMPOSSIBLE_TRAVEL_WINDOW_SEC = 2 * 60 * 60                      # 2h
FAILED_LOGIN_WINDOW_SEC = 5 * 60
FAILED_LOGIN_THRESHOLD = 3
LARGE_TRANSFER_THRESHOLD = float(os.getenv("LARGE_TRANSFER_THRESHOLD", "10000"))
VELOCITY_WINDOW_SEC = 5 * 60
VELOCITY_COUNT_THRESHOLD = 4
SENSITIVE_ENDPOINTS = ["/transfers", "/login", "/beneficiaries"]
API_RATE_WINDOW_SEC = 60
API_RATE_THRESHOLD = 50
MAX_RETAINED_TIMESTAMPS = 50

RISK_WEIGHTS = {
    "NEW_DEVICE": 30,
    "NEW_COUNTRY": 40,
    "IMPOSSIBLE_TRAVEL": 60,
    "MULTIPLE_FAILED_LOGINS": 35,
    "LARGE_TRANSFER_AMOUNT": 25,
    "VELOCITY_VIOLATION": 30,
    "SUSPICIOUS_API_PATTERN": 20,
}

# --------------------------------------------------------------------------
# Prometheus metrics -- scraped by prometheus.yml's fraud-engine-secondary job
# --------------------------------------------------------------------------
EVENTS_PROCESSED = Counter(
    "secondary_engine_events_processed_total", "Events processed by the secondary risk engine", ["topic"]
)
ALERTS_EMITTED = Counter(
    "secondary_engine_alerts_emitted_total", "Alerts emitted by the secondary risk engine", ["severity"]
)
ENGINE_UP = Gauge(
    "secondary_engine_up", "1 while the secondary engine's consumer loop is alive"
)
LAST_EVENT_TS = Gauge(
    "secondary_engine_last_event_unixtime", "Unix timestamp of the last event this engine processed"
)
IDENTITIES_TRACKED = Gauge(
    "secondary_engine_identities_tracked", "Number of identities currently held in in-memory risk state"
)

# --------------------------------------------------------------------------
# Avro deserializer for api_requests (same schema/lookup logic as rules_engine.py)
# --------------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
SCHEMA_PATH = os.getenv(
    "API_REQUEST_SCHEMA_PATH",
    os.path.join(REPO_ROOT_DIR, "config", "schemas", "api_request.avsc"),
)
if not os.path.exists(SCHEMA_PATH):
    SCHEMA_PATH = os.path.join(REPO_ROOT_DIR, "schemas", "api_request.avsc")

with open(SCHEMA_PATH, "r") as f:
    _api_request_schema_str = f.read()

_schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
_avro_deserializer = AvroDeserializer(_schema_registry_client, _api_request_schema_str, lambda d, ctx: d)
_string_deserializer = StringDeserializer("utf_8")
_api_requests_ctx = SerializationContext("api_requests", MessageField.VALUE)

# --------------------------------------------------------------------------
# In-memory state (the "lean-state" equivalent of RiskProfileState.java)
# --------------------------------------------------------------------------
account_to_customer = {}  # account_id -> customer_id, built from banking.accounts

def _new_profile():
    return {
        "score": 0.0,
        "last_decay": time.time(),
        "indicators": deque(maxlen=8),
        "last_device": None,
        "last_country": None,
        "last_login_ts": None,
        "last_alert_ts": 0.0,
        "failed_login_ts": deque(maxlen=MAX_RETAINED_TIMESTAMPS),
        "tx_ts": deque(maxlen=MAX_RETAINED_TIMESTAMPS),
        "api_ts": deque(maxlen=MAX_RETAINED_TIMESTAMPS),
    }

risk_state = defaultdict(_new_profile)
_state_lock = threading.Lock()

producer = Producer({"bootstrap.servers": KAFKA_BROKER})


def _prune(dq, cutoff):
    while dq and dq[0] < cutoff:
        dq.popleft()


def _resolve_identity(customer_id, account_id, ip_address):
    """Mirrors RiskEventNormalizer's fallback chain: customer_id > acct: > ip:"""
    if customer_id:
        return customer_id
    if account_id:
        resolved = account_to_customer.get(account_id)
        if resolved:
            return resolved
        return f"acct:{account_id}"
    if ip_address:
        return f"ip:{ip_address}"
    return None


def _apply_decay(profile, now):
    elapsed = now - profile["last_decay"]
    halvings = min(int(elapsed // DECAY_SEC), 10)
    if halvings > 0:
        profile["score"] = profile["score"] / (2 ** halvings)
        profile["last_decay"] = now


def _maybe_alert(identity_key, account_id, profile, now):
    if profile["score"] < ALERT_THRESHOLD:
        return
    if now - profile["last_alert_ts"] < COOLDOWN_SEC:
        return

    severity = "CRITICAL" if profile["score"] >= CRITICAL_THRESHOLD else "FRAUD"
    indicators = list(profile["indicators"])
    alert = {
        "alert_id": f"SEC-{identity_key}-{int(now // COOLDOWN_SEC)}",
        "customer_id": identity_key,
        "account_id": account_id,
        "event_time": int(now * 1000),
        "severity": severity,
        "cumulative_score": round(profile["score"], 2),
        "threshold": ALERT_THRESHOLD,
        "contributing_indicators": indicators,
        "trigger_reason": indicators[-1] if indicators else "ANOMALY_DETECTED",
        "first_seen_ts": int(now * 1000),
        "latest_seen_ts": int(now * 1000),
        "source_event_ids": [],
        "action_required": "FREEZE_ACCOUNT" if severity == "CRITICAL" else "REQUIRE_MFA",
        "engine_source": ENGINE_SOURCE,
    }

    topic = OUTPUT_CRITICAL_TOPIC if severity == "CRITICAL" else OUTPUT_FRAUD_TOPIC
    producer.produce(topic, key=identity_key, value=json.dumps(alert))
    producer.flush()
    ALERTS_EMITTED.labels(severity=severity).inc()
    print(f"🚨 [SECONDARY ENGINE] {severity} alert for {identity_key} (score={profile['score']:.1f}) -> {topic}")

    profile["last_alert_ts"] = now
    profile["score"] = 0.0
    profile["indicators"].clear()


def _add_indicator(profile, name, now):
    profile["score"] += RISK_WEIGHTS.get(name, 0)
    profile["indicators"].append(name)


def handle_login_event(row, now):
    customer_id = row.get("customer_id")
    identity_key = _resolve_identity(customer_id, None, None)
    if not identity_key:
        return

    with _state_lock:
        profile = risk_state[identity_key]
        _apply_decay(profile, now)

        device_id = row.get("device_id")
        country = row.get("country")
        success = row.get("success", row.get("status") not in ("FAILED", "FAILURE"))

        if device_id and profile["last_device"] and device_id != profile["last_device"]:
            _add_indicator(profile, "NEW_DEVICE", now)
        if country and profile["last_country"] and country != profile["last_country"]:
            _add_indicator(profile, "NEW_COUNTRY", now)
            if profile["last_login_ts"] and (now - profile["last_login_ts"]) < IMPOSSIBLE_TRAVEL_WINDOW_SEC:
                _add_indicator(profile, "IMPOSSIBLE_TRAVEL", now)
        if device_id:
            profile["last_device"] = device_id
        if country:
            profile["last_country"] = country
        profile["last_login_ts"] = now

        if not success:
            profile["failed_login_ts"].append(now)
            _prune(profile["failed_login_ts"], now - FAILED_LOGIN_WINDOW_SEC)
            if len(profile["failed_login_ts"]) >= FAILED_LOGIN_THRESHOLD:
                _add_indicator(profile, "MULTIPLE_FAILED_LOGINS", now)

        _maybe_alert(identity_key, None, profile, now)


def handle_transaction(row, now):
    from_account = row.get("from_account")
    customer_id = account_to_customer.get(from_account)
    identity_key = _resolve_identity(customer_id, from_account, None)
    if not identity_key:
        return

    with _state_lock:
        profile = risk_state[identity_key]
        _apply_decay(profile, now)

        amount = float(row.get("amount") or 0)
        if amount > LARGE_TRANSFER_THRESHOLD:
            _add_indicator(profile, "LARGE_TRANSFER_AMOUNT", now)

        profile["tx_ts"].append(now)
        _prune(profile["tx_ts"], now - VELOCITY_WINDOW_SEC)
        if len(profile["tx_ts"]) >= VELOCITY_COUNT_THRESHOLD:
            _add_indicator(profile, "VELOCITY_VIOLATION", now)

        _maybe_alert(identity_key, from_account, profile, now)


def handle_api_request(payload, now):
    ip_address = payload.get("ip_address")
    endpoint = payload.get("endpoint") or ""
    identity_key = _resolve_identity(None, None, ip_address)
    if not identity_key:
        return
    if not any(sensitive in endpoint for sensitive in SENSITIVE_ENDPOINTS):
        return

    with _state_lock:
        profile = risk_state[identity_key]
        _apply_decay(profile, now)

        profile["api_ts"].append(now)
        _prune(profile["api_ts"], now - API_RATE_WINDOW_SEC)
        if len(profile["api_ts"]) >= API_RATE_THRESHOLD:
            _add_indicator(profile, "SUSPICIOUS_API_PATTERN", now)

        _maybe_alert(identity_key, None, profile, now)


def handle_account_cdc(row, op):
    account_id = row.get("id") or row.get("account_id")
    customer_id = row.get("customer_id")
    if not account_id:
        return
    if op == "d":
        account_to_customer.pop(account_id, None)
    elif customer_id:
        account_to_customer[account_id] = customer_id


def _unwrap_cdc(raw_value):
    """Mirrors RiskEventNormalizer / opensearch_sink's unwrap_cdc_envelope."""
    envelope = json.loads(raw_value.decode("utf-8"))
    op = envelope.get("op")
    after = envelope.get("after")
    return op, after


def consume_topic(topic, handler_kind):
    """One Consumer per topic -- deliberately mirrors the per-topic-thread fix
    documented in analytics/opensearch/opensearch_sink.py (Chapter 8), so this
    engine doesn't reintroduce the same single-poll-loop starvation bug."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([topic])
    print(f"🎧 [SECONDARY ENGINE] listening on {topic}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            now = time.time()
            try:
                if handler_kind == "api_requests":
                    payload = _avro_deserializer(msg.value(), _api_requests_ctx)
                    if payload:
                        handle_api_request(payload, now)
                elif handler_kind == "login_events":
                    op, after = _unwrap_cdc(msg.value())
                    if op != "d" and after:
                        handle_login_event(after, now)
                elif handler_kind == "transactions":
                    op, after = _unwrap_cdc(msg.value())
                    if op != "d" and after:
                        handle_transaction(after, now)
                elif handler_kind == "accounts":
                    op, after = _unwrap_cdc(msg.value())
                    handle_account_cdc(after or {}, op)
            except Exception as e:
                print(f"⚠️  [SECONDARY ENGINE] failed to process message on {topic}: {e}")
                continue

            EVENTS_PROCESSED.labels(topic=topic).inc()
            LAST_EVENT_TS.set(now)
            IDENTITIES_TRACKED.set(len(risk_state))
    finally:
        consumer.close()


def main():
    start_http_server(METRICS_PORT)
    ENGINE_UP.set(1)
    print(f"📊 [SECONDARY ENGINE] Prometheus metrics on :{METRICS_PORT}/metrics")
    print("Secondary Risk Engine online (active-active alongside the Flink engine).")

    topics = [
        ("api_requests", "api_requests"),
        ("banking.login_events", "login_events"),
        ("banking.transactions", "transactions"),
        ("banking.accounts", "accounts"),
    ]
    threads = [
        threading.Thread(target=consume_topic, args=(topic, kind), daemon=True)
        for topic, kind in topics
    ]
    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("Shutting down Secondary Risk Engine...")
        ENGINE_UP.set(0)


if __name__ == "__main__":
    main()