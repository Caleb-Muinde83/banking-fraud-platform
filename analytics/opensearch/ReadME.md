# Development Plan: Dual Alert Routing, Geo-Point Support, OpenSearch Sink Rewrite

Covers three linked pieces of work, in dependency order:

1. **Alert routing (Flink)** — split `fraud_alerts_v2` / `critical_alerts_v2` into two real
   Kafka topics instead of one topic with a `severity` field.
2. **Geo-point support (Postgres → API → simulator)** — add real lat/lon data for
   `login_events` so the OpenSearch geo-map goal is actually achievable.
3. **`opensearch_sink.py` rewrite** — fix the topic-name, Avro, and envelope-unwrapping
   bugs found in the audit, and consume the new geo fields.

Directory structure referenced throughout:
```
banking-fraud-platform/
├── analytics/
│   ├── flink-risk-engine-java/   (Java Flink job)
│   └── opensearch/               (Python OpenSearch sink)
├── api/                          (FastAPI)
└── simulator/                    (Python traffic/attack simulator)
```

---

## Part 1 — Alert routing split (Flink)

**Decision (confirmed):** Option B — update `RiskScoringJob` to route `≥100` scores to
`critical_alerts_v2` and `>80, <100` scores to `fraud_alerts_v2`, as two real Kafka
topics. This matches the original approved plan and the topics `kafka-init` already
provisions (which currently sit empty for `critical_alerts_v2`).

### 1.1 Split the alert stream

`RiskScoringProcessor` already tags every `FraudAlert` with `severity` (`"FRAUD"` /
`"CRITICAL"`). Split the single `alerts` `DataStream<FraudAlert>` into two branches by
`severity` using two `.filter(...)` calls off the same stream, each with its own
`.sinkTo(...)`:

- `alerts.filter(a -> "FRAUD".equals(a.getSeverity()))` → sink → `fraud_alerts_v2`
- `alerts.filter(a -> "CRITICAL".equals(a.getSeverity()))` → sink → `critical_alerts_v2`

This is simpler than Flink side-outputs and adequate at this volume — both filters
re-scan the same stream, which costs a bit of duplicate CPU but avoids the extra
`OutputTag`/`ProcessFunction.Context.output()` machinery. Worth switching to side
outputs later only if profiling shows the double-filter is actually a bottleneck.

### 1.2 New/changed config

- `KAFKA_ALERT_TOPIC` env var becomes `KAFKA_FRAUD_ALERT_TOPIC` (default
  `fraud_alerts_v2`) — rename for clarity now that there are two.
- New `KAFKA_CRITICAL_ALERT_TOPIC` env var (default `critical_alerts_v2`).
- `RiskScoringConfig` gets a second `SCHEMA_REGISTRY_SUBJECT`-equivalent for the
  critical topic (`critical_alerts_v2-value`), or reuse the same `FraudAlert` Avro
  schema for both subjects — the schema is identical, only the topic differs, so a
  single `FraudAlertAvroSerializationSchema` instance can serve both sinks.

### 1.3 Docker Compose

Update the `fraud-engine` service's environment block in `docker-compose.yml`:
```yaml
environment:
  KAFKA_BOOTSTRAP_SERVERS: kafka:29092
  FLINK_RISK_GROUP_ID: fraud-engine-v2
  KAFKA_FRAUD_ALERT_TOPIC: fraud_alerts_v2
  KAFKA_CRITICAL_ALERT_TOPIC: critical_alerts_v2
```

### 1.4 Test impact

`RiskScoringIntegrationTest`'s existing "velocity attack" scenario ends at score 100
(CRITICAL) — once routing splits, that test should assert the alert appears in the
**critical** sink path, not just that one `FraudAlert` object was collected. Add a
second scenario that stays in the 81–99 range to explicitly cover the `fraud_alerts_v2`
path once the split exists, so both branches have direct coverage rather than only the
CRITICAL path being exercised.

---

## Part 2 — Geo-point support

**Decision (confirmed):** store real lat/lon on `login_events` via schema changes,
not a runtime GeoIP lookup. Real IP-geolocation (e.g. MaxMind GeoLite2) would be the
production-correct approach, but it's pointless here — the simulator's IPs are
synthetic, so looking up geo data for fake IPs returns nothing meaningful. Instead,
the simulator generates a country (as it already does) and derives a matching lat/lon
from a static country→coordinate table, and the API just persists what it's given.

Scope is deliberately **`login_events` only** — that's what the original OpenSearch
plan's geo-map goal (Phase 4: "geographical map visualization... from failed logins")
actually needs. Not extending to `sessions`/`transactions` unless a concrete need
shows up later.

### 2.1 Postgres schema change

Add two nullable columns to `login_events`:

```sql
ALTER TABLE public.login_events
  ADD COLUMN latitude DOUBLE PRECISION,
  ADD COLUMN longitude DOUBLE PRECISION;
```

Use `DOUBLE PRECISION`, not `NUMERIC` — sidesteps any ambiguity with the Debezium
connector's `decimal.handling.mode: double` setting entirely (plain doubles are
already emitted as JSON numbers, no decimal-handling path involved).

Existing historical rows will have `NULL` lat/lon — no backfill planned; only new
rows generated after this change carry geo data. Acceptable for a demo/homelab
dataset; flag if backfilling old rows turns out to matter.

Debezium requires no reconfiguration — `public.login_events` is already in
`table.include.list`; new columns just start appearing in future `after` blocks
automatically.

### 2.2 SQLAlchemy model (`api/app/models/domain.py`)

Add to the `LoginEvent` model:
```python
latitude = Column(Float, nullable=True)
longitude = Column(Float, nullable=True)
```

### 2.3 Pydantic schemas (`api/app/schemas/*.py`)

`LoginRequest`/`LoginPayload` gain optional `latitude`/`longitude` fields (client
supplies them — see §2.5 on why the *simulator* computes these, not the API):
```python
latitude: Optional[float] = None
longitude: Optional[float] = None
```

### 2.4 API changes (`api/app/main.py`)

In `api_login`, when constructing `models.LoginEvent(...)`, pass through
`latitude=payload.latitude, longitude=payload.longitude`. No geolocation logic
belongs in the API itself — it just persists whatever the caller (simulator, or a
real client) provides. Real clients (browser/mobile) would supply device GPS or a
geolocation API result; the simulator supplies its synthetic equivalent.

### 2.5 Simulator changes (`simulator/app/...`)

Add a small static `COUNTRY_COORDINATES` table (country code → approximate
centroid lat/lon, e.g. `"US": (39.8, -98.6)`, `"UA": (48.4, 31.2)`, `"NG": (9.1, 8.7)`,
`"CN": (35.9, 104.2)`, `"KE": (-0.02, 37.9)`, covering whatever country codes the
simulator's scenarios already use). Wherever the simulator currently picks/sends a
`country` for a login attempt (`scenarios.py`, `customer.py`, or wherever
`LoginPayload`/`LoginRequest` gets built), look up the matching coordinates and add a
small random jitter (e.g. ±2 degrees) so simulated events aren't all stacked on the
exact same point on the map, then include `latitude`/`longitude` in the request
payload sent to `/api/login`.

This keeps geo generation deterministic, dependency-free (no MaxMind database file to
ship/manage), and semantically consistent with the country the scenario already
claims to be attacking from.

### 2.6 Flink engine (optional follow-up, not required for the OpenSearch goal)

`RiskEventNormalizer`/`RiskScoringProcessor` could eventually use lat/lon for a more
precise impossible-travel check (actual distance/speed calculation instead of "country
changed within N hours"). Not needed to unblock the OpenSearch geo-map work — noting
it here as a natural future enhancement once the data exists, not committing to it now.

---

## Part 3 — `opensearch_sink.py` rewrite

Location: `analytics/opensearch/opensearch_sink.py`.

### 3.1 Topic name corrections

```python
TOPIC_INDEX_MAP = {
    "api_requests": "api_requests",
    "banking.login_events": "login_events",
    "fraud_alerts_v2": "alerts",
    "critical_alerts_v2": "alerts",   # same OpenSearch index; severity field distinguishes them
}
```
Both alert topics feed the same `alerts` index — no need for two OpenSearch indices
just because there are now two Kafka topics; `severity` in the payload already
distinguishes FRAUD from CRITICAL for querying/filtering.

### 3.2 Avro handling for the alert topics

`api_requests` and `banking.login_events` are plain JSON (Debezium's `JsonConverter`,
and the API's `AIOKafkaProducer` both confirmed JSON, not Avro). `fraud_alerts_v2`
and `critical_alerts_v2` are Avro via Schema Registry (`FraudAlertAvroSerializationSchema`).

A single `KafkaConsumer` with one `value_deserializer` can't do both — switch to a
**per-message** deserialization strategy based on topic, using a real
Schema-Registry-aware Avro deserializer for the alert topics:

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer
```

Recommend switching the whole consumer from `kafka-python` to `confluent-kafka`
(the library the Schema Registry integration is actually built for), with a
`DeserializingConsumer` configured per-topic, or a manual branch in the message loop:
JSON path for CDC/telemetry topics, `AvroDeserializer` path (backed by
`SchemaRegistryClient` pointed at `SCHEMA_REGISTRY_URL`) for the two alert topics.

Critically: **wrap deserialization itself in the try/except**, not just the
`index()` call — a single malformed/mismatched message should log and continue, not
kill the whole consumer loop (this was the root cause of the original crash risk).

### 3.3 Debezium envelope unwrapping

For `banking.login_events` (and any other CDC topic added later), extract the
`after` block before indexing, and skip `op == "d"` (delete) messages — mirrors what
`RiskEventNormalizer` already does on the Flink side:

```python
def unwrap_cdc_envelope(payload: dict) -> dict | None:
    if payload.get("op") == "d":
        return None
    after = payload.get("after")
    return after if after is not None else payload
```

### 3.4 Index mapping corrections

Fix field names to match the real schema, and add the new geo fields:

```python
"login_events": {
    "mappings": {
        "properties": {
            "timestamp":  {"type": "date"},
            "customer_id":{"type": "keyword"},
            "success":    {"type": "boolean"},   # was "status" (keyword) — wrong field entirely
            "ip_address": {"type": "ip"},
            "country":    {"type": "keyword"},   # was "location" — real column is "country"
            "device_id":  {"type": "keyword"},
            "geo_location": {"type": "geo_point"}  # NEW — derived from latitude/longitude at index time
        }
    }
},
"api_requests": {
    "mappings": {
        "properties": {
            "timestamp":   {"type": "date"},
            "request_id":  {"type": "keyword"},   # real field, wasn't mapped before
            "ip_address":  {"type": "ip"},
            "user_agent":  {"type": "keyword"},   # real field, wasn't mapped before
            "endpoint":    {"type": "keyword"},
            "method":      {"type": "keyword"},   # real field, wasn't mapped before
            "status_code": {"type": "integer"}    # real field, wasn't mapped before
            # "customer_id" / "action" / "amount" removed — don't exist in api_request.avsc
        }
    }
},
"alerts": {
    "mappings": {
        "properties": {
            "timestamp":               {"type": "date"},
            "alert_id":                {"type": "keyword"},
            "customer_id":             {"type": "keyword"},
            "severity":                {"type": "keyword"},  # NEW — distinguishes FRAUD vs CRITICAL now that both topics share this index
            "cumulative_score":        {"type": "integer"},
            "contributing_indicators": {"type": "keyword"},
            "action_required":         {"type": "keyword"}
        }
    }
}
```

### 3.5 Geo-point transform at index time

`login_events` documents (post-envelope-unwrap) carry separate `latitude`/`longitude`
fields from Postgres — OpenSearch's `geo_point` type expects a single field. Transform
before indexing:

```python
if doc.get("latitude") is not None and doc.get("longitude") is not None:
    doc["geo_location"] = {"lat": doc["latitude"], "lon": doc["longitude"]}
```

### 3.6 Starting offset for initial validation

Switch `auto_offset_reset` to `earliest` for the first validation pass (so Phase 3's
"check Discover tab" step actually shows the backlog of already-produced events), then
decide whether to keep `earliest` or move to `latest` for steady-state running —
`earliest` risks a large reindex on every fresh sink restart once topics have real
volume, `latest` misses anything produced while the sink was down. Worth deciding
based on how you want restart behavior to work, not defaulting silently either way.

---

## Part 4 — Deployment wiring

### 4.1 Dockerfile for the sink

New file: `analytics/opensearch/Dockerfile` — Python base image, install
`confluent-kafka`, `opensearch-py`, run `opensearch_sink.py`.

### 4.2 Docker Compose service

New service in `docker-compose.yml`, alongside the existing `fraud-engine` entry:

```yaml
opensearch-sink:
  build:
    context: ./analytics/opensearch
    dockerfile: Dockerfile
  container_name: bank_opensearch_sink
  depends_on:
    - kafka
    - opensearch
    - schema-registry
  environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    OPENSEARCH_HOST: opensearch
    OPENSEARCH_PORT: 9200
    SCHEMA_REGISTRY_URL: http://schema-registry:8081
  restart: unless-stopped
```

---

## Part 5 — Validation plan

1. Apply the Postgres migration; confirm `\d+ login_events` shows the new columns.
2. Update simulator + API; run one login-based scenario; confirm the new row in
   Postgres has non-null `latitude`/`longitude`.
3. Confirm Debezium picks up the new columns without connector changes (check a raw
   `banking.login_events` message via console consumer).
4. Rebuild `RiskScoringJob` with the routing split; confirm (via console consumer or
   `kafka-consumer-groups`) that a FRAUD-range and a CRITICAL-range alert each land on
   their respective topic.
5. Bring up `opensearch-sink`; confirm via OpenSearch Dashboards' Discover tab that:
   - `login_events` documents have populated `customer_id`/`success`/`geo_location`
     (not empty — this was the original envelope-unwrap bug)
   - `alerts` documents from both `fraud_alerts_v2` and `critical_alerts_v2` appear in
     the same index with the correct `severity` value
   - `api_requests` documents have the real fields (`request_id`, `method`,
     `status_code`, etc.)
6. Only after the above: build the Phase 4 threat-hunting DSL queries and the
   geo-map visualization against real, correctly-shaped data.

---

## Open questions before implementation starts

- **Country coordinate table completeness** — need the full list of country codes the
  simulator's scenarios actually use, to make sure `COUNTRY_COORDINATES` covers all
  of them (missing entries would need a fallback, e.g. `(0, 0)` or skip geo entirely
  for that event).
- **`confluent-kafka` vs. staying on `kafka-python` + manual Schema Registry HTTP
  calls** — `confluent-kafka` is the more standard/robust choice but is a new
  dependency (C-extension based, needs `librdkafka`); worth confirming that's
  acceptable in the sink's runtime image versus a pure-Python alternative
  (`fastavro` + manual registry fetch) if dependency weight is a concern.


  *************************************************
  *****************************************************
# OpenSearch Dashboards One-Time Setup

> **Note:** This setup has not been completed yet. Without index patterns, the **Discover** tab will remain empty even when data exists in OpenSearch indices.

## Open OpenSearch Dashboards

Navigate to:

```text
http://localhost:5601/app/login
```

Log in using:

```text
Username: admin
Password: admin
```

## Create Index Patterns

From the left sidebar:

```text
Dashboards Management → Index Patterns → Create index pattern
```

Create the following index patterns:

- `login_events`
- `api_requests`
- `alerts`

## Configure Time Fields

When creating each index pattern, select the correct time field:

| Index Pattern | Time Field |
|--------------|------------|
| `login_events` | `timestamp` |
| `api_requests` | `timestamp` |
| `alerts` | `event_time` |

> The `alerts` index uses `event_time` because it matches the actual `FraudAlert` schema and the OpenSearch mapping configuration.