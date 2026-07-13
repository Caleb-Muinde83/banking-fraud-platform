F:\DaTech-kafka\banking-fraud-platform\analytics\flink-risk-engine-java\README.md

# Flink Risk Engine (Java)

Real-time, stateful fraud-scoring engine for the banking fraud platform. Runs as an
Apache Flink job on the Swarm cluster's Flink JobManager/TaskManager, consuming CDC
events from Kafka and emitting fraud alerts back to Kafka.

This module is **additive** to the existing Python prototypes
(`analytics/apache-flink-scripts/flink_risk_engine.py`,
`analytics/kafka-stream-scripts/risk_scoring_engine.py`), which remain untouched as
learner reference material. This engine uses its own consumer groups and its own
output topics (`_v2` suffix) so the two can run side by side without colliding.

---

## 1. What this engine does

For every transaction, login, session, and API-request event flowing through the
platform's CDC pipeline, this engine maintains a running "risk profile" per identity
(customer, or a fallback identity — see §3) and:

1. Detects fraud **indicators** by comparing each incoming event against that
   identity's stored history (new device, new country, impossible travel timing,
   large transfer, transaction velocity, high-risk recipient, suspicious API rate,
   repeated failed logins).
2. Accumulates indicator weights into a **cumulative score**, which decays over time
   if no new suspicious activity occurs.
3. Emits a **fraud alert** to Kafka once the score crosses a threshold, with
   deterministic, idempotent alert IDs and cooldown-based suppression so a sustained
   attack produces one alert per cooldown window, not a storm.

### Pipeline shape

```
banking.transactions -.
banking.login_events -+-> normalize (unwrap Debezium envelope) -> [account enrichment] -> filter (has identity)
banking.sessions     -'                                                  ^
api_requests ------------------------------------------------------------'
                                                                          |
banking.accounts --> normalize --> broadcast (account_id -> customer_id) -'
                                                                          |
                                                                          v
                                         assign watermarks (event time, idleness-aware)
                                                                          |
                                                                          v
                                               keyBy(identityKey) -> RiskScoringProcessor
                                               (derive indicators, score, decay, alert)
                                                                          |
                                                                          v
                                         Avro-serialize (Schema Registry) -> Kafka sink
                                                                          |
                                                                          v
                                                           fraud_alerts_v2 (Kafka topic)
```

---

## 2. Source file reference (`src/main/java/com/banking/fraud/`)

### `RiskEvent.java`
Plain data object representing one normalized event, regardless of which source
topic it came from. Carries both the common fields (`customerId`, `eventTime`,
`dedupKey`, `identityKey`) and source-specific raw fields used for indicator
derivation (`country`, `deviceId`, `loginSuccess`, `amount`, `fromAccountId`,
`toAccountId`, `ipAddress`). The `indicator` field is an optional explicit override
(back-compat path) — when absent, indicators are derived by `RiskScoringProcessor`
instead of trusted from upstream.

### `RiskEventNormalizer.java`
Unwraps the Debezium CDC envelope (`{before, after, source, op, ts_ms}`) for
`banking.transactions` / `banking.login_events` / `banking.sessions`, and also
handles raw `api_requests` telemetry (no CDC envelope at all). Responsibilities:
- Filters out `op: "d"` (delete) events.
- Extracts the table name from `source.table`, stripping the `public.` schema prefix.
- Extracts raw fields per table, matching the actual Postgres schema (confirmed
  against `schema.sql`): `login_events` has `country`/`device_id`/`success`;
  `transactions` has `from_account`/`to_account`/`amount` (**not** `customer_id` or
  `beneficiary_id` — the real schema has neither column).
- Computes `identityKey`: `customer_id` when present, else `"ip:<address>"` for
  `api_requests` (which has no `customer_id` at all), else
  `"acct:<from_account>"` for transactions as a fallback until
  `AccountEnrichmentFunction` resolves the real customer (see below).
- Computes a dedup key, preferring `source.lsn`, then `source.txId`, then a
  `table:event_id` composite, so Debezium redelivery after a connector restart
  doesn't get rescored.

### `AccountUpdate.java`
Small data object: one `account_id -> customer_id` mapping event (or a deletion),
derived from the `banking.accounts` CDC stream.

### `AccountEventNormalizer.java`
Normalizes `banking.accounts` CDC events into `AccountUpdate` objects. On delete,
reads the departing `account_id` from the `before` block (since `after` is null).

### `AccountEnrichmentFunction.java`
Resolves the *real* `customer_id` for transaction events via Flink's **broadcast
state pattern**: `banking.accounts` is small and slow-changing relative to
transaction volume, so it's broadcast in full to every parallel instance of this
function rather than partitioned. For each transaction event with no `customer_id`,
it looks up `from_account` in the broadcast map; if found, overwrites
`customerId`/`identityKey` with the resolved value. If not found (mapping hasn't
arrived yet, or genuinely unknown), the event keeps its `acct:<from_account>`
fallback identity rather than being dropped.

**Known limitation** (inherent to the broadcast state pattern, not fixable at the
application level): the broadcast side and the main stream are not guaranteed to be
processed in strict global order relative to each other. A transaction on a
brand-new account can occasionally be scored under the `acct:` fallback if it
arrives before its account's CDC row has propagated through the join.

### `RiskProfileState.java`
Per-identity state, keyed by `identityKey`. Holds:
- `cumulativeScore`, `lastScoreUpdateTs`, `lastAlertTs`
- `activeIndicators` (hard-capped at 8 — a ring buffer, not an unbounded log)
- `lastKnownCountry`, `lastKnownDeviceId`, `lastLoginEventTs` (used for
  `NEW_DEVICE`/`NEW_COUNTRY`/`IMPOSSIBLE_TRAVEL`)
- Bounded timestamp ring buffers (hard-capped at 50 entries regardless of time
  window) for failed logins, transactions, and API requests, used to compute
  velocity-style indicators
- Decay bookkeeping: `lastDecayAppliedTs`, `nextDecayTimerTs`

### `RiskScoringConfig.java`
Loads `risk-scoring.properties` from the classpath and exposes typed constants:
alert/critical thresholds, cooldown, state TTL, decay interval, indicator weights,
indicator-derivation thresholds (large-transfer amount, velocity/failed-login/API-rate
windows and counts, impossible-travel window), the high-risk-recipient account list,
and Schema Registry URL/subject.

### `RiskScoringProcessor.java`
The core of the engine — a `KeyedProcessFunction<String, RiskEvent, FraudAlert>`.
Per event:
1. **Dedup** — checks the event's dedup key against a bounded, age-pruned `MapState`
   (not an unbounded set); duplicates are dropped before they can affect the score.
2. **Decay** — reduces `cumulativeScore` for elapsed `DECAY_MS` intervals since the
   last update (halving per interval), so an old spike doesn't keep contributing
   forever. Also self-schedules an event-time timer (`onTimer`) so decay continues
   even without further events for that identity.
3. **Indicator derivation** (`deriveIndicators`, package-private for direct
   testability) — computes indicators from raw event fields vs. stored profile
   state, per source table:
   - `login_events`: `NEW_DEVICE`, `NEW_COUNTRY`, `IMPOSSIBLE_TRAVEL` (a country
     change happening within `IMPOSSIBLE_TRAVEL_WINDOW_MS` of the previous login),
     `MULTIPLE_FAILED_LOGINS`
   - `transactions`: `LARGE_TRANSFER_AMOUNT`, `VELOCITY_VIOLATION`,
     `HIGH_RISK_RECIPIENT` (checked against `to_account`, since the real
     `Transaction` table has no `beneficiary_id` column)
   - `api_requests`: `SUSPICIOUS_API_PATTERN`
   - `sessions`: no indicator — the real `Session` table carries no country/geo
     column, so nothing in the original taxonomy can be derived from it
   - If the event carries an explicit `indicator` override, derivation is skipped
     entirely (back-compat path).
4. **Alerting** — if `cumulativeScore > ALERT_THRESHOLD` (80) and the cooldown has
   expired, builds a deterministic alert ID from `(identityKey, severity,
   cooldownWindowStart)` and emits a `FraudAlert`, checked against a bounded
   `emittedAlertIds` map so the same logical alert is never re-emitted within its
   window. Severity is `CRITICAL` (action `LOCK_ACCOUNT`) at `>= 100`, else `FRAUD`
   (action `REQUIRE_MFA`).

State TTL (`TTL_MS`, default 24h) is applied to all three `MapState`/`ValueState`
descriptors via `StateTtlConfig`, with `cleanupInRocksdbCompactFilter(1000)` so
expired entries are actually reclaimed from RocksDB, not just hidden on read.

### `FraudAlert.java`
Plain data object for the alert schema: `alertId`, `customerId`, `accountId`,
`eventTime`, `severity`, `cumulativeScore`, `threshold`, `contributingIndicators`,
`triggerReason`, `firstSeenTs`, `latestSeenTs`, `sourceEventIds`, `actionRequired`.

### `FraudAlertAvroSerializationSchema.java`
Serializes `FraudAlert` via Flink's `ConfluentRegistryAvroSerializationSchema`,
registering/resolving the schema against the real Schema Registry (not a hand-rolled
local schema) so downstream consumers using standard Confluent Avro deserialization
can actually read the topic.

### `RiskScoringJob.java`
Wires everything into an executable Flink job (`main()`):
- `EmbeddedRocksDBStateBackend`, checkpointing every 15s (`EXACTLY_ONCE`), 3 retries
  with a 10s fixed delay.
- Kafka source over `banking.transactions`, `banking.login_events`,
  `banking.sessions`, `api_requests` (consumer group `fraud-engine-v2` by default).
- Second Kafka source over `banking.accounts` (consumer group
  `<group>-accounts`), broadcast for the account-enrichment join.
- Normalizes -> enriches (account join) -> **then** filters on `identityKey != null`
  (deliberately after enrichment, so transactions awaiting their account mapping
  aren't dropped prematurely) -> assigns watermarks
  (`forBoundedOutOfOrderness(300s)` + `withIdleness(60s)`) -> keys by `identityKey` ->
  scores -> serializes -> sinks to Kafka (`AT_LEAST_ONCE`).

---

## 3. Identity resolution — how events get keyed

| Source | Primary identity | Fallback |
|---|---|---|
| `login_events`, `sessions` | `customer_id` (always present) | — |
| `transactions` | `customer_id` resolved via `AccountEnrichmentFunction` (accounts join) | `"acct:<from_account>"` if the mapping hasn't arrived/doesn't exist |
| `api_requests` | — (schema has no `customer_id`) | `"ip:<ip_address>"` |

Seeing `acct:` or `ip:` prefixes in alert output is expected under normal
operation, not necessarily a bug — it means the identity is resolved at the
account/IP level rather than the customer level for that event.

---

## 4. Configuration (`src/main/resources/risk-scoring.properties`)

| Key | Default | Meaning |
|---|---|---|
| `alert.threshold` | 80 | Score above which a FRAUD alert fires |
| `alert.critical-threshold` | 100 | Score at/above which severity is CRITICAL |
| `alert.cooldown-ms` | 900000 (15 min) | Suppression window per identity per severity |
| `state.ttl-ms` | 86400000 (24h) | Profile/dedup/alert-id state expiry |
| `state.decay-ms` | 1800000 (30 min) | Score halves per elapsed interval |
| `weights.*` | 20-50 | Per-indicator score contribution |
| `indicator.large-transfer-threshold` | 10000 | Amount above which `LARGE_TRANSFER_AMOUNT` fires |
| `indicator.velocity-window-ms` / `-count-threshold` | 300000 / 4 | Transaction velocity window/count |
| `indicator.failed-login-window-ms` / `-threshold` | 300000 / 3 | Failed-login burst window/count |
| `indicator.impossible-travel-window-ms` | 7200000 (2h) | Max gap for a country change to count as impossible travel |
| `indicator.api-rate-window-ms` / `-threshold` | 60000 / 50 | API request-rate window/count |
| `indicator.high-risk-recipients` | *(empty)* | Comma-separated `to_account` values to flag as `HIGH_RISK_RECIPIENT` |
| `schema-registry.url` / `-subject` | `http://schema-registry:8081` / `fraud_alerts_v2-value` | Avro registration target |

All of the `indicator.*` thresholds were introduced specifically for this engine —
they're reasonable starting points, not values derived from historical fraud data.
Expect to tune them once real alert volume is observed.

---

## 5. Running the tests

```bash
cd analytics/flink-risk-engine-java
mvn clean package
```

This runs all five test classes (23 tests total as of the last verified build):

| Test class | What it covers | How |
|---|---|---|
| `RiskEventNormalizerTest` | Debezium unwrapping, dedup key derivation, raw field extraction, IP-fallback identity | Plain unit tests, no Flink runtime |
| `AccountEventNormalizerTest` | `banking.accounts` CDC parsing (create/update/delete) | Plain unit tests |
| `RiskScoringProcessorTest` | `deriveIndicators` across all source tables, bounded state, deterministic alert IDs | Plain unit tests (`deriveIndicators` is package-private specifically so it's testable without a Flink harness) |
| `AccountEnrichmentFunctionIntegrationTest` | Broadcast join resolves customer_id; falls back gracefully when unknown; handles account deletion | `ProcessFunctionTestHarnesses.forBroadcastProcessFunction(...)` — deterministic, explicit ordering of broadcast vs. main-stream input, no MiniCluster needed |
| `RiskScoringIntegrationTest` | Full keyed-process scoring flow end-to-end (score crossing 80->100 across a sequence of events, cooldown suppression, action-required mapping) | Real Flink `MiniCluster` via `env.fromElements(...).keyBy(...).process(...).executeAndCollect()` |

`mvn test` runs tests only; `mvn clean package` also builds the shaded (fat) jar
needed for actual deployment.

**A note on test speed:** the `MiniCluster`-based `RiskScoringIntegrationTest` takes
~15-45s to spin up per run — much slower than the harness-based tests, which run in
milliseconds. This is expected; it's genuinely starting a local Flink cluster.

---

## 6. Running the job

### Build the deployable jar

```bash
cd analytics/flink-risk-engine-java
mvn clean package
```

Produces `target/flink-risk-engine-java-1.0-SNAPSHOT.jar` — a shaded (fat) jar
with all dependencies bundled (Kafka connector, Avro, Schema Registry client,
RocksDB), required for submission to a real Flink cluster.

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka cluster address |
| `FLINK_RISK_GROUP_ID` | `fraud-engine-v2` | Consumer group for the main source (accounts source uses `<this>-accounts`) |
| `KAFKA_ALERT_TOPIC` | `fraud_alerts_v2` | Output topic |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | Overrides the properties-file default |
| `SCHEMA_REGISTRY_SUBJECT` | `fraud_alerts_v2-value` | Overrides the properties-file default |

### Run locally (inside the packaged jar, standalone JVM — not a real Flink cluster)

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
SCHEMA_REGISTRY_URL=http://localhost:8081 \
java -cp target/flink-risk-engine-java-1.0-SNAPSHOT.jar com.banking.fraud.RiskScoringJob
```

This runs Flink's local execution environment in-process — fine for a quick smoke
test against your local Docker Compose stack, but not how it should run in Swarm.

### Submit to the Flink cluster (Swarm)

Via the Flink JobManager's REST/CLI, from a node with `flink` CLI access or via the
JobManager container:

```bash
flink run -c com.banking.fraud.RiskScoringJob \
  /path/to/flink-risk-engine-java-1.0-SNAPSHOT.jar
```

Or build the module's own Docker image (`analytics/flink-risk-engine-java/Dockerfile`)
and add it as its own service in `infra/docker-compose.yml`, per the coexistence
plan — it should run as an independent service pointed at the existing
`flink-jobmanager`/`flink-taskmanager`, not replace the Python prototypes' execution
path.

---

## 7. Monitoring — how to tell it's actually working

### Flink Web UI
The JobManager UI (port `8082` per `infra/docker-compose.yml`, or `8081` internally)
shows:
- **Job status**: RUNNING, not FAILED/RESTARTING on a loop (check the restart count —
  the job is configured for 3 retries with a 10s delay; repeated restarts past that
  mean it's crash-looping, not recovering).
- **Checkpoint history**: should complete every ~15s. Repeated checkpoint
  **failures or timeouts** are the earliest sign of a real problem (state growing
  too large, RocksDB I/O pressure, or a slow sink).
- **Backpressure** indicators on the `risk-scoring-processor`/`account-enrichment`
  operators — sustained high backpressure means the scoring logic or the Kafka sink
  can't keep up with input volume.
- **Watermarks** per source — if a source's watermark stalls (stops advancing),
  either that topic has gone quiet beyond the `60s` idleness threshold (expected,
  harmless) or something's stuck upstream (Debezium connector down, topic not
  receiving events — worth checking `connect-init` registered successfully).

### Kafka-level checks

Confirm alerts are actually landing on the output topic:

```bash
docker exec -it <kafka-container> kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic fraud_alerts_v2 \
  --from-beginning
```

(Payloads are Avro via Schema Registry, so a plain console consumer will show
binary — use `kafka-avro-console-consumer` if available, or check via the Schema
Registry / a small consumer that deserializes against the registered schema.)

Check consumer lag to confirm the engine is actually keeping pace with the source
topics:

```bash
docker exec -it <kafka-container> kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --describe --group fraud-engine-v2
```

Growing, never-shrinking lag on `banking.transactions` or `banking.login_events`
means the job is falling behind — check backpressure in the Flink UI next.

### Schema Registry check

Confirm the alert schema actually registered (not just serialized locally):

```bash
curl http://localhost:8081/subjects/fraud_alerts_v2-value/versions
```

An empty/404 response here despite alerts flowing means something's silently
falling back or misconfigured — worth investigating before other services start
depending on this topic's schema.

### Signs specific to this engine's known limitations

- **Alert `customerId` looks like `acct:ACC-...` or `ip:10.x.x.x`** instead of a
  real customer ID — not a bug per se (see §3), but if it's *persistent* for an
  account rather than transient, check that `banking.accounts` CDC events are
  actually flowing (connector registered, topic has messages) — a persistently
  unresolved `acct:` identity usually means the accounts join has nothing to join
  against.
- **Two alerts for what looks like one continuous fraud burst, at different
  severities** — this is a known, not-yet-fixed limitation: alert IDs currently
  include severity, so an escalation from FRAUD -> CRITICAL within the same
  cooldown window can emit a second alert rather than updating the first in place.
  Not incorrect data, just not deduplicated the way the design intends.

---

## 8. Known open items (carried over from development, not yet resolved)

- Alert-ID composition includes severity, which can produce two alerts on a
  FRAUD->CRITICAL escalation instead of one updated alert (see above).
- The `acct:`/`ip:` fallback identities are permanent for `api_requests` (there's no
  richer identity available in that schema) but should become rare for
  `transactions` once the accounts join is consistently warmed up — worth
  confirming in practice, not just in theory.
- No end-to-end proof yet against the live Swarm stack with real Debezium +
  simulator traffic — everything verified so far is unit/harness/MiniCluster level.
  Recommended next step: bring up the stack, trigger a simulator scenario (e.g.
  `wire_fraud`), and confirm an alert lands on `fraud_alerts_v2` with the expected
  severity end to end.