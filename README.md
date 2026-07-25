# Banking Fraud Platform

A local banking fraud simulation platform with core banking API, Kafka/Postgres infrastructure, and a transaction simulator.

## Quick Start

### 1. Start the container stack

From the project root:

```powershell
cd F:\DaTech-kafka\banking-fraud-platform
docker compose up -d
```

The stack starts:
- `postgres` on `localhost:5433`
- `kafka` on `localhost:29092`
- `connect` on `localhost:8083`

The root `.env` file contains required settings for `docker compose`.

To stop the stack:

```powershell
docker compose down
```

### Seed the database

After the containers are running, seed a fresh database from the `api` folder:

```powershell
cd api
venv\Scripts\activate
python seed_db.py
```


### 2. Run the API

From the `api` folder:

```powershell
cd api
venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Alternatively, run using the virtualenv's python (safer on Windows):

```powershell
venv\Scripts\python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at:

- `http://localhost:8000`

### 3. Run the fraud engine

The Flink fraud engine is included in the compose stack and can be launched with:

```powershell
cd F:\DaTech-kafka\banking-fraud-platform
docker compose up -d fraud-engine
```

It consumes the CDC-backed Kafka streams, emits alerts to `fraud_alerts_v2`, and uses idleness-aware watermarks plus checkpointed state for resilience.

### 4. Run the simulator

From the `simulator` folder:

```powershell
cd simulator
venv\Scripts\activate
pip install -r requirements.txt
python trigger.py credential_stuffing
```

Replace `credential_stuffing` with any available scenario name.

#### Example simulator scenarios

- `credential_stuffing`
- `account_takeover`
- `wire_fraud`
- `insider_scraping`
- `auth_abuse`
- `aml_smurfing`
- `api_enumeration`
- `social_engineering`
- `ddos_flood`
- `known_threat_device`
- `privilege_abuse`
- `malware_hijack`
- `money_laundering`
- `fraud_ring`
- `ransomware`

## Project Structure

```text
.
├── .env
├── .gitignore
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── seed_db.py
│   └── app/
│       ├── main.py
│       ├── core/
│       ├── api/
│       ├── models/
│       └── schemas/
├── backend/
│   └── app/
├── debezium-postgres-connector.json
├── docker-compose.yml
├── k8s/
└── simulator/
    ├── Dockerfile
    ├── requirements.txt
    ├── trigger.py
    └── app/
```

# Phase 3: Change Data Capture (CDC) Pipeline Documentation

This document outlines the implementation details and verification procedures for the infrastructure-level **Change Data Capture (CDC)** pipeline. This pipeline uses **Debezium** and **Kafka Connect** to stream low-latency, row-level mutations directly from PostgreSQL's Write-Ahead Log (WAL) into Kafka, bypassing application logic dependencies.

## 1. Implementation Architecture

The CDC pipeline captures `INSERT`, `UPDATE`, and `DELETE` mutations on critical core banking tables. To keep the broker topics organized and cleanly structured, a Single Message Transformation (SMT) is applied inside Kafka Connect to strip the database schema name out of the default generated topic name.

### Component Breakdown

- **PostgreSQL Engine:** Configured with logical replication (`wal_level=logical`) to publish state changes to a replication slot.
- **Debezium PostgreSQL Connector:** Polling the replication slot inside the Kafka Connect JVM, converting WAL streams into structured JSON events.
- **RegexRouter SMT:** Intercepts out-of-the-box topics matching `banking.public.<table_name>` and automatically rewrites them to clean production targets matching `banking.<table_name>`.

## 2. Infrastructure Configuration

### Step A: PostgreSQL Engine Configuration (`docker-compose.yml`)

To initialize the Logical Decoding plugin (`pgoutput`), parameters must be injected during container initialization. The modified service block handles this via the `command` directive:

```yaml
services:
  postgres:
    image: postgres:15-alpine
    container_name: bank_postgres
    command: 
      - "postgres"
      - "-c"
      - "wal_level=logical"
      - "-c"
      - "max_replication_slots=4"
      - "-c"
      - "max_wal_senders=4"
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "${POSTGRES_PORT}:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    restart: unless-stopped
```

### Step B: Connector Deployment Registry (`debezium-postgres-connector.json`)

Located at `F:\DaTech-kafka\banking-fraud-platform\debezium-postgres-connector.json`, this payload isolates high-value fraud vector targets and applies routing rules:

```json
{
  "name": "banking-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres_admin",
    "database.password": "SecureBankPassword2026!",
    "database.dbname": "banking_db",
    "plugin.name": "pgoutput",
    
    "topic.prefix": "banking",
    "table.include.list": "public.login_events,public.sessions,public.accounts,public.transactions,public.beneficiaries,public.employee_actions",
    
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "decimal.handling.mode": "double",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",

    "transforms": "RouteTable",
    "transforms.RouteTable.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.RouteTable.regex": "banking\\.public\\.(.*)",
    "transforms.RouteTable.replacement": "banking.$1"
  }
}
```

## 3. Operations & Lifecycle Commands

Execute all operational commands below using a standard host terminal environment (PowerShell/Command Prompt) within the project's root development workspace.

### Command 1: Register the CDC Connector

Submit the JSON template to the Kafka Connect REST API boundary layer using the native Windows cURL binary:

```powershell
curl.exe -X POST -H "Content-Type: application/json" -d @F:\DaTech-kafka\banking-fraud-platform\debezium-postgres-connector.json http://localhost:8083/connectors
```

### Command 2: Verify Active Kafka Topics

Interrogate the Confluent broker instance internally to confirm that clean, transformed topics exist without `.public.` schema markers:

```powershell
docker exec -it bank_kafka kafka-topics --bootstrap-server localhost:29092 --list
```

### Command 3: Tail the Live Mutation Stream

Open a dedicated, persistent consumer window to observe incoming row modifications. For example, to track the `accounts` ledger space:

```powershell
docker exec -it bank_kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic banking.accounts --from-beginning
```

### Command 4: Trigger a Simulation Mutation

To force state transitions and test end-to-end delivery performance, run execution scripts or execute SQL updates inside the runtime database instance:

```powershell
docker exec -it bank_postgres psql -U postgres_admin -d banking_db -c "UPDATE accounts SET balance = balance + 250.00 WHERE account_id = 'ACC-GEN00000';"
```

## 4. Expected Payload Structure

Successful verification logs will yield structured JSON blocks containing operational event fields (`"op"`: `"r"` for reading snapshots, `"c"` for record instantiation, `"u"` for updates) along with contextual validation frames:

```json
{
  "before": null,
  "after": {
    "account_id": "ACC-376414",
    "customer_id": "cust_external_mule",
    "balance": 15013.95,
    "currency": "USD",
    "status": "ACTIVE",
    "created_at": 1781913231931852
  },
  "source": {
    "version": "2.4.2.Final",
    "connector": "postgresql",
    "name": "banking",
    "ts_ms": 1781920044118,
    "snapshot": "true",
    "db": "banking_db",
    "schema": "public",
    "table": "accounts"
  },
  "op": "r",
  "ts_ms": 1781920055239
}
```

## Notes

- Make sure local ports `5433`, `9092`, and `8083` are available.
- For Windows, use `venv\Scripts\activate` to activate the Python venv.
- Do not commit production secrets.

## Open Source Governance

This repository is owned by `Caleb-Muinde83` and may be maintained by the Datech Community (`https://github.com/DatechCommunity`).

- `LICENSE` establishes the project as open source.
- `CONTRIBUTING.md` explains how the community can propose changes and submit pull requests.
- Maintainers and collaborators should be granted admin/write access to review and merge PRs.

************************************************************
*****************************************************************
**Documentation Summary**

I updated `banking-fraud-platform` from a mostly local single-node Compose project toward a deployable Docker Swarm/Tailscale homelab layout, while preserving the existing source tree and avoiding destructive cleanup.

The main goal was to implement the structure from your deployment plan inside the real project folder, not in a separate `12-real-world-projects/...` path.

**New Deployment Structure**

Added these folders and files:

```text
banking-fraud-platform/
├── .github/
│   └── workflows/
│       └── deploy.yml
├── infra/
│   └── docker-compose.yml
├── config/
│   ├── kafka-connect/
│   │   └── postgres-source.json
│   ├── opensearch/
│   │   └── mappings.json
│   └── schemas/
│       └── api_request.avsc
```

Also added a repository-root workflow:

```text
.github/
└── workflows/
    └── deploy-banking-fraud-platform.yml
```

This root workflow matters because the actual Git repository root is:

```text
F:\DaTech-kafka
```

not:

```text
F:\DaTech-kafka\banking-fraud-platform
```

GitHub Actions only detects workflows under the Git root `.github/workflows/`.

**Swarm Stack Added**

Created:

```text
banking-fraud-platform/infra/docker-compose.yml
```

This defines the distributed Docker Swarm deployment across your Tailscale nodes:

```text
k3s-master
- PostgreSQL
- Banking API
- Simulator
- Kafka Connect

k3s-worker-2
- Kafka
- Schema Registry

k3s-worker-3
- OpenSearch
- Flink JobManager
- Flink TaskManager
```

The stack uses an overlay network:

```yaml
fraud_network:
  driver: overlay
  attachable: true
```

and Swarm placement constraints such as:

```yaml
deploy:
  placement:
    constraints:
      - node.hostname == k3s-master
```

**Debezium CDC Config Added**

Created:

```text
banking-fraud-platform/config/kafka-connect/postgres-source.json
```

This config registers a Debezium PostgreSQL connector for the core banking tables:

```text
login_events
sessions
accounts
transactions
beneficiaries
employee_actions
```

It emits events under the Kafka topic prefix:

```text
banking
```

Example output topics:

```text
banking.login_events
banking.transactions
banking.accounts
```

**OpenSearch Mapping Config Added**

Created:

```text
banking-fraud-platform/config/opensearch/mappings.json
```

This documents intended mappings for:

```text
api_requests
login_events
alerts
```

The mappings define fields like timestamps, customer IDs, IP addresses, endpoints, status codes, alert IDs, and risk scores.

**Schema Organization Updated**

Copied:

```text
banking-fraud-platform/schemas/api_request.avsc
```

to:

```text
banking-fraud-platform/config/schemas/api_request.avsc
```

The original file was left in place for backward compatibility with existing local scripts.

Code was updated to prefer the new path:

```text
config/schemas/api_request.avsc
```

and fall back to the old path:

```text
schemas/api_request.avsc
```

**GitHub Actions Added**

Added project-local workflow:

```text
banking-fraud-platform/.github/workflows/deploy.yml
```

Added actual repo-root workflow:

```text
.github/workflows/deploy-banking-fraud-platform.yml
```

The repo-root workflow:

1. Runs on your self-hosted runner.
2. Builds the API image:

```bash
docker build -f api/Dockerfile -t banking-api:latest .
```

3. Builds the simulator image:

```bash
docker build -f simulator/Dockerfile -t banking-simulator:latest .
```

4. Deploys the Swarm stack:

```bash
docker stack deploy -c infra/docker-compose.yml fraud_platform
```

5. Waits for services to come up.
6. Registers the Debezium connector through Kafka Connect.

**Dockerfiles Completed**

The API and simulator Dockerfiles were previously empty.

Updated:

```text
banking-fraud-platform/api/Dockerfile
banking-fraud-platform/simulator/Dockerfile
```

The API Dockerfile now:

- Uses `python:3.12-slim`
- Installs `api/requirements.txt`
- Copies `api/app`
- Copies `api/seed_db.py`
- Copies `config`
- Copies `schemas`
- Starts FastAPI with Uvicorn on port `8000`

The simulator Dockerfile now:

- Uses `python:3.12-slim`
- Installs simulator requirements
- Copies simulator source
- Runs the simulator main engine

**Runtime Configuration Improvements**

Updated several files so services work both locally and inside Swarm.

Environment-variable support was added for:

```text
KAFKA_BOOTSTRAP_SERVERS
SCHEMA_REGISTRY_URL
API_REQUEST_SCHEMA_PATH
OPENSEARCH_HOST
OPENSEARCH_PORT
OPENSEARCH_USER
OPENSEARCH_PASSWORD
API_URL
BANKING_API_URL
```

Files updated include:

```text
api/app/middleware/kafka_producer.py
analytics/kafka-stream-scripts/rules_engine.py
analytics/kafka-stream-scripts/risk_scoring_engine.py
analytics/opensearch/opensearch_sink.py
analytics/apache-flink-scripts/flink_risk_engine.py
simulator/app/main.py
simulator/app/attacks/scenarios.py
simulator/app/actors/employee.py
```

This replaces hardcoded local assumptions like:

```text
localhost:29092
localhost:8081
127.0.0.1:8000
localhost:9200
```

with environment-aware defaults.

**Simulator Fix**

Fixed a method mismatch in:

```text
simulator/app/actors/customer.py
```

The simulator loop was calling:

```python
perform_internal_action(...)
```

on `AsyncEmployeeActor`, but the class only had:

```python
perform_action(...)
```

I added `perform_internal_action(...)` so employee simulation loops can run correctly.

**Dependency Fix**

Updated:

```text
api/requirements.txt
```

Added:

```text
email-validator
```

because your Pydantic schemas import `EmailStr`, which requires `email-validator` at runtime.

**Git Ignore Update**

Updated:

```text
banking-fraud-platform/.gitignore
```

Added:

```gitignore
persisted-volumes/
**/persisted-volumes/
```

This prevents Postgres and OpenSearch runtime data from being committed.

I did not delete:

```text
backend/
api/venv/
simulator/venv/
persisted-volumes/
```

I only made sure generated/runtime folders are ignored going forward.

**Validation Performed**

I ran Python syntax checks on the changed Python files:

```bash
python -m py_compile ...
```

Result: passed.

I also validated the new Swarm compose file:

```bash
docker compose -f banking-fraud-platform\infra\docker-compose.yml config
```

Result: passed.

Docker Compose gave a warning that `version: "3.8"` is obsolete in newer Compose, but the file still renders correctly. It was kept because Swarm stack examples commonly use that format.

**Suggested Commit Message**

```text
feat: add Tailscale Swarm deployment for banking fraud platform

- Add distributed Docker Swarm stack under infra/docker-compose.yml
- Add GitHub Actions deployment workflow for self-hosted runner
- Add Debezium PostgreSQL CDC connector config
- Add OpenSearch index mappings and relocated Avro schema config
- Complete API and simulator Dockerfiles for local image builds
- Make Kafka, Schema Registry, OpenSearch, and API URLs env-configurable
- Fix simulator employee actor lifecycle method mismatch
- Add email-validator dependency required by Pydantic EmailStr
- Ignore persisted runtime volumes to keep service data out of Git
```

***********************************
I'll scan the repo for Dockerfiles and docker build commands to enumerate custom images, then list each with a short description and provide push commands. I'll run a quick workspace search.

Searched for files matching `**/Dockerfile`, 2 matches

Searched for files matching `**/*.Dockerfile`, 1 match

Searching for text `docker build -t|docker build -f|image: banking-postgres|image: banking-api|banking-simulator|banking-postgres` (`DaTech-kafka` · `banking-fraud-platform/**`)

Read [](file:///f%3A/DaTech-kafka/banking-fraud-platform/api/Dockerfile)

Read [](file:///f%3A/DaTech-kafka/banking-fraud-platform/simulator/Dockerfile)

Read [](file:///f%3A/DaTech-kafka/banking-fraud-platform/infra/postgres.Dockerfile)

Searching for text `docker build -t` (`DaTech-kafka` · `banking-fraud-platform/**`)

I'll list the custom images found, give one-line descriptions, then show copy-pasteable commands to build, tag and push them to a registry (supports Docker Hub or GHCR via env vars).

Custom images in this project
- `banking-api:latest` — API server (FastAPI / Uvicorn). Built from `api/Dockerfile` and serves the platform API on port 8000.  
- `banking-simulator:latest` — Traffic/attack simulator. Built from `simulator/Dockerfile`, runs the simulator process.  
- `banking-postgres:latest` — Custom Postgres image (base: `debezium/postgres:15-alpine`) containing postgres-init.sh baked in for Swarm-safe initialization. Built from `infra/postgres.Dockerfile`.

Generic push workflow (pick registry and set env vars)
- Set your registry/owner once:
  - Docker Hub example:
    - `REGISTRY=docker.io`
    - `OWNER=<your-dockerhub-username>`
  - GitHub Container Registry (GHCR) example:
    - `REGISTRY=ghcr.io`
    - `OWNER=<your-github-username>`

Commands (run from project root)
1) Build (if not already built locally)
- docker build -t banking-api:latest ./api
- docker build -t banking-simulator:latest ./simulator
- docker build -f infra/postgres.Dockerfile -t banking-postgres:latest infra/

2) Tag for your registry
- docker tag banking-api:latest     $REGISTRY/$OWNER/banking-api:latest
- docker tag banking-simulator:latest $REGISTRY/$OWNER/banking-simulator:latest
- docker tag banking-postgres:latest  $REGISTRY/$OWNER/banking-postgres:latest

3) Login (example)
- Docker Hub:
  - docker login --username "$OWNER"
- GHCR:
  - echo "$GHCR_TOKEN" | docker login ghcr.io -u "$OWNER" --password-stdin

4) Push
- docker push $REGISTRY/$OWNER/banking-api:latest
- docker push $REGISTRY/$OWNER/banking-simulator:latest
- docker push $REGISTRY/$OWNER/banking-postgres:latest

Optional: push all in a single loop
- REGISTRY=ghcr.io; OWNER=Caleb-Muinde83
- for name in banking-api banking-simulator banking-postgres; do \
    docker tag $name:latest $REGISTRY/$OWNER/$name:latest && \
    docker push $REGISTRY/$OWNER/$name:latest; \
  done

Notes / recommendations
- For GHCR you must create a PAT with `write:packages` and set it as `GHCR_TOKEN` in CI or your shell when running `docker login`.
- Consider using repository-scoped image names (e.g., `$REGISTRY/$OWNER/banking-fraud-platform/banking-api:latest`) if you want images grouped under the repo.
- If the cluster pulls images from a registry, update `infra/docker-compose.yml` to reference the pushed image names (fully qualified) and remove local-build-only assumptions.

Want me to:
- (A) push these now to a specific registry (provide `REGISTRY`, `OWNER`, and credentials/where to read the token), or  
- (B) update `infra/docker-compose.yml` to point at the fully-qualified registry image names after push?

************************************************
Here's a clean, reusable guide documenting the exact process you followed, including the issue you encountered and how you fixed it.

### Push Custom Docker Images to GitHub Container Registry (GHCR)

This guide explains how to build custom Docker images locally and publish them to GitHub Container Registry (GHCR).

---

# 1. Activate the Python Virtual Environment

```bash
source /f/DaTech-kafka/.venv/Scripts/activate
```

Navigate to the project.

```bash
cd banking-fraud-platform
```

---

# 2. Set Registry Variables

Set the registry endpoint.

```bash
export REGISTRY=ghcr.io
```

Initially the owner was set as:

```bash
export OWNER=Caleb-Muinde83
```

This later caused an error because **GHCR repository names must be lowercase**.

The correct value is:

```bash
export OWNER=caleb-muinde83
```

---

# 3. Build the Images

### Banking API

```bash
docker build -t banking-api:latest ./api
```

### Banking Simulator

```bash
docker build -t banking-simulator:latest ./simulator
```

### PostgreSQL Image

```bash
docker build \
    -f infra/postgres.Dockerfile \
    -t banking-postgres:latest \
    infra/
```

After this you should have three local images:

```
banking-api:latest

banking-simulator:latest

banking-postgres:latest
```

---

# 4. Tag the Images

Tag each image so Docker knows where it will be pushed.

```bash
docker tag banking-api:latest \
    $REGISTRY/$OWNER/banking-api:latest

docker tag banking-simulator:latest \
    $REGISTRY/$OWNER/banking-simulator:latest

docker tag banking-postgres:latest \
    $REGISTRY/$OWNER/banking-postgres:latest
```

---

# Common Error

Using an uppercase GitHub username produces:

```text
invalid reference format:
repository name must be lowercase
```

Example:

```text
ghcr.io/Caleb-Muinde83/banking-api
```

### Fix

Use a lowercase owner name.

```bash
export OWNER=caleb-muinde83
```

Then rerun the `docker tag` commands.

---

# 5. Login to GHCR

### Attempt Using Environment Variable

```bash
echo "$GHCR_TOKEN" | docker login ghcr.io \
    -u "$OWNER" \
    --password-stdin
```

This produced:

```text
cannot perform an interactive login from a non-TTY device
```

### Solution (Git Bash on Windows)

Use **winpty**:

```bash
winpty docker login ghcr.io -u "$OWNER"
```

Enter your GitHub Personal Access Token when prompted.

Successful login:

```text
Login Succeeded
```

---

# 6. Push All Images

Push each image individually or use a loop.

```bash
for name in banking-api banking-simulator banking-postgres; do
    docker push $REGISTRY/$OWNER/$name:latest
done
```

Docker uploads each image.

Example output:

```
latest: digest: sha256:...
```

This confirms the image has been published successfully.

---

# Published Images

The following images are now available in GitHub Container Registry:

```
ghcr.io/caleb-muinde83/banking-api:latest

ghcr.io/caleb-muinde83/banking-simulator:latest

ghcr.io/caleb-muinde83/banking-postgres:latest
```

---

# Verify Images

List local images:

```bash
docker images
```

Inspect an image:

```bash
docker inspect ghcr.io/caleb-muinde83/banking-api:latest
```

Pull from GHCR:

```bash
docker pull ghcr.io/caleb-muinde83/banking-api:latest
```

---

# Summary

```bash
# Activate environment
source /f/DaTech-kafka/.venv/Scripts/activate

# Enter project
cd banking-fraud-platform

# Registry variables
export REGISTRY=ghcr.io
export OWNER=caleb-muinde83

# Build images
docker build -t banking-api:latest ./api
docker build -t banking-simulator:latest ./simulator
docker build -f infra/postgres.Dockerfile -t banking-postgres:latest infra/

# Tag images
docker tag banking-api:latest $REGISTRY/$OWNER/banking-api:latest
docker tag banking-simulator:latest $REGISTRY/$OWNER/banking-simulator:latest
docker tag banking-postgres:latest $REGISTRY/$OWNER/banking-postgres:latest

# Login
winpty docker login ghcr.io -u "$OWNER"

# Push
for name in banking-api banking-simulator banking-postgres; do
    docker push $REGISTRY/$OWNER/$name:latest
done
```

This process builds your three custom images, tags them for GitHub Container Registry, authenticates with GHCR, and publishes them under the `caleb-muinde83` namespace.


*****************************
Back in Your Terminal
Copy the generated token immediately (GitHub will never show it to you again).

Go back to your Git Bash window where it says Password:.

Right-click inside the terminal window and select Paste (or press Shift + Insert).





















----------------------------------------------------------------------------------------
model 1
Based on the plan's structure, here's where things stand:

**Met / underway**

- **§0 Coexistence** — ✅ New module lives at `analytics/flink-risk-engine-java/`, fully separate from the existing Python prototypes, which weren't touched. Swarm service entries and `_v2` topics were added to both compose files rather than colliding with existing config.
- **Phase 1 — Contracts (partial)** — `RiskScoringConfig.java` + `risk-scoring.properties` give you config-driven weights, matching the "no broadcast state in v1" decision. `RiskEvent.java` and `FraudAlert.java` establish the event/alert shapes in code.
- **Phase 3 — Stateful Scoring (partial)** — `RiskProfileState.java` + `RiskScoringProcessor.java` implement keyed per-customer state, threshold crossing, and cooldown-based suppression — this is the core of the score-to-alert contract, though it's worth confirming it matches the exact rule (deterministic `alert_id`, escalation-in-place rather than a new alert) rather than just "don't re-alert during cooldown."
- **Phase 4 — Threshold & Alerting (partial)** — threshold logic and cooldown suppression exist in the processor.
- **Build/test loop** — `mvn test` passes (3 tests, scoring config/thresholds/bounded state), `mvn package` produces a working jar. This is real, verified progress, not just scaffolding.
- **Deploy scaffolding** — Dockerfile and README exist for the new service, and a Swarm service block was wired in.

**Not yet met — still open** do the following next

- **Debezium envelope unwrapping** — no CDC source integration yet; this was flagged as the "do this first" item and hasn't been validated against real CDC output.
- **Kafka source/sink wiring + Avro serialization** — explicitly called out as the next step, not done yet. Right now the job has scoring logic but isn't consuming from or producing to real topics.
- **Watermarks + idleness detection** — not implemented.
- **Idempotency/dedup design** (stable dedup key from LSN/offset, deterministic `alert_id`) — not confirmed in what's built; the cooldown suppression alone isn't the same as the dedup contract from the plan.
- **RocksDB state backend, checkpointing, TTL/decay** — not configured yet (Phase 6 items).
- **Schema Registry registration** — schemas exist as Java classes but haven't been registered.
- **Integration/replay tests** — only unit tests exist so far; no test against the simulator's real event sequences yet.

**Net position:** you're solidly through the early part of Phase 1 and into Phase 3 for the in-memory scoring logic, with a verified, compiling, tested module. The next real milestone — wiring actual Kafka I/O with envelope unwrapping and idleness-aware watermarks — is the one to tackle before this is anything more than a well-tested scoring function running in isolation.


******************************
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


**************************************************

# 4. `check_port_8000.ps1`

Place this script wherever is most convenient (for example, in the repository root).

This is not a code fix—there is no code-based solution for a stray process occupying the port. Instead, it serves as a quick pre-flight check that takes only a few seconds to run before starting the simulator.

## Run the Script

```powershell
powershell -ExecutionPolicy Bypass -File check_port_8000.ps1
```

## What It Does

The script checks for any process bound specifically to:

```text
127.0.0.1:8000
```

This is the exact conflict pattern that caused the issue previously.

If a matching process is found, the script:

- Displays the process ID (PID).
- Prints the full command line used to launch the process.
- Provides the correctly escaped `taskkill` command immediately.

This avoids the usual `MINGW64` path-mangling problems and makes it easy to terminate the offending process before launching the simulator.

---

**********************************************************
# Docker Compose Service Overview

When you run:

```bash
docker compose up
```

Docker automatically spins up every container service defined in your `docker-compose.yml` file.

If you then manually start the FastAPI backend outside Docker (for example, by running `uvicorn main:app` in your host terminal), two major problems occur:

- **Port Collision:** Both instances compete for port `8000`.
- **Network Mismatch:** The manually started API connects to `localhost:29092`, while the Dockerized API connects to the internal Docker network at `kafka:29092`.

---

# 1. Automatically Started Services (Managed by Docker)

These services start and remain running inside Docker containers whenever you execute:

```bash
docker compose up -d
```

| Container Name | Service in `docker-compose.yml` | What It Does |
|---|---|---|
| `bank_api` | `api` | **FastAPI Backend:** Handles user logins, transfers, and emits telemetry events via `Kafka` using `RequestLoggerMiddleware`. |
| `bank_opensearch_sink` | `opensearch-sink` | **Ingestion Daemon:** The Python script (`opensearch_sink.py`) that reads messages from Kafka topics (`api_requests`, `banking.login_events`, `fraud_alerts_v2`, `critical_alerts_v2`) and writes them directly into OpenSearch. |
| `bank_postgres` | `postgres` | **Transactional Database:** Stores accounts, logins, and sessions, configured with logical replication (`wal_level=logical`) for CDC. |
| `bank_kafka` | `kafka` | **Event Streaming Broker:** Manages topics and buffers raw streaming messages across all partitions. |
| `bank_schema_registry` | `schema-registry` | **Confluent Schema Registry:** Stores and validates Avro schemas for structured payloads (for example, `api_requests` and `fraud_alerts_v2`). |
| `bank_debezium` | `debezium` | **CDC Connector:** Captures PostgreSQL database mutations in real time and publishes them to Kafka topics such as `banking.login_events`. |
| `bank_opensearch` | `opensearch` | **Search & Analytics Engine:** Stores indexed documents for fast querying. |
| `bank_opensearch_dashboards` | `opensearch-dashboards` | **Web UI (Port `5601`):** Visualization dashboard for viewing alerts, logs, and user telemetry. |
| `bank_flink_jobmanager` / `bank_flink_taskmanager` | `flink-jobmanager` / `taskmanager` | **Apache Flink Engine:** Evaluates fraud-detection rules in real time across incoming event streams. |

---

# 2. Manually Started Services & Scripts

These components live outside the core daemon stack and must be executed manually when testing or generating traffic.

| Service / Script | How to Run It | Why It's Manual |
|---|---|---|
| Traffic Simulator (`simulator.py`) | `python simulator.py` (optionally inside a dedicated virtual environment) | Simulates external user traffic by continuously sending synthetic HTTP requests to `http://localhost:8000` to generate logins, transfers, and API activity. |
| Ad-Hoc Query / Test Scripts | `python test_auth.py` or `curl` | Used to trigger edge cases or manually test security and fraud-detection rules against the API endpoint. |
| Flink Job Submission (if not auto-submitted) | `docker exec bank_flink_jobmanager flink run ...` | Submits or updates streaming pipeline JARs or PyFlink scripts on the running Flink cluster. |

---

# Summary Checklist for a Clean System Run

## 1. Start the Infrastructure Stack

```bash
docker compose up -d
```

## 2. Verify Containers Are Healthy

```bash
docker compose ps
```

## 3. Run the Simulator (Only)

```bash
python simulator.py
```

## 4. View Real-Time Data

Open OpenSearch Dashboards in your browser:

```text
https://localhost:5601
```

Use it to query:

- `api_requests`
- `login_events`
- `fraud_alerts_v2`
- `critical_alerts_v2`

---

# Threat Hunting Execution & Validation Report

**Environment:** Banking Fraud & Threat Analytics Platform
**Target Engine:** OpenSearch
**Execution Status:** ✅ **ALL PASSED (4/4 Queries Executed Successfully)**

## Executive Summary

| # | Target Index       | Query Intent                                    | Execution Status | Matches (Hits) | Risk Assessment                                                  |
| - | ------------------ | ----------------------------------------------- | ---------------- | -------------: | ---------------------------------------------------------------- |
| 1 | `employee_actions` | Mass Exfiltration / High-Volume Customer Access | SUCCESS          |          6,525 | 🚨 **HIGH RISK:** Potential insider data harvesting detected     |
| 2 | `employee_actions` | Targeted Audit on Employee `EMP_001`            | SUCCESS          |              0 | 🟢 **CLEAN:** No actions found for target ID (expected)          |
| 3 | `api_requests`     | API Reconnaissance & Failed Requests (`!200`)   | SUCCESS          |              0 | 🟢 **CLEAN:** Zero non-200 API errors in the past hour           |
| 4 | `alerts`           | High & Critical Severity Fraud Event Triaging   | SUCCESS          |             78 | ⚠️ **ACTION REQUIRED:** 78 active alerts requiring account locks |

---

# Detailed Threat Hunting Query Analysis

## Query 1: Mass Exfiltration / Insider Threat Aggregation

### Purpose

Identifies employees viewing an unusually high volume of unique customer accounts within a 24-hour window, sorting by unique customer cardinality to uncover potential insider data scraping.

### OpenSearch DSL Query

```json
GET /employee_actions/_search
{
  "size": 0,
  "query": {
    "range": {
      "timestamp": {
        "gte": "now-24h"
      }
    }
  },
  "aggs": {
    "employees": {
      "terms": {
        "field": "employee_id",
        "size": 10,
        "order": {
          "unique_customers_accessed": "desc"
        }
      },
      "aggs": {
        "unique_customers_accessed": {
          "cardinality": {
            "field": "customer_id"
          }
        },
        "action_breakdown": {
          "terms": {
            "field": "action_type"
          }
        }
      }
    }
  }
}
```

### Execution Findings & Security Analysis

* **Execution Status:** Successful (`took: 1026ms`, `1 shard successful`)
* **Total Matching Documents:** `6,525`

#### Top Anomalous Actors

* `emp_mgr_02`: `3,505` `VIEW_ACCOUNT` operations across `23` unique customer accounts.
* `emp_teller_01`: `3,020` `VIEW_ACCOUNT` operations across `23` unique customer accounts.

#### Threat Assessment

Potential insider threat or compromised employee credentials. Both accounts are performing high-frequency lookup operations across identical customer clusters.

#### Schema Verification

Confirms `customer_id` and `employee_id` are stored as pure keyword types, enabling precise cardinality and terms aggregation without tokenization errors.

---

## Query 2: Targeted Employee Audit (`EMP_001`)

### Purpose

Audits all historical activities for a specific employee identifier (`EMP_001`) within the last 7 days.

### OpenSearch DSL Query

```json
GET /employee_actions/_search
{
  "size": 100,
  "sort": [
    {
      "timestamp": {
        "order": "desc"
      }
    }
  ],
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "employee_id": "EMP_001"
          }
        }
      ],
      "filter": [
        {
          "range": {
            "timestamp": {
              "gte": "now-7d"
            }
          }
        }
      ]
    }
  }
}
```

### Execution Findings & Security Analysis

* **Execution Status:** Successful (`took: 569ms`, `1 shard successful`)
* **Total Matching Documents:** `0`

#### Threat Assessment

Valid execution with zero hits. Analysis of Query 1 indicates active synthetic employee IDs follow the `emp_mgr_XX` and `emp_teller_XX` naming convention rather than `EMP_001`.

#### Schema Verification

Exact-match term filter ran on `employee_id.keyword` without throwing string-matching exceptions.

---

## Query 3: Suspicious API Errors & Probing Detection

### Purpose

Detects potential endpoint fuzzing, credential stuffing, or broken object-level authorization (BOLA) attempts by aggregating non-200 HTTP response status codes grouped by client IP over the last 1 hour.

### OpenSearch DSL Query

```json
GET /api_requests/_search
{
  "size": 0,
  "query": {
    "bool": {
      "must_not": [
        {
          "term": {
            "status_code": 200
          }
        }
      ],
      "filter": [
        {
          "range": {
            "timestamp": {
              "gte": "now-1h"
            }
          }
        }
      ]
    }
  },
  "aggs": {
    "suspicious_ips": {
      "terms": {
        "field": "ip_address",
        "size": 10
      },
      "aggs": {
        "targeted_endpoints": {
          "terms": {
            "field": "endpoint"
          }
        },
        "error_codes": {
          "terms": {
            "field": "status_code"
          }
        }
      }
    }
  }
}
```

### Execution Findings & Security Analysis

* **Execution Status:** Successful (`took: 164ms`, `1 shard successful`)
* **Total Matching Documents:** `0`

#### Threat Assessment

No failed API interactions or suspicious HTTP responses observed in the last 60 minutes. All system API endpoints reported normal HTTP `200 OK` operations.

---

## Query 4: High & Critical Fraud Alerts Triaging

### Purpose

Retrieves real-time alerts emitted by the Flink Streaming Engine filtered for `CRITICAL` or `HIGH` severity levels over the last 24 hours, ordered chronologically to prioritize immediate SOC triage.

### OpenSearch DSL Query

```json
GET /alerts/_search
{
  "size": 50,
  "sort": [
    {
      "event_time": {
        "order": "desc"
      }
    }
  ],
  "query": {
    "bool": {
      "should": [
        {
          "term": {
            "severity": "CRITICAL"
          }
        },
        {
          "term": {
            "severity": "HIGH"
          }
        }
      ],
      "minimum_should_match": 1,
      "filter": [
        {
          "range": {
            "event_time": {
              "gte": "now-24h"
            }
          }
        }
      ]
    }
  }
}
```

### Execution Findings & Security Analysis

* **Execution Status:** Successful (`took: 162ms`, `1 shard successful`)
* **Total Matching Documents:** `78`

#### Top High-Risk Fraud Findings Sample

```json
[
  {
    "alert_id": "custgen11-critical-1784598300000",
    "customer_id": "cust_gen_11",
    "severity": "CRITICAL",
    "cumulative_score": 9020,
    "threshold": 80,
    "trigger_reason": "IMPOSSIBLE_TRAVEL",
    "contributing_indicators": [
      "NEW_COUNTRY",
      "IMPOSSIBLE_TRAVEL",
      "NEW_DEVICE"
    ],
    "action_required": "LOCK_ACCOUNT"
  },
  {
    "alert_id": "custgen4-critical-1784598300000",
    "customer_id": "cust_gen_4",
    "account_id": "ACC-GEN00004",
    "severity": "CRITICAL",
    "cumulative_score": 6922,
    "threshold": 80,
    "trigger_reason": "LARGE_TRANSFER_AMOUNT",
    "contributing_indicators": [
      "IMPOSSIBLE_TRAVEL",
      "NEW_DEVICE",
      "NEW_COUNTRY",
      "LARGE_TRANSFER_AMOUNT"
    ],
    "action_required": "LOCK_ACCOUNT"
  }
]
```

#### Threat Assessment

Active automated threat response required. Multiple customers exhibit high risk scores (for example, score `9020` versus threshold `80`) driven by simultaneous impossible travel and new-device logins.

Both alerts specify:

```text
action_required: LOCK_ACCOUNT
```

---

# Conclusion & Next Actions

## Schema Integrity Confirmed

All OpenSearch indices properly map strings to keyword fields and dates to epoch/ISO timestamps. Aggregations, boolean filters, and range queries perform as expected without performance bottlenecks.

## SOC Incident Escalation

### Incident #1: Insider Risk

Open an internal investigation ticket for employees:

* `emp_mgr_02`
* `emp_teller_01`

Reason: Bulk `VIEW_ACCOUNT` queries across overlapping customer populations.

### Incident #2: Automated Fraud

Confirm execution of account-lock workflows (`LOCK_ACCOUNT`) for the following customer accounts:

* `cust_gen_11`
* `cust_gen_4`

Reason: High-risk fraud indicators, including:

* Impossible travel
* New device access
* New country logins
* Large transfer amounts
