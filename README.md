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

### 3. Run the simulator

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