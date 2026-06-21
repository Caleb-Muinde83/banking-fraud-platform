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
