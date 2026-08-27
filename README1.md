# 🛡️ The Banking Fraud Platform

[![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org)
[![Apache Flink](https://img.shields.io/badge/Apache_Flink-E6522C?style=for-the-badge&logo=apache-flink&logoColor=white)](https://flink.apache.org)
[![OpenSearch](https://img.shields.io/badge/OpenSearch-005EB8?style=for-the-badge&logo=opensearch&logoColor=white)](https://opensearch.org)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com)

The **Banking Fraud Platform** is a self-contained, containerized distributed systems simulation of a real-time core banking system paired with an advanced, event-driven fraud-detection pipeline. 

Rather than checking transactions in a nightly batch process or blocking transactions synchronously, this repository demonstrates how modern fintechs, payment processors, and banks detect fraudulent behavior in **near-real-time** as events unfold. It serves as both a complete technical implementation and an educational case study in building stateful, streaming-first backend systems.

---

## 🏗️ System Architecture & Data Flow

The platform resolves the fundamental tension in real-time fraud detection: the need for **historical context** (which typically requires expensive queries) available at the exact millisecond a **new event arrives** (which requires low-latency processing). 

To solve this, the platform implements a **parallel Two-Path Design** that unifies real-time application behavior and database changes:

```
                  ┌──────────────────────────────────────────┐
                  │          Traffic Simulator               │
                  │  (20 Persona-Driven Customers + Attacks) │
                  └────────────────────┬─────────────────────┘
                                       │ HTTP
                                       ▼
                  ┌──────────────────────────────────────────┐
                  │       Core Banking API (FastAPI)         │
                  └──────────┬────────────────────┬──────────┘
                             │ DB Writes          │ Telemetry
                             ▼                    ▼
   ┌───────────────────────────┐         ┌───────────────────────────┐
   │ PostgreSQL 15 Database    │         │      api_requests         │
   │  (System of Record)       │         │      (Kafka Topic)        │
   └─────────────┬─────────────┘         └────────────┬──────────────┘
                 │ WAL Tail (logical cdc)             │
                 ▼                                    │
   ┌───────────────────────────┐                      │
   │ Debezium / Kafka Connect  │                      │
   └─────────────┬─────────────┘                      │
                 │                                    │
                 ▼                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│                  Apache Kafka Event Backbone                       │
│      (banking.accounts, banking.transactions, banking.logins)      │
└────────────────┬──────────────────────────┬────────────────────────┘
                 │                          │
                 ▼ Consumes (Active-Active) ▼ Consumes
   ┌───────────────────────────┐   ┌──────────────────────────┐
   │    Apache Flink Engine    │   │   Secondary Risk Engine  │
   │       (Java Stream)       │   │      (Python Stream)     │
   └─────────────┬─────────────┘   └────────────┬─────────────┘
                 │ FraudAlerts                  │ FraudAlerts
                 ▼                              ▼
┌────────────────────────────────────────────────────────────────────┐
│                    Alert Aggregator Worker                         │
│     (Deduplicates by identity key into consolidated incidents)     │
└────────────────────────────────┬───────────────────────────────────┘
                                 ▼
                     PostgreSQL (incidents table)
                                 │
                                 ▼
                     Analyst Incident Manager API
```

### 1. The Application Path
The **FastAPI Gateway** handles incoming HTTP requests (logins, transfers, beneficiary updates). As an asynchronous side effect of each request, it publishes a structured telemetry event to a Kafka topic named `api_requests` using an Avro schema governed by the **Schema Registry**. This path captures events that might not touch the database (e.g., failed endpoints or rapid API probing).

### 2. The CDC (Change Data Capture) Path
Independently of the API, every row-level mutation to critical PostgreSQL tables (`accounts`, `transactions`, `login_events`, `sessions`, `beneficiaries`, and `employee_actions`) is streamed straight out of PostgreSQL’s Write-Ahead Log (WAL) using **Debezium** running on **Kafka Connect**. This captures the absolute ground truth of what was committed, including direct SQL updates or admin operations.

Both paths converge as active-active inputs into our distributed stream-processing risk engines.

---

## 🛠️ Technology Stack

| Technology | Logo | Role in the Platform |
| :--- | :---: | :--- |
| **FastAPI** | <img src="https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png" width="40"/> | High-performance, async-native HTTP front door representing the banking gateway and incident-management interface. |
| **PostgreSQL 15** | <img src="https://www.postgresql.org/media/img/about/press/elephant.png" width="40"/> | The transactional system of record. Configured with `wal_level=logical` to act as the source of truth for Debezium logical replication. |
| **Debezium** | <img src="https://debezium.io/images/debezium-logo-color.svg" width="40"/> | Tails the PostgreSQL Write-Ahead Log (WAL) and publishes database changes to Kafka as structured JSON CDC envelopes without any app overhead. |
| **Apache Kafka** | <img src="https://kafka.apache.org/images/logo.png" width="50"/> | The durable, replayable event backbone, running in modern **KRaft mode** (no ZooKeeper dependency) with 3 partitions per topic. |
| **Confluent Schema Registry** | <img src="https://styles.redditmedia.com/t5_2s7r0/styles/communityIcon_v0e405uizw851.png" width="40"/> | Governs API request telemetry and alert topics with strict Avro schemas, ensuring strict evolutionary compatibility. |
| **Apache Flink** | <img src="https://flink.apache.org/img/logo/png/100/flink_squirrel_100_color.png" width="40"/> | Distributed stateful stream processor (Java) that computes real-time fraud risk profiles using keyed, RocksDB-backed state and checkpointing. |
| **OpenSearch** | <img src="https://opensearch.org/assets/brand/OS_Logo_Circle_Color.svg" width="40"/> | Houses indexed telemetry, login logs, and fraud alerts. Connected via a dedicated python-based ingestion sink daemon. |
| **Prometheus & Grafana** | <img src="https://grafana.com/static/assets/img/downloads/grafana_logo.svg" width="40"/> | Handles the platform-wide observability plane. Tracks API latency histograms, consumer lag metrics, and engine redundancy. |

---

## 📂 Directory Layout

```hl
banking-fraud-platform/
├── .github/workflows/        # GitHub Actions CI/CD deployment pipeline
├── analytics/                # Real-time streaming engines & data sinks
│   ├── flink-risk-engine-java/ # Primary Java Flink risk scoring engine
│   ├── kafka-stream-scripts/ # Python secondary risk engine (active-active)
│   ├── engine-watchdog/      # Watchdog monitoring primary and secondary liveness
│   └── opensearch/           # Kafka-to-OpenSearch ingestion sink daemon
├── api/                      # Core FastAPI Banking Platform & Analyst Gateway
│   ├── app/                  # Application code, routers, database engines, and workers
│   └── alembic/              # Database migration definitions for incident schemas
├── config/                   # System-wide static connector and index configurations
├── grafana/                  # Grafana dashboards (Observability & Redundancy)
├── infra/                    # Docker Swarm-flavored production topology configuration
├── prometheus/               # Prometheus target scrape configurations
├── simulator/                # synthetic traffic generator and 15-scenario attack engine
└── docker-compose.yml        # Orchestration definition for the 16 local containers
```

---

## 🚀 Getting Started

### Prerequisites
- **Docker** and **Docker Compose** (with at least 8GB of allocated RAM to support the JVM-heavy JVM, Flink, Kafka, and OpenSearch containers).
- **Python 3.12+** (if you wish to run the simulator natively outside container orchestration).

---

### Step 1: Bootstrapping the Platform
The platform uses Docker Compose with health checks to orchestrate a strict tier-based startup sequence (e.g., waiting for PostgreSQL and Kafka to report healthy before starting APIs, connector initialization, and Flink).

```bash
# Clone the repository
git clone https://github.com/calebmuinde4/banking-fraud-platform.git
cd banking-fraud-platform

# Spin up the infrastructure stack (this may take 2-4 minutes on first boot)
docker compose up -d --build
```

### Step 2: Registering the CDC Connector
Once Kafka Connect starts, the platform automatically triggers `connect-init` to register the Debezium connector. If you ever need to register or re-create the connector manually, use Kafka Connect's REST API:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d @debezium-postgres-connector.json \
  http://localhost:8083/connectors
```

---

### Step 3: Tailing the Event Stream
To verify that database writes are successfully streaming into Kafka as CDC events, you can attach a console consumer directly to the Kafka container:

```bash
# Verify Kafka topics are created
docker exec -it bank_kafka kafka-topics --bootstrap-server localhost:29092 --list

# Tail raw login CDC events
docker exec -it bank_kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic banking.login_events \
  --from-beginning
```

---

### Step 4: Forcing Direct Database Mutations (CDC Proof)
To prove that Change Data Capture captures data independently of the API layer, run a raw SQL transaction directly in the PostgreSQL container and watch your Kafka consumer:

```bash
docker exec -it bank_postgres psql -U postgres_admin -d banking_db \
  -c "UPDATE accounts SET balance = balance + 250.00 WHERE account_id = 'ACC-GEN00000';"
```
*Observe that the update is published as a CDC message to `banking.accounts` within milliseconds, bypassing all application-level middleware.*

---

## 😈 Synthetic Traffic & Attack Library

The repository includes a robust **Simulator & Attack Engine** (`/simulator`) that serves as a synthetic ground-truth oracle. It models continuous everyday banking traffic and injects **15 distinct fraud patterns** on a scheduled loop or on-demand:

* **Weighted Personas**: Evaluates risk bounds by modeling distinct student, business, professional, and retiree profiles with specific transfer size and velocity limits.
* **15 Simulated Fraud Scenarios** (trip conditions mapped out in Flink):
  1. `run_credential_stuffing`: 25 rapid failed logins from a single country/device (`MULTIPLE_FAILED_LOGINS` indicator).
  2. `run_account_takeover`: Victim login from a new device/country, followed immediately by transfer (`NEW_COUNTRY` + `IMPOSSIBLE_TRAVEL`).
  3. `run_wire_fraud`: Large value wire transfers exceeding standard threshold limits (`LARGE_TRANSFER_AMOUNT`).
  4. `run_insider_threat_scraping`: Employee credentials crawling client profiles in rapid succession.
  5. `run_mule_structuring_network`: High-frequency transfer chain loops across multiple accounts (`VELOCITY_VIOLATION` + shared rings).
  6. *And 10 other realistic exploit patterns (MFA-fatigue, API enumeration, SIM-swaps, etc.)*

### Triggering Attacks On-Demand
You can manually run specific attack campaigns against the running platform using the API gateway:

```bash
# Trigger an immediate wire fraud attack scenario
curl -X POST http://localhost:8000/api/demo/scenarios/run?scenario=WIRE_FRAUD
```

---

## 📈 Observability & Analyst Surfaces

The platform provides a complete monitoring stack to visualize infrastructure health and track detection accuracy:

* **FastAPI Gateway Endpoint**: `http://localhost:8000` (Swagger UI available at `/docs`).
* **OpenSearch Dashboards**: Access `http://localhost:5601` to run ad-hoc threat-hunting queries or visualize aggregated alerts over the indexed telemetry.
* **Flink Web Dashboard**: Access `http://localhost:8082` to monitor job health, checkpoint durations, and RocksDB task states.
* **Prometheus Console**: Reachable at `http://localhost:9090` to execute PromQL queries.
* **Grafana Dashboards**: Access `http://localhost:3000` to view pre-built dashboards:
  * **Fraud Engine Redundancy**: Track liveness metrics, primary/secondary active-active states, and aggregate alert signals.
  * **Cluster Resource Usage**: Monitor per-container and host CPU, memory, network, and disk I/O metrics pulled via `node-exporter` and `cAdvisor`.

---

## 🛡️ Production Perspectives & Trade-offs

This codebase is a highly realistic demonstration of enterprise architecture designed for a homelab environment. When deploying a system of this nature in a true production environment, consider the following trade-offs:

1. **Active-Active Redundancy**: The platform runs the primary JVM-based Flink engine and a lightweight Python secondary engine concurrently. If Flink crashes, the Python engine acts as an immediate safety net. In production, the Python engine’s local in-memory dict state would be hardened with distributed caching or snapshot persistence to eliminate state loss on restart.
2. **High Availability Datastores**: PostgreSQL, Kafka, and OpenSearch run as single containers. Real banking systems deploy multi-broker Kafka clusters with replication factors $\ge 3$ and PostgreSQL read-replicas with dedicated logical replication slots to isolate analytical loads from transactional write paths.
3. **Fail-Open vs. Fail-Closed**: In this testing simulator, components are built to **fail-open** (e.g., JIT provisioning placeholder accounts on unexpected transfers or gracefully skipping failed lookup queries). In a production bank, write-path transactions must **fail-closed** to protect actual funds and preserve referential database integrity.
