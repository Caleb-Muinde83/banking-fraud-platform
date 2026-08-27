# Real-Time Core Banking, CDC Streaming & Fraud Analytics Platform

> **Production-Grade Banking Fraud Platform.**
>
> This documentation incorporates all system subsystems (FastAPI Gateway, Async PostgreSQL, Debezium CDC, Apache Flink Java Risk Engine, OpenSearch Threat Analytics, Prometheus/Grafana Observability, Attack Scenario Simulator, and Swarm/GHCR Deployment Pipelines).

---

## Table of Contents

- [1. Project Overview](#1-project-overview)
- [2. High-Level System Architecture](#2-high-level-system-architecture)
  - [2.1 Architecture Description](#21-architecture-description)
  - [2.2 End-to-End Architecture Diagram](#22-end-to-end-architecture-diagram)
- [3. End-to-End Request & Data Lifecycle](#3-end-to-end-request--data-lifecycle)
- [4. Technology Stack](#4-technology-stack)
- [5. Repository / Project Structure](#5-repository--project-structure)
- [6. Core System Components](#6-core-system-components)
  - [6.1 Simulator (Automated Attack Scenario Engine)](#61-simulator-automated-attack-scenario-engine)
  - [6.2 FastAPI API (Core Banking Gateway)](#62-fastapi-api-core-banking-gateway)
  - [6.3 PostgreSQL](#63-postgresql)
  - [6.4 Debezium & Kafka Connect](#64-debezium--kafka-connect)
  - [6.5 Apache Kafka](#65-apache-kafka)
  - [6.6 Schema Registry](#66-schema-registry)
  - [6.7 Apache Flink (Java Risk Engine)](#67-apache-flink-java-risk-engine)
  - [6.8 Python Stream Processors](#68-python-stream-processors)
  - [6.9 OpenSearch](#69-opensearch)
- [7. Docker Compose Infrastructure](#7-docker-compose-infrastructure)
- [8. Container-by-Container Reference](#8-container-by-container-reference)
  - [8.1 Tier 1 — Core Storage & API Services](#81-tier-1--core-storage--api-services)
  - [8.2 Tier 2 — Event Streaming & Message Backbone](#82-tier-2--event-streaming--message-backbone)
  - [8.3 Tier 3 — Change Data Capture (CDC)](#83-tier-3--change-data-capture-cdc)
  - [8.4 Tier 4 — Apache Flink Cluster](#84-tier-4--apache-flink-cluster)
  - [8.5 Tier 5 — Threat Hunting](#85-tier-5--threat-hunting)
  - [8.6 Tier 6 — Observability](#86-tier-6--observability)
  - [8.7 Technical Summary Matrix](#87-technical-summary-matrix)
- [9. Data Flow Deep Dive](#9-data-flow-deep-dive)
  - [9.1 Change Data Capture (CDC) Pipeline Specification](#91-change-data-capture-cdc-pipeline-specification)
  - [9.2 Apache Flink Java Risk Engine Deep Dive](#92-apache-flink-java-risk-engine-deep-dive)
- [10. Kafka Topics & Event Pipelines](#10-kafka-topics--event-pipelines)
- [11. Observability Stack](#11-observability-stack)
- [12. Deployment Guide](#12-deployment-guide)
  - [12.1 Local Development Quick Start](#121-local-development-quick-start)
  - [12.2 Multi-Node Production Deployment (Docker Swarm & GHCR)](#122-multi-node-production-deployment-docker-swarm--ghcr)
- [13. Appendix & Reference Material](#13-appendix--reference-material)
  - [13.1 SRE Safeguards & Protected Demo Controls](#131-sre-safeguards--protected-demo-controls)
  - [13.2 Open Source Governance](#132-open-source-governance)

---

## 1. Project Overview

An enterprise-grade, end-to-end core banking platform and real-time threat intelligence ecosystem. This platform combines a high-performance core banking API, Change Data Capture (CDC) streaming via PostgreSQL Write-Ahead Logging (WAL), real-time stateful risk scoring with Apache Flink, OpenSearch threat hunting, Prometheus/Grafana telemetry, and programmable multi-vector attack simulation.

---

## 2. High-Level System Architecture

### 2.1 Architecture Description

The architecture uses a dual-path telemetry and data pipeline:
1. **Application Path:** FastAPI processes HTTP REST traffic, performs async database mutations on PostgreSQL, and logs request telemetry to Kafka via custom middleware.
2. **CDC Path:** State changes (`INSERT`, `UPDATE`, `DELETE`) in PostgreSQL are captured at the Write-Ahead Log (WAL) level by Debezium inside Kafka Connect and streamed to Kafka without relying on application-level triggers.
3. **Analytics & Detection Path:** Apache Flink streams CDC topics and telemetry, runs stateful risk scoring with decay/suppression logic, performs OpenSearch enrichments, and emits alerts to `fraud_alerts_v2` and `critical_alerts_v2`.
4. **Hunting & Visualization:** OpenSearch Sink ingests Kafka streams into OpenSearch indices for search and OpenSearch Dashboards visualization.

### 2.2 End-to-End Architecture Diagram

flowchart TB
    SIM["Simulator<br/>20 persona-driven customer actors + 2 employee actors<br/>+ 15 named attack scenarios (scenarios.py)"]

    subgraph API["FastAPI Gateway (api/)"]
        ROUTES["Core Banking: /api/auth, /api/accounts, /api/transfers, /api/cards<br/>SOC Gateway: /api/v1/incidents, /api/graph, /api/v1/analytics"]
        MW["Middleware: KafkaRequestLoggerMiddleware (Avro telemetry, fire-and-forget)<br/>+ Prometheus metrics middleware"]
    end

    PG[("PostgreSQL 15<br/>wal_level=logical")]
    SR["Schema Registry :8081<br/>Avro schemas (.avsc)"]

    subgraph DBZ["Debezium / Kafka Connect"]
        CDC["postgres-banking-cdc connector<br/>table.include.list:<br/>login_events, sessions, accounts,<br/>transactions, beneficiaries, employee_actions"]
    end

    subgraph KAFKA["Kafka Broker (KRaft)"]
        T1["api_requests / dead_letter_queue"]
        T2["banking.login_events, banking.sessions,<br/>banking.accounts, banking.transactions,<br/>banking.beneficiaries, banking.employee_actions"]
        T3["fraud_alerts_v2 / critical_alerts_v2"]
    end

    subgraph FLINK["Apache Flink Risk Engine (Java) — the only LIVE scoring engine"]
        NORM["RiskEventNormalizer + AccountEventNormalizer"]
        ENRICH["AccountEnrichmentFunction<br/>(broadcast join: account_id → customer_id)"]
        OSLOOKUP["OpenSearchLookupFunction<br/>(async I/O, fail-open, 100 in-flight cap)"]
        PROC["RiskScoringProcessor<br/>keyed RocksDB state, decay, cooldown,<br/>indicator taxonomy → cumulative score"]
        NORM --> ENRICH --> OSLOOKUP --> PROC
    end

    subgraph GRAPH["Graph Analytics (api/app/services/)"]
        GB["GraphBuilderService<br/>NetworkX MultiDiGraph from Postgres"]
        RD["RingDetectorService<br/>simple_cycles + shared-infra clustering"]
        GB --> RD
    end

    AGG["Alert Aggregator worker<br/>(api/app/workers/alert_aggregator.py)<br/>dedups alerts → Incident / IncidentAlert rows"]

    subgraph OS["OpenSearch"]
        IDX["Indices: api_requests, login_events,<br/>employee_actions, alerts (fraud+critical merged)"]
        DASH["OpenSearch Dashboards :5601"]
    end

    SINK["OpenSearch Sink daemon<br/>(analytics/opensearch/) — one thread per topic"]

    subgraph OBS["Observability"]
        PROM["Prometheus"]
        GRAF["Grafana"]
        KEXP["kafka-exporter"]
    end

    SOC["SOC Analyst<br/>via /api/v1/incidents and /api/graph"]

    %% Flow
    SIM -->|HTTP| ROUTES
    ROUTES --> MW
    MW -->|SQL writes, asyncpg| PG
    MW -->|Avro telemetry| T1
    SR -.->|schema contracts| T1
    SR -.->|schema contracts| T3

    PG -->|WAL logical replication| CDC
    CDC --> T2

    T2 --> NORM
    T1 --> NORM
    T3 -.->|env schema config only| FLINK

    PROC -->|FRAUD severity| T3
    PROC -->|CRITICAL severity| T3

    T3 --> AGG
    AGG -->|writes| PG
    PG -.->|queried on demand| GB
    ROUTES -->|GET /api/graph/*| GB
    RD -->|FraudRingAlert| ROUTES

    T1 --> SINK
    T2 --> SINK
    T3 --> SINK
    SINK --> IDX
    IDX --> DASH

    ROUTES <-->|list/assign/resolve incidents| SOC
    DASH -->|threat hunting| SOC

    ROUTES -.->|/metrics| PROM
    FLINK -.->|Prometheus reporter :9249| PROM
    KAFKA -.->|kafka-exporter :9308| PROM
    PROM --> GRAF

    classDef live fill:#1f6f43,color:#fff,stroke:#0d3d24
    classDef infra fill:#2c3e50,color:#fff,stroke:#111
    classDef store fill:#34495e,color:#fff,stroke:#111
    class FLINK,GRAPH,AGG,SINK live
    class API,DBZ,KAFKA,OS,OBS infra
    class PG,SR store

---

## 3. End-to-End Request & Data Lifecycle

# Data Flow

1. **Users & Simulator**
   - Generate legitimate banking activity and fraud scenarios.
   - Send requests to the FastAPI gateway.

2. **FastAPI Gateway**
   - Processes banking transactions and SOC operations.
   - Writes transactional data into PostgreSQL.
   - Publishes HTTP telemetry to Kafka using Avro serialization.

3. **PostgreSQL**
   - Stores transactional banking data.
   - Produces logical replication events through the Write-Ahead Log (WAL).

4. **Debezium Kafka Connect**
   - Reads the PostgreSQL WAL through logical replication.
   - Converts database changes into Kafka CDC events without polling tables.

5. **Kafka Broker**
   - Central event streaming platform.
   - Stores:
     - Telemetry topics
     - CDC topics
     - Fraud alert topics

6. **Apache Flink Analytics**
   - Performs stateful stream processing.
   - Joins CDC account updates with transaction streams.
   - Executes:
     - Risk scoring
     - Complex Event Processing (CEP)
     - Alert suppression
     - Fraud pattern detection

7. **Python Kafka Stream Engines**
   - Execute lightweight rule-based analytics.
   - Perform dynamic policy evaluation.
   - Produce anomaly signals.

8. **OpenSearch**
   - Indexes telemetry, alerts, and logs.
   - Supports fast searches and historical investigations.
   - Serves dashboards through OpenSearch Dashboards.

---

## 4. Technology Stack

# Architecture Summary

| Layer | Primary Components | Responsibility |
|--------|--------------------|----------------|
| Client Layer | Simulator, Users | Generate banking transactions and fraud scenarios |
| API Layer | FastAPI Gateway | Banking APIs, SOC APIs, telemetry generation |
| Data Layer | PostgreSQL | Transactional database with logical replication |
| CDC Layer | Debezium Kafka Connect | Streams database changes into Kafka |
| Streaming Layer | Apache Kafka (KRaft) | Central event backbone for telemetry, CDC, and alerts |
| Analytics Layer | Apache Flink, Python Kafka Streams | Real-time fraud detection, CEP, and risk scoring |
| Search Layer | OpenSearch & Dashboards | Threat hunting, indexing, and visualization |
| Observability Layer | Prometheus, Grafana, Kafka Exporter | Monitoring, metrics, dashboards, and alerting |

---

## 5. Repository / Project Structure

# Banking Fraud Platform Directory Structure

```text
banking-fraud-platform/
├── .github/
│   └── workflows/
│       └── deploy.yml              # CI/CD automated build and deployment pipeline
├── analytics/                      # Streaming engines & search index processing
│   ├── apache-flink-scripts/
│   │   ├── flink_cep_engine.py     # PyFlink Complex Event Processing engine for pattern detection
│   │   └── flink_risk_engine.py    # PyFlink streaming pipeline for real-time risk evaluation
│   ├── flink-risk-engine-java/     # Production Java Flink Risk Engine
│   │   ├── Dockerfile              # Build configuration for containerized Flink job runner
│   │   ├── pom.xml                 # Maven build dependencies and packaging configuration
│   │   ├── submit-job.sh           # Shell script to deploy compiled JAR to Flink JobManager
│   │   └── src/
│   │       ├── main/
│   │       │   ├── java/com/banking/fraud/
│   │       │   │   ├── AccountEnrichmentFunction.java         # Statefully enriches events with account metadata
│   │       │   │   ├── AccountEventNormalizer.java            # Normalizes account state changes from CDC
│   │       │   │   ├── AccountUpdate.java                     # Data model representing account state changes
│   │       │   │   ├── FraudAlert.java                        # Output data model for generated risk alerts
│   │       │   │   ├── FraudAlertAvroSerializationSchema.java # Avro serializer for Kafka alert output
│   │       │   │   ├── OpenSearchLookupFunction.java          # Async enricher querying OpenSearch for history
│   │       │   │   ├── RiskEvent.java                         # Standardized stream event object
│   │       │   │   ├── RiskEventNormalizer.java               # Converts raw Kafka messages into RiskEvents
│   │       │   │   ├── RiskProfileState.java                  # Flink Managed State tracking customer history
│   │       │   │   ├── RiskScoringConfig.java                 # Parser for risk rules, weights, and thresholds
│   │       │   │   ├── RiskScoringJob.java                    # Entry point establishing the Flink execution DAG
│   │       │   │   ├── RiskScoringProcessor.java              # KeyedProcessFunction evaluating rules & windowing
│   │       │   │   └── TrustAllSslContext.java                # SSL context helper for development environments
│   │       │   └── resources/
│   │       │       └── risk-scoring.properties                # Scoring rule weights, thresholds, & endpoints
│   │       └── test/                                          # Unit & integration tests for Flink functions
│   ├── kafka-stream-scripts/
│   │   ├── risk_scoring_engine.py  # Lightweight Python Kafka consumer for risk scoring
│   │   └── rules_engine.py         # Dynamic rule engine evaluating telemetry events
│   └── opensearch/
│       ├── Dockerfile              # Container configuration for OpenSearch consumer sink
│       ├── opensearch_sink.py      # Python worker writing Kafka streams directly into OpenSearch
│       └── requirements.txt        # OpenSearch ingestion dependencies
├── api/                            # Core FastAPI Banking Platform & SOC Gateway
│   ├── Dockerfile                  # Container build configuration for the main API service
│   ├── alembic.ini                 # Database migration configuration file
│   ├── alembic/
│   │   └── versions/
│   │       └── 002_create_incident_management_tables.py # DB migration for SOC incident tables
│   ├── app/
│   │   ├── main.py                 # FastAPI app entry point, middleware, & startup tasks
│   │   ├── database.py / core/database.py # Async SQLAlchemy database connection setup
│   │   ├── api/
│   │   │   ├── accounts.py         # Bank account creation & balance endpoints
│   │   │   ├── analytics.py        # Analytics & metrics query endpoints for dashboards
│   │   │   ├── auth.py             # User authentication & token issuance
│   │   │   ├── cards.py            # Card management & blocking endpoints
│   │   │   ├── demo.py             # Demo environment reset & seed control
│   │   │   ├── employee.py         # Bank staff & teller management endpoints
│   │   │   ├── graph.py            # Fraud ring graph network query endpoints
│   │   │   ├── incidents.py        # SOC analyst incident triage & case management
│   │   │   ├── security.py         # Audit trail & security telemetry endpoints
│   │   │   ├── transfers.py        # Money transfer & transaction execution
│   │   │   └── users.py            # Customer profile endpoints
│   │   ├── middleware/
│   │   │   ├── kafka_logger.py     # Middleware emitting all HTTP traffic to Kafka
│   │   │   └── kafka_producer.py   # Asynchronous Kafka producer service wrapper
│   │   ├── models/
│   │   │   ├── domain.py           # Users, accounts, transactions, and devices models
│   │   │   └── incidents.py        # Fraud alerts and incident management models
│   │   ├── schemas/
│   │   │   ├── analytics.py        # Metrics data models
│   │   │   ├── domain.py           # Core banking entities schemas
│   │   │   ├── graph.py            # Network topology schemas
│   │   │   └── incident.py         # Fraud alert and incident response schemas
│   │   ├── services/
│   │   │   ├── analytics_service.py # Aggregates metrics for SOC dashboards
│   │   │   ├── graph_builder.py    # Builds entity relationship graphs (users, IPs, devices)
│   │   │   └── ring_detector.py    # Graph algorithms identifying organized fraud rings
│   │   └── workers/
│   │       └── alert_aggregator.py # Background process aggregating and deduplicating alerts
│   ├── seed_db.py                  # Standalone script to seed fresh DB tables & test profiles
│   └── tests/                      # Integration tests for provisioning and API endpoints
├── backend/                        # Alternative/Legacy Core Banking service module
│   ├── database.py / models.py / schemas.py / main.py
│   └── routers/                    # Auth, Core Banking, and Security endpoints
├── config/                         # System-wide static configurations
│   ├── kafka-connect/
│   │   └── postgres-source.json    # Debezium CDC source connector template
│   ├── opensearch/
│   │   └── mappings.json           # OpenSearch index field mapping definitions
│   └── schemas/
│       └── api_request.avsc        # Avro schema for raw API telemetry events
├── grafana/                        # Observability dashboards configuration
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboards.yml      # Automated dashboard provisioning configuration
│       └── datasources/
│           └── ds-prometheus.yml   # Prometheus datasource definition
├── infra/                          # Infrastructure & container setup
│   ├── docker-compose.yml          # Secondary infrastructure compose stack
│   ├── postgres-init.sh            # Database initialization script (wal_level=logical)
│   └── postgres.Dockerfile         # PostgreSQL image customized for Debezium CDC
├── prometheus/
│   └── prometheus.yml              # Prometheus scraping rules for system metrics
├── simulator/                      # Transaction & Fraud Scenario Generator
│   ├── Dockerfile                  # Container build file for simulation worker
│   ├── app/
│   │   ├── main.py                 # Main simulation runner executing actor loops
│   │   ├── actors/
│   │   │   ├── customer.py         # Simulates legitimate user behaviors (transfers, logins)
│   │   │   └── employee.py         # Simulates internal staff behaviors (account lookups)
│   │   ├── attacks/
│   │   │   └── scenarios.py        # Fraud attack implementations (ATO, Impossible Travel, Rings)
│   │   ├── core/
│   │   │   └── client.py           # HTTP client wrapping core banking API requests
│   │   └── utils/
│   │       └── generators.py       # Faker utilities generating realistic user/device payloads
│   └── trigger.py                  # CLI command tool to manually execute targeted attack scenarios
├── .gitignore                      # Git exclusion rules
├── check_port_8000.ps1             # PowerShell utility verifying API port availability
├── debezium-postgres-connector.json # Debezium connector registration configuration
├── docker-compose.yml              # Primary Docker Compose orchestration file
├── kill_orphans.ps1                # PowerShell script clearing background zombie processes
├── schema.sql                      # SQL baseline database schema
└── test_governance.py              # Governance & compliance test script
```

---

## 6. Core System Components

This section introduces each subsystem's responsibilities before the deeper Docker Compose container reference and data-flow deep dives that follow.

### 6.1 Simulator (Automated Attack Scenario Engine)

- **Native API Scenarios**
  - Execute scenarios through:
    - `/api/demo/scenarios/run`

  **Available Scenarios:**
  - `SHARED_INFRASTRUCTURE`
  - `MULE_STRUCTURING`
  - `KNOWN_DEVICE_THREAT`
  - `WIRE_FRAUD`
  - `SYNTHETIC_IDENTITY`

- **CLI Trigger Simulator**
  - Includes **15+** standalone attack simulation scripts
  - Entry point: `simulator/trigger.py`

### 6.2 FastAPI API (Core Banking Gateway)

Core Banking Gateway (FastAPI + Async SQLAlchemy):

- **Authentication & Sessions**
  - Token issuance
  - Password reset workflows
  - Session management
  - Brute-force protection
  - **API Endpoint:** `/api/auth`

- **Account Ledger & Cards**
  - Multi-account management
  - Balance inquiries
  - Ledger posting
  - Debit and credit card issuance
  - Card spending limits
  - Instant card locking
  - **API Endpoints:** `/api/accounts`, `/api/cards`

- **Funds Transfer System**
  - Internal account transfers
  - External wire transfers
  - Beneficiary verification
  - **API Endpoint:** `/api/transfers`

- **Employee Audit Tracker**
  - Staff privilege management
  - Teller activity logging
  - Insider threat auditing
  - **API Endpoint:** `/api/employee`

- **Network Graph Routing**
  - NetworkX-based entity relationship mapping
  - Account-to-device relationship analysis
  - Money mule network detection
  - **API Endpoint:** `/api/graph`

- **Incident Management**
  - Security Operations Center (SOC) alert triage
  - Incident lifecycle tracking
  - Manual and automated account locking
  - **API Endpoint:** `/api/v1/incidents`

### 6.3 PostgreSQL

- **PostgreSQL 15 (WAL Logical Replication)**
  - Pre-configured with:
    - `wal_level=logical`
    - `max_replication_slots=4`
    - `max_wal_senders=4`

### 6.4 Debezium & Kafka Connect

- **Kafka Connect + Debezium CDC**
  - Real-time Change Data Capture (CDC) stream replication for:
    - `login_events`
    - `sessions`
    - `accounts`
    - `transactions`
    - `beneficiaries`
    - `employee_actions`

### 6.5 Apache Kafka

- **Kafka KRaft**
  - Single-node or clustered event messaging

### 6.6 Schema Registry

- **Schema Registry**
  - Avro schema management

### 6.7 Apache Flink (Java Risk Engine)

Apache Flink Java Risk Engine (`flink-risk-engine-java`):

- **Stateful Risk Scoring**
  - Maintains keyed per-identity state
  - Evaluates:
    - Transaction velocity
    - Location anomalies
    - New device identifiers
    - Impossible travel events
    - High-value transfers

- **Score Decay & Cooldown Suppression**
  - Automatic score decay over time
  - Cooldown-based alert suppression to reduce alert fatigue

- **OpenSearch Lookup Enrichment**
  - Dynamic enrichment using:
    - `OpenSearchLookupFunction`
    - `TrustAllSslContext`

A full deep dive into the engine's processing pipeline, responsibilities, and internal class reference is provided in [9.2 Apache Flink Java Risk Engine Deep Dive](#92-apache-flink-java-risk-engine-deep-dive).

### 6.8 Python Stream Processors

Python Kafka Stream Scripts (`analytics/kafka-stream-scripts/`):

1. `rules_engine.py`
   - Dynamic policy evaluation
   - Low-latency anomaly detection

2. `risk_scoring_engine.py`
   - Lightweight stateless/sliding risk evaluation

These engines execute lightweight rule-based analytics, perform dynamic policy evaluation, and produce anomaly signals as part of the end-to-end data lifecycle (see [3. End-to-End Request & Data Lifecycle](#3-end-to-end-request--data-lifecycle)).

### 6.9 OpenSearch

Search, Analytics & Threat Hunting:

- **OpenSearch Cluster & Dashboards**
  - Indexes:
    - `api_requests`
    - `login_events`
    - `alerts`
  - Dashboard available on **Port 5601**

- **Python OpenSearch Sink**
  - High-throughput asynchronous daemon
  - Consumes Kafka CDC streams and Apache Flink alerts
  - Writes data directly into OpenSearch indices

---

## 7. Docker Compose Infrastructure

# Docker Compose Architecture & Container Documentation

This document provides a comprehensive technical reference for the `docker-compose.yml` file driving the **Banking Fraud Platform**.

The stack orchestrates **16 specialized containers** spanning:

- Database transactional layers
- ZooKeeper-less Kafka streaming
- Change Data Capture (CDC) pipelines
- Stateful Apache Flink analytics
- OpenSearch threat hunting
- Prometheus/Grafana observability

### Architecture Overview

```text
                                  +-------------------+
                                  |   bank_postgres   | (PostgreSQL 15 CDC Source)
                                  +---------+---------+
                                            |
                                   (WAL Logical Replication)
                                            |
                                            v
                                  +-------------------+
                                  |   bank_connect    | (Debezium Kafka Connect)
                                  +---------+---------+
                                            |
                                            v
+------------------+              +-------------------+              +-----------------------+
|     bank_api     |  =========>  |    bank_kafka     |  =========>  | bank_schema_registry  |
| (FastAPI Backend)| (HTTP Logs)  |   (KRaft Mode)    | (Avro Schemas)|   (Schema Registry)  |
+------------------+              +---------+---------+              +-----------------------+
                                            |
                   +-------------------------+-------------------------+
                   |                                                   |
                   v                                                   v
       +-----------------------+                           +-----------------------+
       | bank_flink_jobmanager |                           | bank_opensearch_sink |
       +-----------+-----------+                           +-----------+-----------+
                   |                                                   |
                   v                                                   v
       +-----------------------+                           +-----------------------+
       | bank_flink_taskmanager|                           |    bank_opensearch    |
       +-----------------------+                           +-----------+-----------+
                   |                                                   |
                   v                                                   v
       +-----------------------+                           +-----------------------+
       |   bank_fraud_engine   |                           | opensearch_dashboards |
       | (Submits Flink Job)   |                           +-----------------------+
       +-----------------------+
```

---

## 8. Container-by-Container Reference

### 8.1 Tier 1 — Core Storage & API Services

#### 1. PostgreSQL (`bank_postgres`)

**Role**

Primary relational database for:

- User accounts
- Ledger balances
- Employee audit records
- CDC source for Debezium

**Image**

```text
postgres:15-alpine
```

**Ports**

| Host | Container |
|------|-----------|
| `${POSTGRES_PORT}` | `5432` |

##### Runtime Configuration

```text
-c wal_level=logical
-c max_replication_slots=4
-c max_wal_senders=4
```

##### Persisted Volume

```text
Host:
F:/DaTech-kafka/banking-fraud-platform/persisted-volumes/postgres

Container:
/var/lib/postgresql/data
```

##### Environment Variables

- POSTGRES_USER
- POSTGRES_PASSWORD
- POSTGRES_DB

#### 2. API (`bank_api`)

##### Role

FastAPI backend responsible for:

- Banking APIs
- Transaction processing
- Fraud graph generation
- SOC incident triage
- HTTP telemetry logging

##### Build Context

```text
./api
```

##### Dependencies

- Kafka
- PostgreSQL
- Schema Registry

##### Environment

```text
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

DATABASE_URL=postgresql+asyncpg://...

SCHEMA_REGISTRY_URL=http://schema-registry:8081

ENABLE_DEMO_RESET=true
```

Supports an external `.env` file.

### 8.2 Tier 2 — Event Streaming & Message Backbone

#### 3. Kafka (`bank_kafka`)

##### Role

Central messaging backbone running **Apache Kafka in ZooKeeper-less KRaft Mode**.

##### Image

```text
confluentinc/cp-kafka:7.4.0
```

##### Ports

| Host | Container |
|------|-----------|
|29092|9092|

##### Important Configuration

- Combined Broker + Controller
- KRaft Metadata Consensus
- Auto Topic Creation
- Advertised Listeners
- Cluster ID
- Log Directory

##### Health Check

```yaml
kafka-topics --bootstrap-server localhost:29092 --list
```

#### 4. Schema Registry (`bank_schema_registry`)

##### Role

Stores and validates Avro schemas used across producers and consumers.

##### Image

```text
confluentinc/cp-schema-registry:7.4.0
```

##### Port

|Host|Container|
|----|---------|
|18081|8081|

### 8.3 Tier 3 — Change Data Capture (CDC)

#### 5. Kafka Connect (`bank_connect`)

##### Role

Runs the **Debezium PostgreSQL Connector**.

Responsibilities include:

- Reading PostgreSQL WAL
- Capturing inserts, updates and deletes
- Publishing CDC events into Kafka
- Tracking connector offsets

##### Image

```text
debezium/connect:2.4
```

##### Port

|Host|Container|
|----|---------|
|8083|8083|

##### Distributed Topics

- my_connect_configs
- my_connect_offsets
- my_connect_statuses

#### 6. Connect Init (`bank_connect_init`)

##### Role

Bootstrap container responsible for:

- Waiting for Kafka Connect
- Registering the PostgreSQL Debezium connector
- Exiting once registration completes

Configuration file:

```text
debezium-postgres-connector.json
```

#### 7. Kafka Init (`bank_kafka_init`)

##### Role

Creates Kafka topics automatically.

The full list of topics created is provided in [10. Kafka Topics & Event Pipelines](#10-kafka-topics--event-pipelines).

### 8.4 Tier 4 — Apache Flink Cluster

#### 8. Flink JobManager (`bank_flink_jobmanager`)

##### Responsibilities

- Job scheduling
- Checkpoint coordination
- DAG execution
- REST API
- Web UI

##### Image

```text
flink:1.18-scala_2.12-java11
```

##### Ports

|Host|Container|
|----|---------|
|8082|8081|
|9249|9249|

#### 9. Flink TaskManager (`bank_flink_taskmanager`)

##### Responsibilities

Executes:

- Keyed Process Functions
- Sliding Windows
- Account Enrichment
- Velocity Scoring
- Stateful Stream Operators

Task Slots

```text
2
```

#### 10. Fraud Engine (`bank_fraud_engine`)

##### Responsibilities

- Compiles Java Flink project
- Packages JAR
- Deploys JAR to Flink JobManager
- Exits after successful submission

### 8.5 Tier 5 — Threat Hunting

#### 11. OpenSearch (`bank_opensearch`)

##### Responsibilities

Indexes:

- API logs
- Fraud alerts
- Security events
- Historical profiles

##### Image

```text
opensearchproject/opensearch:2.11.0
```

##### Ports

|Host|Container|
|----|---------|
|9200|9200|
|9600|9600|

#### 12. OpenSearch Dashboards

Provides:

- Threat hunting UI
- Dashboards
- Search
- Timeline visualization

Port:

```text
5601
```

#### 13. OpenSearch Sink

Python worker responsible for:

- Reading Kafka topics
- Deserializing Avro
- Bulk indexing documents into OpenSearch

### 8.6 Tier 6 — Observability

#### 14. Prometheus

Collects metrics for:

- CPU
- Memory
- Kafka
- Flink
- API
- Queue depth

Port

```text
9090
```

#### 15. Grafana

Provides dashboards for:

- SOC monitoring
- Kafka metrics
- System latency
- Infrastructure health

Port

```text
3000
```

#### 16. Kafka Exporter

Exports Kafka metrics such as:

- Consumer lag
- Topic throughput
- Partition offsets

Port

```text
9308
```

### 8.7 Technical Summary Matrix

# Technical Summary Matrix

| Service | Container | Host Port | Internal Port | Restart |
|----------|-----------|-----------|---------------|----------|
| PostgreSQL | bank_postgres | `${POSTGRES_PORT}` | 5432 | unless-stopped |
| API | bank_api | 8000 | 8000 | unless-stopped |
| Kafka | bank_kafka | 29092 | 9092 | unless-stopped |
| Schema Registry | bank_schema_registry | 18081 | 8081 | unless-stopped |
| Kafka Connect | bank_connect | 8083 | 8083 | unless-stopped |
| Connect Init | bank_connect_init | — | — | no |
| Kafka Init | bank_kafka_init | — | — | no |
| Flink JobManager | bank_flink_jobmanager | 8082, 9249 | 8081, 9249 | unless-stopped |
| Flink TaskManager | bank_flink_taskmanager | — | 9249 | unless-stopped |
| Fraud Engine | bank_fraud_engine | — | — | no |
| OpenSearch | bank_opensearch | 9200,9600 | 9200,9600 | unless-stopped |
| OpenSearch Dashboards | bank_opensearch_dashboards | 5601 | 5601 | unless-stopped |
| OpenSearch Sink | bank_opensearch_sink | — | — | unless-stopped |
| Prometheus | bank_prometheus | 9090 | 9090 | always |
| Grafana | bank_grafana | 3000 | 3000 | always |
| Kafka Exporter | bank_kafka_exporter | 9308 | 9308 | always |

---

## 9. Data Flow Deep Dive

### 9.1 Change Data Capture (CDC) Pipeline Specification

This infrastructure pipeline uses **Debezium** and **Kafka Connect** to stream row-level mutations directly from PostgreSQL's Write-Ahead Log (WAL) into Kafka.

#### 9.1.1 Architecture & Single Message Transformations (SMT)

The CDC pipeline captures `INSERT`, `UPDATE`, and `DELETE` events on key banking tables. To keep broker topic names clean, a **RegexRouter Single Message Transformation (SMT)** strips out the `.public.` schema marker.

##### Data Flow

1. **PostgreSQL**
   - Logical replication plugin (`pgoutput`) publishes mutations to a replication slot.

2. **Debezium Connector**
   - Polls the replication slot and converts raw WAL streams into structured JSON events.

3. **RegexRouter SMT**
   - Rewrites topics matching `banking.public.<table_name>` to `banking.<table_name>`.

#### 9.1.2 PostgreSQL WAL & Debezium Configuration

##### PostgreSQL Container Configuration (`docker-compose.yml`)

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

##### Connector Deployment Payload (`debezium-postgres-connector.json`)

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

#### 9.1.3 Operations & Lifecycle Commands

##### Register the CDC Connector

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d @debezium-postgres-connector.json `
  http://localhost:8083/connectors
```

##### Verify Active Kafka Topics

```powershell
docker exec -it bank_kafka kafka-topics \
  --bootstrap-server localhost:29092 \
  --list
```

##### Tail Live CDC Mutations (`banking.accounts`)

```powershell
docker exec -it bank_kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic banking.accounts \
  --from-beginning
```

##### Force a Direct Database Mutation

```powershell
docker exec -it bank_postgres psql \
  -U postgres_admin \
  -d banking_db \
  -c "UPDATE accounts SET balance = balance + 250.00 WHERE account_id = 'ACC-GEN00000';"
```

#### 9.1.4 CDC Payload Structure

CDC events follow the standard Debezium envelope:

- `before`
- `after`
- `source`
- `op`
- `ts_ms`

##### Example Event

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

##### Operation Types

| Operation | Meaning |
|-----------|---------|
| `r` | Initial Snapshot Read |
| `c` | Record Creation (`INSERT`) |
| `u` | Record Modification (`UPDATE`) |
| `d` | Record Deletion (`DELETE`) |

### 9.2 Apache Flink Java Risk Engine Deep Dive

# Apache Flink Java Risk Engine (`flink-risk-engine-java`)

Located in `analytics/flink-risk-engine-java/`, this stateful Java application replaces the original Python stream prototypes with enterprise Flink processing.

The engine writes alerts to the **v2 topic space**:

- `fraud_alerts_v2`
- `critical_alerts_v2`

#### Processing Pipeline

```text
banking.transactions ──┐
banking.login_events ──┼─► RiskEventNormalizer
banking.sessions ──────┤        │
api_requests ──────────┘        ▼
                         Watermarking
                               │
                               ▼
                     KeyBy(identityKey)
                               │
                               ▼
                    RiskScoringProcessor
                               │
                               ▼
                      Kafka Alert Sink
```

#### 9.2.1 Engine Responsibilities

##### CDC Envelope Normalization

Unwraps raw Debezium CDC envelopes (`before`/`after`) into `RiskEvent` objects.

##### Dynamic Risk Scoring

Calculates behavioral indicator weights, including:

- Impossible travel
- New country
- Device switching
- High velocity
- Large wire amounts

##### OpenSearch Lookups

Uses `OpenSearchLookupFunction` to query OpenSearch for contextual identity profile history.

##### Time-Decayed State

Applies exponential score decay during quiet periods to avoid false positives.

##### Cooldown-Based Suppression

Emits alerts when thresholds are crossed, suppressing duplicate alerts within the configured cooldown window.

#### 9.2.2 Internal Java Class Reference

- **`RiskEvent.java`**: Unified internal record model across all event types.
- **`OpenSearchLookupFunction.java`**: Async lookup function for querying OpenSearch indices.
- **`TrustAllSslContext.java`**: Custom SSL context handler for local OpenSearch cluster connections.
- **`RiskScoringJob.java`**: Main execution graph containing sources, watermarks, operators, and sinks.

---

## 10. Kafka Topics & Event Pipelines

The `bank_kafka_init` container creates the following Kafka topics automatically:

**Telemetry**

- api_requests
- dead_letter_queue
- enriched_transactions
- model_features
- banking.unified_timeline

**Fraud**

- fraud_alerts
- critical_alerts
- risk_scores
- risk_scores_v2
- fraud_alerts_v2
- critical_alerts_v2

**CDC**

- banking.accounts
- banking.transactions
- banking.sessions
- banking.login_events
- banking.employee_actions
- banking.beneficiaries

---

## 11. Observability Stack

# Observability & Operational Metrics

```text
===========================================================================================
                    OBSERVABILITY & OPERATIONAL METRICS TIER
===========================================================================================

┌──────────────────┐      ┌─────────────────────────┐      ┌─────────────────────────┐
│  FastAPI / API   │      │ Flink Job & TaskManager │      │     Kafka Exporter      │
│ Metrics Endpoint │      │ Prometheus Port (9249)  │      │ Broker & Consumer Lag   │
└────────┬─────────┘      └────────────┬────────────┘      └────────────┬────────────┘
         │                             │                                │
         └─────────────────────────────┼────────────────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────┐
                    │       Prometheus Server (9090)           │
                    │                                          │
                    │ • Scrapes metrics                        │
                    │ • Applies prometheus.yml rules           │
                    └──────────────────┬───────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────┐
                    │        Grafana Dashboards (3000)         │
                    │                                          │
                    │ • Operational dashboards                 │
                    │ • SOC monitoring                         │
                    │ • Kafka monitoring                       │
                    │ • Infrastructure health                  │
                    └──────────────────────────────────────────┘
```

### 11.1 Prometheus Metrics

The FastAPI gateway exports metrics via the `/metrics` endpoint.

#### Exported Metrics

- **`banking_api_requests_total`**: Request counter tagged by HTTP method, endpoint, and status code.
- **`banking_api_request_duration_seconds`**: Histogram measuring API endpoint latency.

### 11.2 Grafana Dashboards

Pre-configured dashboards in `grafana/` track real-time platform metrics, including:

- HTTP API request rates
- API latency percentiles (`p95`, `p99`)
- Kafka ingestion rates
- Debezium CDC lag
- Fraud alerts triggered per minute by severity (`WARNING`, `CRITICAL`)

---

## 12. Deployment Guide

### 12.1 Local Development Quick Start

#### 1. Infrastructure Stack Setup

From the repository root, start the local Docker Compose infrastructure:

```powershell
docker compose up -d
```

##### Exposed Local Services

| Service | Address |
|---------|---------|
| PostgreSQL | `localhost:5433` (Internal: `5432`) |
| Apache Kafka | `localhost:29092` |
| Schema Registry | `localhost:8081` |
| Kafka Connect | `localhost:8083` |
| OpenSearch | `localhost:9200` |
| OpenSearch Dashboards | http://localhost:5601 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

To stop the container stack:

```powershell
docker compose down
```

#### 2. Database Seeding

After the containers are healthy, populate PostgreSQL with seed accounts, mock customers, VIP targets, and initial schema states:

```powershell
cd api
venv\Scripts\activate
python seed_db.py
```

#### 3. Running the Core Banking API

Start the FastAPI backend with Uvicorn:

```powershell
cd api
venv\Scripts\activate
pip install -r requirements.txt
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The API interactive Swagger UI is available at:

- http://localhost:8000/docs

#### 4. CDC Connector Registration

Register the Debezium PostgreSQL connector with Kafka Connect:

```powershell
curl.exe -X POST -H "Content-Type: application/json" -d @debezium-postgres-connector.json http://localhost:8083/connectors
```

Confirm active topics created by Debezium:

```powershell
docker exec -it bank_kafka kafka-topics --bootstrap-server localhost:29092 --list
```

#### 5. Running the Apache Flink Java Risk Engine

Launch the Flink engine container from Docker Compose:

```powershell
docker compose up -d fraud-engine
```

Alternatively, submit the compiled JAR manually into a running JobManager:

```bash
cd analytics/flink-risk-engine-java
./submit-job.sh
```

#### 6. OpenSearch Threat Hunting Daemon

Start the Python OpenSearch Sink daemon to consume Kafka streams into OpenSearch:

```powershell
docker compose up -d opensearch-sink
```

#### 7. Fraud & Attack Scenario Simulator

Run attack scenarios using either the REST API or the standalone Python simulator script.

##### Option A: Via REST API Endpoint

```powershell
curl -X POST "http://localhost:8000/api/demo/scenarios/run" `
     -H "Content-Type: application/json" `
     -d "{\"scenario_type\": \"MULE_STRUCTURING\", \"account_count\": 10}"
```

##### Option B: Via Python Simulator CLI

```powershell
cd simulator
venv\Scripts\activate
pip install -r requirements.txt
python trigger.py credential_stuffing
```

##### Available Attack Scenarios

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

### 12.2 Multi-Node Production Deployment (Docker Swarm & GHCR)

For distributed homelab and production deployments, the platform supports multi-node deployment via **Docker Swarm** and **Tailscale**.

```text
k3s-master Node:
  - PostgreSQL (Primary CDC source)
  - Core Banking API Gateway
  - Attack Simulator
  - Kafka Connect

k3s-worker-2 Node:
  - Apache Kafka (KRaft Broker)
  - Schema Registry

k3s-worker-3 Node:
  - OpenSearch Cluster
  - Apache Flink JobManager & TaskManager
```

#### 1. Building & Publishing Custom Images to GHCR

Custom images are published to the **GitHub Container Registry (GHCR)** using lowercase image identifiers.

##### A. Set Registry Variables

```bash
export REGISTRY=ghcr.io
export OWNER=caleb-muinde83
```

##### B. Build the Docker Images

```bash
# Core API
docker build -t banking-api:latest ./api

# Attack Simulator
docker build -t banking-simulator:latest ./simulator

# PostgreSQL CDC Image
docker build -f infra/postgres.Dockerfile -t banking-postgres:latest infra/
```

##### C. Tag Images for GHCR

```bash
docker tag banking-api:latest $REGISTRY/$OWNER/banking-api:latest
docker tag banking-simulator:latest $REGISTRY/$OWNER/banking-simulator:latest
docker tag banking-postgres:latest $REGISTRY/$OWNER/banking-postgres:latest
```

##### D. Authenticate and Push

On **Windows Git Bash**, use `winpty` for interactive authentication.

```bash
winpty docker login ghcr.io -u "$OWNER"
```

Push all images:

```bash
for name in banking-api banking-simulator banking-postgres; do
  docker push $REGISTRY/$OWNER/$name:latest
done
```

##### Published Registry References

```text
ghcr.io/caleb-muinde83/banking-api:latest
ghcr.io/caleb-muinde83/banking-simulator:latest
ghcr.io/caleb-muinde83/banking-postgres:latest
```

#### 2. Docker Swarm Stack Deployment

Deploy the distributed stack to your Swarm manager node.

```bash
docker stack deploy -c infra/docker-compose.yml fraud_platform
```

Verify service placement across Swarm nodes.

```bash
docker stack services fraud_platform
```

#### 3. CI/CD via GitHub Actions

Automated deployments are defined in:

```text
.github/workflows/deploy-banking-fraud-platform.yml
```

On commits to the `main` branch, the workflow automatically:

- Builds the core API image.
- Builds the attack simulator image.
- Deploys the updated stack using `docker stack deploy`.
- Auto-registers the Debezium CDC connector with Kafka Connect after the containers become healthy.

---

## 13. Appendix & Reference Material

### 13.1 SRE Safeguards & Protected Demo Controls

The platform includes a multi-guarded environment reset mechanism (`/api/demo/reset`) to prevent accidental data destruction in staging or production environments.

#### The 4 Reset Guards

##### 1. Environment Flag

Requires `ENABLE_DEMO_RESET=true` in `.env`.

##### 2. Admin Key Header

Validates the `X-Demo-Admin-Key` request header against `DEMO_ADMIN_KEY`.

##### 3. Payload Acknowledgment

Requires explicit payload confirmation flags:

```json
{
  "confirm_reset": true,
  "ack_data_loss": true,
  "environment_tag": "demo",
  "dry_run": false
}
```

##### 4. Dry-Run Mode

Executes in safe simulation mode when `dry_run: true` is passed (the default setting).

### 13.2 Open Source Governance

This repository is maintained by **Caleb-Muinde83** and supported by the **Datech Community**.

#### License

See the `LICENSE` file for open-source licensing information.

#### Contributions

Review `CONTRIBUTING.md` before submitting pull requests or feature proposals.

#### Support & Issues

Submit bug reports or feature requests through the GitHub issue tracker:

```text
https://github.com/DatechCommunity
```

