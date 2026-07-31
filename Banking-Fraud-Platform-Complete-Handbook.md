# The Banking Fraud Platform Handbook

### A Distributed Systems & Event-Driven Architecture Case Study

---

## What this handbook is

This is a two-purpose document.

First, it is **complete technical documentation** for the `banking-fraud-platform` repository: what every service does, how the pieces fit together, how to run it, and where the sharp edges are.

Second, it is a **teaching text**. The repository is used as a case study to explain, from first principles, the ideas that make up modern event-driven, streaming-first backend systems: Change Data Capture, Kafka-based event backbones, stateful stream processing with Apache Flink, schema governance, and the operational disciplines (observability, deployment, CI/CD) that turn a collection of containers into a system someone can actually run in production.

Every chapter follows the same rhythm. It introduces a concept as if you have never seen it before, explains why the concept exists at all (what problem it solves, and what the alternatives look like), then walks through *this specific repository's* implementation of it — file by file, function by function where it matters — followed by a description of what actually happens while the system is running, a "Learning Concepts" primer connecting the implementation back to the wider field, and a "Production Perspective" that is honest about where a homelab project like this one differs from what you'd build at a company with an SRE team and a compliance department.

### How to read the labels in this handbook

Three kinds of claims appear throughout, and they are deliberately kept distinct:

- **Repository fact** — something directly observable in the code, configuration, or comments in this specific repository. These are the load-bearing claims; if a repository fact is wrong, the code it describes will show you.
- **Architectural inference** — a reasonable conclusion drawn from combining several repository facts (for example: "the two FastAPI database modules imply two independently-evolved persistence layers that haven't been unified yet"). Inferences are flagged as such and could, in principle, be wrong if there's context outside the repository that isn't visible here.
- **General engineering guidance** — industry practice, alternatives, and tradeoffs that are *not* specific to this repository at all. This is the material that generalizes to any system with a similar shape, mostly concentrated in the "Production Perspective" sections.

---

## Table of Contents

1. **[Introduction & System Overview](01-introduction-and-system-overview.md)**
   What the platform is, who it's for, and the two-path design (application path + CDC path) that everything else in this handbook builds on.

2. **[Architecture & End-to-End Data Flow](02-architecture-and-data-flow.md)**
   The full system diagram, the request lifecycle, the event lifecycle, and the fraud-detection lifecycle traced end to end.

3. **[The Core Banking API (FastAPI)](03-core-banking-api.md)**
   The HTTP gateway: routing, the two competing database layers, middleware, dependency injection, and the "Just-In-Time" data model that makes the simulator work.

4. **[PostgreSQL & the Data Model](04-postgresql-and-data-model.md)**
   The system of record, its schema, and why `wal_level=logical` is the single configuration line the rest of the platform depends on.

5. **[Change Data Capture: Debezium & Kafka Connect](05-cdc-debezium-kafka-connect.md)**
   How database rows become an event stream without touching application code, and the CDC envelope format that every downstream consumer has to understand.

6. **[Apache Kafka & Schema Registry](06-kafka-and-schema-registry.md)**
   The event backbone: topics, partitions, consumer groups, KRaft mode, and Avro schema governance.

7. **[Python Stream Processors](07-stream-processing-python.md)**
   The lightweight, stateless-ish rule engines that predate the Flink engine, and what they teach about the limits of in-memory Python stream processing.

8. **[The Apache Flink Risk Engine (Java)](08-flink-risk-engine-java.md)**
   The centerpiece of the platform: stateful, keyed, checkpointed real-time fraud scoring. The longest and most detailed chapter in this handbook.

9. **[OpenSearch: Threat Hunting & Search](09-opensearch-threat-hunting.md)**
   Turning Kafka streams into a queryable, dashboard-ready index, and the operational lessons learned from a real consumer-starvation incident.

10. **[The Simulator & Attack Scenario Engine](10-simulator-and-attack-scenarios.md)**
    Synthetic traffic generation as a first-class system component, and why realistic fraud platforms need a way to manufacture their own fraud.

11. **[Observability: Prometheus & Grafana](11-observability.md)**
    Metrics collection, the RED method, and what this platform monitors versus what a production deployment would add.

12. **[Docker Compose, Infrastructure & Deployment](12-docker-compose-and-deployment.md)**
    Container orchestration, startup-order dependencies, Docker Swarm multi-node deployment, and the GitHub Actions CI/CD pipeline.

13. **[Design Patterns & Architecture Decisions](13-design-patterns-and-architecture-decisions.md)**
    The recurring design patterns used across the codebase (and a few notable places where two different patterns compete for the same job), examined with their tradeoffs.

14. **[Appendix: Common Pitfalls, Repository Inconsistencies & Further Reading](14-appendix.md)**
    A consolidated list of the rough edges an engineer extending this system should know about, plus pointers for going deeper into each underlying technology.

15. **[Addendum: Engine Redundancy & Cluster Resource Observability](15-redundancy-and-resource-observability.md)**
    Two extensions made after the rest of this handbook was written: an active-active secondary risk engine that removes Flink as a single point of failure, and cluster-wide resource-usage monitoring via cAdvisor and node-exporter — plus a candid account of what broke during rollout and why.

---

## A map of the repository, for reference while reading

```text
banking-fraud-platform/
├── api/                     Chapter 3 — FastAPI core banking + SOC gateway (the "current" backend)
├── backend/                 Chapter 3 — an earlier, simpler FastAPI prototype (superseded, not wired into docker-compose.yml)
├── simulator/                Chapter 10 — synthetic traffic + attack scenario generator
├── analytics/
│   ├── kafka-stream-scripts/         Chapter 7 — Python rule engines (rules_engine.py, risk_scoring_engine.py)
│   ├── apache-flink-scripts/         Chapter 7 — PyFlink prototypes, superseded by the Java engine
│   ├── flink-risk-engine-java/       Chapter 8 — the production Flink risk-scoring job
│   └── opensearch/                   Chapter 9 — the OpenSearch ingestion sink daemon
├── config/, schemas/, debezium-postgres-connector.json   Chapter 5 & 6 — CDC connector spec, Avro schema
├── infra/                   Chapter 12 — Docker Swarm multi-node compose file, Postgres image build
├── prometheus/, grafana/    Chapter 11 — metrics scrape config, dashboard provisioning
├── .github/workflows/        Chapter 12 — CI/CD pipeline
├── docker-compose.yml        Chapter 2 & 12 — the single-host orchestration file this handbook uses as ground truth
└── schema.sql                 Chapter 4 — a pg_dump snapshot of the real, running schema
```

Proceed to **[Chapter 1 — Introduction & System Overview](01-introduction-and-system-overview.md)**.

# Chapter 1 — Introduction & System Overview

*Previous: [Index](00-index.md) · Next: [Chapter 2 — Architecture & Data Flow](02-architecture-and-data-flow.md)*

---

## 1.1 What this repository is

`banking-fraud-platform` is a self-contained, containerized simulation of a real-time core banking system paired with a fraud-detection pipeline. It is not a bank; there is no real money and no real customers. What it *is*, is a working demonstration of the architecture that real banks, payment processors, and fintechs use to detect fraud as it happens rather than after the fact in a nightly batch job.

**Repository fact.** The system consists of thirteen distinct runtime components wired together through `docker-compose.yml`: a PostgreSQL database, a FastAPI HTTP gateway, Kafka (running in KRaft mode, i.e. without ZooKeeper), a Schema Registry, Kafka Connect running the Debezium PostgreSQL connector, an Apache Flink cluster (JobManager + TaskManager) running a Java risk-scoring job, an OpenSearch cluster with its Dashboards UI, a Python OpenSearch ingestion sink, a traffic simulator, Prometheus, Grafana, and a Kafka metrics exporter.

If you have never worked with an event-driven system before, the natural question is: *why so many moving parts for something that could, in principle, just be "check the transaction, flag it if it looks bad"?* Answering that question honestly is the point of this chapter, and it sets up everything that follows.

## 1.2 The problem: fraud detection is a latency problem wearing a machine-learning costume

A transaction-monitoring rule is not hard to write. "Flag any transfer over $10,000" is one line of code in any language. The actual engineering difficulty in fraud detection is almost entirely about **when** you can know something, not **what** you know.

Consider a single fraudulent wire transfer. To catch it with any confidence, you typically need to correlate several signals that arrive from different places at different times:

- The transaction itself (amount, source account, destination account).
- The login that preceded it (was this a new device? A new country? A login after several failed attempts?).
- The account's recent history (has this identity done anything unusual in the last few minutes? Hours? Days?).
- Possibly, entity-relationship context (is the destination account linked to other accounts already known to be compromised?).

A batch job that runs once a night can answer all of these questions perfectly — with the history sitting in a data warehouse, you can join everything, in any order, and compute whatever score you like. The catch is that the fraud has already happened by the time the job runs. If you want to lock an account or block a transfer *before* the money leaves, the scoring has to happen within the same order of magnitude of time as the transaction itself: seconds, not hours.

This is the fundamental tension that shapes every architectural decision in this repository: **you need historical context (which wants batch-style joins across large amounts of data) available at the moment a single new event arrives (which wants sub-second, streaming-style processing)**. The entire pipeline — CDC, Kafka, Flink's keyed state — exists to resolve that tension.

## 1.3 The two-path design

**Repository fact.** The system is built around two parallel paths that both terminate in the same fraud-scoring engine but start from different origins:

1. **The Application Path.** The FastAPI gateway (`api/app/main.py`) handles HTTP requests — logins, transfers, account lookups — and, as a side effect of every request, publishes a lightweight telemetry event to a Kafka topic called `api_requests`. See `api/app/middleware/kafka_logger.py`.

2. **The CDC Path.** Independently of the API, every row-level change to key PostgreSQL tables (`accounts`, `transactions`, `login_events`, and others) is captured by Debezium reading PostgreSQL's Write-Ahead Log, and republished as a Kafka event — again, without any application code being aware this is happening.

Both paths feed into Apache Flink, which treats them as one unified stream keyed by customer identity, and both paths also feed into OpenSearch for search and dashboarding.

**Architectural inference.** Having two paths that both produce roughly the same information (an API request happened; a database row changed) looks redundant at first glance, but it isn't. It's a deliberate hedge against a single point of failure in event capture. The application-level telemetry (`api_requests`) captures things that never touch the database at all — like a burst of API calls to an endpoint, or a request that gets rejected before a row would ever be written. The CDC path captures the ground truth of what was actually persisted, *including* changes made outside the API entirely (a direct `psql` mutation, a future admin tool, a data migration script) — which application-level logging can never see, because it only fires when application code runs. A production fraud system that only logged at the application layer would be blind to anything that touched the database through a different path. This repository's CDC path exists specifically to close that blind spot; you can see this decision reasoning laid out explicitly in the repository's own README, and it is why Debezium sits at the center of the architecture (Chapter 5) rather than being an optional add-on.

## 1.4 Why not just query the database directly for fraud scoring?

If PostgreSQL already has all the data, why not have the fraud engine just run a `SELECT ... WHERE customer_id = ?` on every incoming transaction and compute a score from that?

This is the second foundational tradeoff worth understanding before diving into the rest of the system:

- **Coupling and blast radius.** A fraud-scoring service that queries the production banking database directly is now a dependency *of* that database's load, and the database is now a dependency *of* the fraud service's availability. A slow fraud query can degrade the bank's core transaction throughput. A streaming architecture decouples these: Flink never touches PostgreSQL directly for the transactional hot path — it reads from Kafka, which acts as a buffer and a shock absorber.
- **State locality.** Repeatedly querying "how many transactions has this customer made in the last 5 minutes?" against a relational database, for every single transaction, at any real scale, turns into a database-hammering hot-path query. Flink's keyed state (Chapter 8) keeps a small, per-customer rolling window *in memory*, updated incrementally as events arrive, so the "last 5 minutes of activity" answer is a local state lookup, not a network round trip and a table scan.
- **Replayability.** Kafka retains events for a configurable period. If you need to reprocess the last 24 hours of activity with an improved scoring algorithm, you can rewind the Flink job's consumer offset and replay history, deterministically, without re-running the entire application. A direct-query architecture has no equivalent "replay" button.

**General engineering guidance.** This is the classic argument for **event-driven architecture (EDA)** over **request-driven, synchronous architecture** for anything that needs both high write throughput and continuous derived computation (fraud scores, recommendation feeds, real-time analytics). The tradeoff is added operational complexity — you now have to run and monitor a message broker, a stream processor, and (in this system's case) a schema registry and a CDC connector, instead of "just" a database and an API. Chapter 13 discusses this tradeoff in more depth alongside the other architectural decisions.

## 1.5 What "real-time" means in this system, precisely

**Repository fact.** The Flink job's watermark strategy allows events up to 300 seconds (5 minutes) of out-of-order arrival before being considered late, and treats a key as idle after 60 seconds of no events (`RiskScoringJob.java`). Kafka's `KafkaSource` builders across the codebase are configured with `OffsetsInitializer.earliest()`, meaning a fresh job replays the entire topic history from the beginning rather than starting from "now." The alert cooldown after a triggered fraud alert is 15 minutes (`alert.cooldown-ms=900000` in `risk-scoring.properties`), and state is decayed on a 30-minute half-life and expires entirely (TTL) after 24 hours.

None of this is sub-millisecond, high-frequency-trading-grade "real-time." It is **near-real-time, streaming, event-at-a-time processing with a tolerance for bounded out-of-order delivery** — the correct and common meaning of "real-time" in fraud detection, where the relevant comparison is "before the transaction clears" (seconds to low minutes), not "before the next CPU cycle."

## 1.6 Who this handbook assumes you are

This handbook assumes you're comfortable writing code and reading it in at least one mainstream language, and that you've built or maintained a typical CRUD web service before (a REST API backed by a relational database). It does **not** assume prior exposure to Kafka, Flink, Debezium, or event-driven design — those are taught from the ground up, using this repository as the worked example, starting in earnest in Chapter 5.

If you already know these technologies well, the "How this project implements it" and "Runtime Behavior" sections of each chapter are where the repository-specific value is; the "What it is" and "Why it exists" sections can be skimmed.

## 1.7 Learning Concepts

- **Latency vs. correctness tradeoff in fraud detection.** Every fraud system trades some analytical completeness (the kind you get from an offline batch job with full historical joins) for speed (the kind you get from streaming, keyed, incremental state).
- **Event-driven architecture (EDA).** A system design style where components communicate by producing and consuming events through a durable log, rather than by calling each other's APIs directly.
- **Change Data Capture (CDC).** Capturing row-level database mutations as a stream of events, sourced from the database's internal replication log rather than application-level instrumentation.
- **Decoupling and blast-radius containment.** Using a message broker as a buffer between producers and consumers so that a slowdown or failure on one side doesn't directly propagate to the other.
- **Replayability.** The ability to reprocess historical events deterministically, made possible by a durable, offset-addressable event log (Kafka) rather than a fire-and-forget message queue.

## 1.8 Production Perspective

This architecture — API + CDC → Kafka → stream processor → search index — is a real, industry-standard pattern, and variations of it run at scale inside payment processors, banks, and any company that needs to react to database changes in near-real-time (this pattern is often called the "outbox pattern" or "CDC-based event sourcing" when discussed generically). Companies that need this shape of system typically reach for:

- **Debezium** itself (used here) is genuinely production-grade and is the de facto standard open-source CDC tool; Confluent, Redpanda, and various cloud vendors also offer managed CDC connectors built on similar WAL-tailing techniques.
- **Kafka** at true production scale is usually run as a managed service (Confluent Cloud, AWS MSK, Redpanda Cloud) rather than self-hosted, specifically to avoid needing in-house expertise in partition rebalancing, ISR management, and broker capacity planning.
- **Flink** is a legitimate, widely-used production choice for exactly this workload (Alibaba, Uber, Netflix, and many financial institutions run Flink for real-time risk and personalization). The alternatives most commonly discussed alongside it — Kafka Streams (lighter-weight, embedded in the application JVM rather than a separate cluster) and Spark Structured Streaming (batch-oriented micro-batching rather than true event-at-a-time) — are covered in depth in Chapter 8's Production Perspective section, where the tradeoffs actually matter for the code you'll read there.

What a production deployment of *this specific repository* would still need before handling real money is covered honestly throughout this handbook rather than glossed over — see in particular the security notes in Chapters 6 and 9 (hardcoded demo credentials, self-signed TLS certificates, disabled certificate verification) and the reliability notes in Chapter 12 (single-broker Kafka, single-node OpenSearch, no multi-AZ anything). This is appropriate and expected for a homelab / portfolio project; the point of flagging it explicitly is so the distinction between "architecturally sound pattern" and "production-hardened deployment" stays clear as you read.

---

*Next: [Chapter 2 — Architecture & End-to-End Data Flow](02-architecture-and-data-flow.md)*

# Chapter 2 — Architecture & End-to-End Data Flow

*Previous: [Chapter 1 — Introduction](01-introduction-and-system-overview.md) · Next: [Chapter 3 — The Core Banking API](03-core-banking-api.md)*

---

## 2.1 The component diagram

**Repository fact.** The following diagram is derived directly from `docker-compose.yml` — every box is a real container, every arrow is a real network dependency (`depends_on`) or data relationship visible in the code.

```text
                         ┌───────────────────────────┐
                         │   Simulator (Python)      │
                         │  customer/employee actors  │
                         │  + attack scenario engine  │
                         └─────────────┬──────────────┘
                                       │ HTTP (httpx)
                                       ▼
   ┌───────────────────────────────────────────────────────────────────┐
   │                    FastAPI Core Banking API (bank_api)             │
   │  /api/login  /api/transfers  /api/beneficiaries  /api/employee/*   │
   │  /incidents  /analytics/metrics  /api/graph/*  /api/demo/*         │
   │                                                                     │
   │  KafkaRequestLoggerMiddleware ──────► api_requests topic (Avro)    │
   │  Prometheus metrics middleware ─────► /metrics endpoint             │
   └───────────┬───────────────────────────────────────────┬───────────┘
               │ SQL (SQLAlchemy, async + sync)              │
               ▼                                             │
   ┌────────────────────────┐                                │
   │  PostgreSQL 15          │                                │
   │  wal_level=logical       │                                │
   │  (bank_postgres)          │                                │
   └───────────┬──────────────┘                                │
               │ logical replication (WAL)                     │
               ▼                                               │
   ┌────────────────────────────┐                              │
   │  Kafka Connect + Debezium   │                              │
   │  (bank_connect)               │                              │
   │  banking-cdc-connector          │                              │
   └───────────┬────────────────────┘                              │
               │ CDC events: banking.accounts, banking.transactions,│
               │ banking.login_events, banking.sessions,             │
               │ banking.beneficiaries, banking.employee_actions      │
               ▼                                                     ▼
   ┌───────────────────────────────────────────────────────────────────┐
   │                     Apache Kafka (KRaft mode, bank_kafka)           │
   │   Topics: api_requests · dead_letter_queue · fraud_alerts_v2 ·      │
   │   critical_alerts_v2 · banking.* CDC topics · (legacy topics)       │
   └───────┬───────────────────────────────────┬─────────────────┬─────┘
           │                                   │                  │
           │ consumes                          │ consumes         │ consumes
           ▼                                   ▼                  ▼
┌────────────────────────┐   ┌───────────────────────────┐  ┌─────────────────────┐
│ Apache Flink Cluster    │   │ Python Stream Scripts       │  │ OpenSearch Sink        │
│ (JobManager+TaskManager)  │   │ (rules_engine.py,             │  │ (bank_opensearch_sink)  │
│ RiskScoringJob.java        │   │  risk_scoring_engine.py)       │  │ one thread per topic      │
│ → fraud_alerts_v2            │   │ → fraud_alerts / high_risk_alerts│  │ → OpenSearch indices       │
│ → critical_alerts_v2           │   └───────────────────────────┘  └──────────┬────────────┘
└───────────┬─────────────────┘                                                 │
            │ Avro (Schema Registry)                                              ▼
            ▼                                                          ┌────────────────────┐
┌───────────────────────────┐                                          │ OpenSearch (bank_opensearch) │
│ alert_aggregator.py worker  │◄─────────────── consumes fraud_alerts_v2 │ indices: api_requests,        │
│ (dedupe alerts → Incidents)  │                                          │ login_events, employee_actions, │
└───────────┬─────────────────┘                                          │ alerts                          │
            │ writes                                                     └───────────┬────────────────────┘
            ▼                                                                        │
┌────────────────────────────┐                                            ┌──────────▼─────────────┐
│ PostgreSQL: incidents,        │                                            │ OpenSearch Dashboards     │
│ incident_alerts,                │                                            │ (bank_opensearch_dashboards)│
│ incident_audit_logs table         │                                            └─────────────────────────┘
└────────────────────────────┘

        ┌──────────────────────────────────────────────────────────┐
        │                  Observability Plane                       │
        │  Prometheus (scrapes bank_api, Flink JM, Kafka Exporter)    │
        │  ─► Grafana dashboards                                        │
        │  Kafka Exporter (bank_kafka_exporter) ─► Kafka broker metrics  │
        └──────────────────────────────────────────────────────────┘
```

A few things in this diagram are worth calling attention to explicitly because they are easy to miss on a first read:

**Architectural inference.** The Python stream scripts (`rules_engine.py`, `risk_scoring_engine.py`) and the Java Flink engine both consume from Kafka and both compute fraud risk, but they are not wired together in `docker-compose.yml` at all — neither script has a container definition. They exist in the repository as earlier iterations of the same idea (Chapter 7 covers this in detail), and the Java Flink engine (`fraud-engine` service) is the one actually deployed and running. This is a common and healthy pattern in a project's history — build a lightweight prototype first, validate the idea works, then rebuild it properly — but it means a reader exploring the `analytics/` directory needs to know which code is live and which is historical, which this handbook makes explicit throughout.

## 2.2 The request lifecycle (a single API call, traced end to end)

This section traces one HTTP request — `POST /api/transfers` — through every system it touches, in the order things actually happen.

```text
 1. Simulator (or a real client) sends:
    POST /api/transfers  { from_account, to_account, amount, currency }
                    │
                    ▼
 2. FastAPI receives the request.
    KafkaRequestLoggerMiddleware.dispatch() runs FIRST (it wraps call_next).
                    │
                    ▼
 3. call_next(request) invokes the route handler:
    execute_transfer() in app/api/transfers.py
      - _ensure_account_exists() JIT-creates missing Customer/Account rows
        if the simulator references accounts that don't exist yet
      - debits source_acc.balance, credits target_acc.balance
      - INSERTs a new row into the `transactions` table
      - db.commit()
                    │
                    ▼
 4. PostgreSQL's Write-Ahead Log (WAL) records the INSERT.
    (This step happens independently of steps 5-6 below — Postgres doesn't
    know or care that Debezium exists; it just writes its WAL as always.)
                    │
                    ▼
 5. Route handler returns a TransactionResponse. FastAPI serializes it to JSON.
                    │
                    ▼
 6. Control returns to KafkaRequestLoggerMiddleware's `finally` block.
    A background asyncio task is scheduled (NOT awaited) to call
    send_telemetry_event() with {request_id, ip_address, endpoint, method,
    status_code, timestamp}. The HTTP response is sent to the client
    immediately — the client never waits for step 7.
                    │
                    ▼
 7. (Background, off the request path) send_telemetry_event() Avro-encodes
    the payload against the api_request.avsc schema and produces it to the
    `api_requests` Kafka topic. On any serialization failure, it produces a
    JSON fallback to `dead_letter_queue` instead of dropping the event.

 --- MEANWHILE, ASYNCHRONOUSLY, ON A COMPLETELY SEPARATE PATH ---

 8. Debezium's PostgreSQL connector, which is continuously tailing the WAL
    via a logical replication slot, picks up the INSERT from step 4 within
    milliseconds of it being committed (not tied to the HTTP request at all).
                    │
                    ▼
 9. Debezium emits a CDC envelope { before: null, after: {...row...},
    source: {table: "transactions", ...}, op: "c", ts_ms: ... } to the
    `banking.transactions` Kafka topic.
                    │
                    ▼
10. Both the api_requests event (step 7) and the banking.transactions CDC
    event (step 9) are now sitting in Kafka, available to any consumer:
    the Flink risk engine, the Python stream scripts, and the OpenSearch
    sink all pick them up independently, on their own schedules.
```

The single most important thing to notice in this trace is that **steps 6-7 (telemetry) and steps 8-9 (CDC) are on two entirely different clocks**, and neither blocks the HTTP response the client sees in step 5. The client gets their transfer confirmation as fast as PostgreSQL can commit a row; the fraud-detection machinery downstream operates on a separate, asynchronous timeline, typically resolving to an alert within seconds — but critically, *after* the money has already moved. This is a direct, concrete illustration of the latency/correctness tradeoff discussed in Chapter 1: catching this specific fraud pattern in-line, before the transfer completed, is architecturally possible (Flink could theoretically score it before the API responds) but this codebase does not do that — it optimizes for API responsiveness and detects fraud as a fast-follow reaction instead. Chapter 8 discusses this choice, and what would need to change to move it in-line, in the Production Perspective section.

## 2.3 The event lifecycle: from raw bytes to a fraud alert

This is the same journey as above, but from Kafka's perspective outward — how one CDC event becomes, potentially, a fraud alert that lands in an analyst's incident queue.

```text
banking.login_events topic
        │
        │  {"before":null,"after":{"customer_id":"cust_gen_4","device_id":"...",
        │   "country":"RU","ip_address":"185.220.101.5","success":true, ...},
        │   "source":{"table":"login_events",...},"op":"c","ts_ms":1783395584407}
        ▼
┌─────────────────────────────────────────────────────────────────┐
│  RiskEventNormalizer.normalize()  (Flink, Java)                     │
│  - unwraps the CDC envelope, reads the "after" block                  │
│  - extracts customer_id → identityKey = "cust_gen_4"                   │
│  - extracts country, device_id, login success flag                       │
│  → RiskEvent object                                                        │
└───────────────────────────┬─────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  AccountEnrichmentFunction (broadcast join against banking.accounts) │
│  - for login_events this is a no-op passthrough (customerId already   │
│    present); this stage matters for `transactions`, which carries       │
│    from_account/to_account but no customer_id of its own                  │
└───────────────────────────┬─────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  OpenSearchLookupFunction (async I/O, non-blocking)                  │
│  - queries OpenSearch: has this identity appeared BEFORE the 24h        │
│    Flink state window? (purely additive long-term context; result        │
│    is not yet wired into scoring — see Chapter 8 §8.5)                     │
└───────────────────────────┬─────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  keyBy(identityKey) → RiskScoringProcessor.processElement()          │
│  1. dedup check against seenEventKeys (skip if already processed)        │
│  2. apply time-based score decay since last update                        │
│  3. deriveIndicators(): compare this login's country/device against       │
│     the stored RiskProfileState → e.g. NEW_COUNTRY, IMPOSSIBLE_TRAVEL       │
│  4. sum indicator weights into cumulativeScore, update state                │
│  5. if cumulativeScore > ALERT_THRESHOLD (80) and cooldown has expired:      │
│     emit a FraudAlert (severity FRAUD, or CRITICAL if score >= 100)           │
└───────────────────────────┬─────────────────────────────────────┘
                             ▼
                fraud_alerts_v2 / critical_alerts_v2 (Avro, Schema Registry)
                             │
              ┌──────────────┴───────────────┐
              ▼                              ▼
┌───────────────────────────┐   ┌─────────────────────────────────┐
│  alert_aggregator.py worker  │   │  OpenSearch Sink (opensearch_sink.py)│
│  - deduplicates repeat alerts  │   │  - both topics route into the "alerts" │
│    for the same identity into    │   │    OpenSearch index                       │
│    a single Incident within a       │   └─────────────────────────────────┘
│    30-minute window                    ▼
│  - writes Incident + IncidentAlert    OpenSearch Dashboards / /incidents API
│    rows to PostgreSQL                  (analyst-facing surfaces)
└───────────────────────────┘
```

## 2.4 The fraud detection lifecycle, in plain terms

Stepping back from the mechanics, here is the same flow described as a sequence of *decisions*, which is the more useful mental model once you understand the plumbing:

1. **Something happens** (a login, a transfer, an API call). This is captured either by the CDC path or the application-telemetry path — see §2.2.
2. **The event is normalized** into a common shape (`RiskEvent`) regardless of which of the four source tables/topics it came from, so that one scoring processor can handle all of them uniformly.
3. **The event is enriched** with context it didn't carry on its own — most importantly, resolving `account_id → customer_id` for transactions via the broadcast join, since a `transactions` row has no `customer_id` column at all (confirmed against the real schema — see Chapter 8, §8.3).
4. **The event is compared against the entity's recent history**, held in Flink's keyed state — not a query, a local lookup, because the state is partitioned by identity and lives with the task that processes that identity's events.
5. **Indicators are derived** — human-readable labels like `NEW_DEVICE`, `IMPOSSIBLE_TRAVEL`, `LARGE_TRANSFER_AMOUNT` — each carrying a configured point weight.
6. **A cumulative score accumulates and decays over time**, so that a burst of suspicious activity in a short window scores higher than the same events spread over days, and old risk fades rather than accumulating forever.
7. **A threshold crossing produces an alert**, subject to a cooldown so the same identity doesn't spam a new alert every single event once it's already over the line.
8. **Alerts are deduplicated into Incidents** for human analysts, so that fifty related alerts about the same account takeover become one case to investigate, not fifty.

## 2.5 The incident lifecycle (post-detection)

**Repository fact.** Once a `FraudAlert` reaches `alert_aggregator.py` (`api/app/workers/alert_aggregator.py`), it is either merged into an existing open `Incident` for the same `identity_key` (if one was updated within the last 30 minutes) or a new `Incident` is created. From there, the incident follows an explicit state machine defined in `api/app/models/incidents.py`:

```text
        ┌────────┐   assign_incident()    ┌────────────────┐
        │  OPEN  │ ───────────────────────►│  INVESTIGATING  │
        └───┬────┘                          └────────┬────────┘
            │                                          │
            │  update_incident_status()                │  update_incident_status()
            │                                          │
            ▼                                          ▼
   ┌─────────────────┐                        ┌──────────────────┐
   │ FALSE_POSITIVE    │                        │  CONFIRMED_FRAUD    │
   └─────────────────┘                        └──────────────────┘
              (both are terminal states, alongside RESOLVED_OTHER)
```

Every transition is recorded as an `IncidentAuditLog` row with the previous status, new status, who made the change, and optional notes (`api/app/api/incidents.py`) — this is what lets `AnalyticsService.compute_metrics()` (Chapter 3) later compute precision and false-positive-rate against ground truth, since `CONFIRMED_FRAUD` vs `FALSE_POSITIVE` labels, applied by a human analyst, are exactly the ground-truth labels a fraud model's precision is measured against.

## 2.6 Learning Concepts

- **Component diagram vs. sequence diagram.** A component diagram (§2.1) shows *what exists and how it's connected*; a sequence/lifecycle diagram (§2.2-2.3) shows *what happens, in order, over time*. Both are necessary to actually understand a distributed system — the first without the second tells you nothing about latency or ordering; the second without the first has no context for why a given hop exists.
- **Fan-out.** A single event (one Kafka message) is independently consumed by multiple, unrelated downstream systems (Flink, the OpenSearch sink, potentially the Python scripts) without any of them coordinating with each other or even being aware of each other's existence. This is one of the core benefits of a pub/sub log over point-to-point messaging.
- **Asynchronous, non-blocking side effects.** The telemetry-publishing step in the request lifecycle is deliberately decoupled from the request/response cycle (via `asyncio.create_task`), a pattern sometimes called "fire-and-forget" — though as Chapter 3 details, this pattern has a specific, real garbage-collection pitfall that this exact repository hit and fixed.
- **State machines for business processes.** The `Incident` status field isn't just an enum column — it's a formally defined state machine with recorded transitions, which is what makes it auditable and analyzable later.

## 2.7 Production Perspective

The diagrams in this chapter describe a **single-host, single-broker, single-node-everything** topology (`docker-compose.yml`), appropriate for a demo or a homelab. A production deployment of the same *logical* architecture would typically differ in the following ways, none of which change the diagrams' shape, only the redundancy and scale behind each box:

- **Kafka** would run as a multi-broker cluster with a replication factor greater than 1 (this repository uses replication factor 1 everywhere — see the `kafka-init` topic-creation commands in `docker-compose.yml` — meaning a single broker failure loses data, which is acceptable for a demo and not for a bank).
- **PostgreSQL** would run with a standby replica and Debezium would typically read from the replica or a dedicated logical-replication-only role, to avoid the CDC connector competing for resources with live transactional traffic.
- **Flink** would run with parallelism greater than 1 (this job explicitly sets `env.setParallelism(1)` — see Chapter 8, §8.5 for why that's a meaningful and deliberate simplification here, not an oversight) and would checkpoint to durable, replicated storage (S3, HDFS) rather than local disk.
- **OpenSearch** would run as a multi-node cluster with index replicas, rather than the single-node `discovery.type=single-node` configuration used here.

What would *not* fundamentally change is the shape of the pipeline itself — application-path + CDC-path → Kafka → stream processor → search index is a durable, well-proven pattern regardless of the redundancy behind each stage, which is exactly why it's worth learning from a simplified, single-host version of it.

---

*Next: [Chapter 3 — The Core Banking API (FastAPI)](03-core-banking-api.md)*

# Chapter 3 — The Core Banking API (FastAPI)

*Previous: [Chapter 2 — Architecture & Data Flow](02-architecture-and-data-flow.md) · Next: [Chapter 4 — PostgreSQL & the Data Model](04-postgresql-and-data-model.md)*

---

## 3.1 What it is

FastAPI is a Python web framework built on top of Starlette (ASGI) and Pydantic. It's the HTTP front door of this entire platform: every login, transfer, account lookup, and administrative action a "customer" or "employee" performs goes through it. If you've used Flask or Express before, FastAPI will feel familiar with two additions that matter a great deal for this codebase: it's **async-native** (routes can be `async def` and run on an event loop rather than a thread pool), and it uses Python type hints plus Pydantic models to validate request/response bodies automatically, generating an OpenAPI schema (and the interactive `/docs` UI) for free.

## 3.2 Why it exists (in this system, specifically)

A fraud-detection pipeline needs *something* generating and receiving the transactions it's supposed to detect fraud within. That's this API's job: it's the "front of house" that makes the whole system feel like a bank rather than an abstract stream-processing exercise. But it's also — and this is easy to overlook — the **entry point for the application-telemetry half of the two-path design** described in Chapter 1. Every request that flows through it is a potential source of the `api_requests` Kafka topic, which is one of the four inputs Flink scores against.

**Why FastAPI specifically, rather than, say, Flask or a Java/Spring service?** Two reasons visible in the code: async I/O maps naturally onto "wait on a database query, wait on a Kafka produce call" workloads without needing a thread pool per request, and Pydantic's validation gives the simulator (Chapter 10) a predictable, strictly-typed contract to generate synthetic traffic against.

## 3.3 How this project implements it

### 3.3.1 Two competing implementations, one deployed

**Repository fact.** There are, in fact, *two separate FastAPI applications* in this repository: `api/app/main.py` (and its supporting `api/app/api/*.py` routers) and `backend/app/main.py` (with `backend/app/routers/*.py`). Only `api/` is built and run by `docker-compose.yml` — the `api` service's build context is `./api`. The `backend/` directory has no service definition anywhere in `docker-compose.yml`.

**Architectural inference.** `backend/` reads as an earlier, simpler prototype: `backend/app/main.py` uses a synchronous Kafka `Producer` called directly inside a single global HTTP middleware, with no schema registry integration, no Avro, and none of the account/beneficiary/employee routers that `api/` has. It looks like the starting point that `api/` grew out of, left in the repository rather than deleted — a common and harmless thing to happen during iterative development, but worth knowing about explicitly so you don't spend time debugging code that isn't actually running. This handbook's remaining discussion of "the API" refers to `api/` unless stated otherwise.

### 3.3.2 Application startup

**Repository fact.** `api/app/main.py` builds an async SQLAlchemy engine from `DATABASE_URL` (falling back to individually-set `POSTGRES_*` env vars if absent), registers a `KafkaRequestLoggerMiddleware`, registers a Prometheus metrics middleware, and defines an `@app.on_event("startup")` hook, `startup_pipeline_provisioning()`, that:

1. Runs `Base.metadata.create_all` against the target database — i.e., the API itself owns schema creation via SQLAlchemy DDL generation, rather than a separate migration step running first (Alembic migrations also exist — see §3.3.5 — but table creation itself happens here, at boot, every time).
2. Runs an explicit `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for `latitude`/`longitude` on `login_events` — a manual, idempotent, code-level schema patch, applied every startup, layered on top of the SQLAlchemy-generated schema.
3. Seeds baseline data: three "fallback" system customers (`cust_external_mule`, `cust_unknown_fallback`, `SYSTEM_TELLER`) that always exist regardless of anything else, then — only if no `cust_gen_%` rows exist yet — twenty simulated customer/account pairs and two VIP executive target accounts with $2.5M balances, explicitly there to be attractive targets for the simulator's attack scenarios (Chapter 10).

```python
@app.on_event("startup")
async def startup_pipeline_provisioning():
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
        await conn.execute(text("""
            ALTER TABLE public.login_events
            ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;
        """))
```

**Architectural inference.** Running `CREATE TABLE IF NOT EXISTS`-equivalent DDL directly from application startup code, rather than exclusively through Alembic migrations, is a pragmatic-but-non-standard choice: it means the schema is defined in two places (the SQLAlchemy model classes *and* the Alembic migration for `incidents` — see §3.3.5) that can drift out of sync. For a fast-moving demo project this trades schema-management rigor for zero-friction "just start the container and it works" bootstrapping. Chapter 13 discusses this alongside the codebase's other patterns.

### 3.3.3 Two database access patterns, side by side

This is the single most important implementation detail to understand about `api/app/`, because it explains a recurring pattern you'll see across nearly every router file.

**Repository fact.** There are two independent database configuration modules:

- `api/app/core/database.py` — an **async** engine (`create_async_engine`, `asyncpg` driver), its own `Base = declarative_base()`, and an async `get_db()` dependency.
- `api/app/database.py` — a **synchronous** engine (`create_engine`, `psycopg2` driver), a *different* `Base = declarative_base()`, and a synchronous `get_db()` dependency.

Both `Base` objects are distinct Python objects. `api/app/models/domain.py` (Customer, Account, Transaction, LoginEvent, Card, etc.) inherits from `app.core.database.Base` — the async one. `api/app/models/incidents.py` (Incident, IncidentAlert, IncidentAuditLog) inherits from `app.database.Base` — the sync one.

Routers split cleanly along this same line:

| Router | DB module used | Session type |
|---|---|---|
| `accounts.py`, `auth.py`, `cards.py`, `employee.py`, `security.py`, `transfers.py`, `users.py` | `app.database` or a mix | Synchronous `Session` |
| `analytics.py`, `graph.py`, `incidents.py` | `app.core.database` | Async `AsyncSession` |
| `main.py` (inline routes: `/api/login`, `/api/transfers`, `/api/accounts/{id}`) | `app.core.database` | Async `AsyncSession` |

**Architectural inference.** This is best read as two features built at different times by different priorities converging on the same application without ever being unified: the original core-banking surface (accounts, transfers, logins) was built synchronously; the newer investigative/analytical surface (incident case management, entity graph traversal) was built async, likely because those endpoints do more genuinely I/O- and CPU-bound work (see §3.3.4) where async's benefits are more pronounced. Both work, because FastAPI supports mixing sync and async route handlers in the same app (sync handlers are automatically run in a thread pool so they don't block the event loop) — but it means the codebase has two data-access idioms rather than one, and two `Base.metadata` registries that a single `create_all` call can't unify (a subtlety that also means `create_all` in `main.py`'s startup hook, bound to the async engine, only creates the tables under the async `Base` — the `incidents`/`incident_alerts`/`incident_audit_logs` tables are provisioned through the Alembic migration in §3.3.5 instead, not through this startup hook).

### 3.3.4 The "Just-In-Time" (JIT) provisioning pattern

**Repository fact.** Across nearly every write-path router, the same defensive pattern recurs: if a referenced `Account` or `Customer` doesn't exist yet, create it on the spot with plausible placeholder data, rather than returning a 404 or 400. For example, `_ensure_account_exists()` in `transfers.py`:

```python
def _ensure_account_exists(db: Session, account_id: str, currency: str) -> Account:
    """Just-In-Time (JIT) creation for simulated records.
    Resolves cascading Foreign Key constraints (Customer -> Account -> Transaction)."""
    acc = db.query(Account).filter(Account.account_id == account_id).first()
    if not acc:
        customer_id = f"SIM-CUST-{account_id[-6:]}"
        cust = db.query(Customer).filter(Customer.customer_id == customer_id).first()
        if not cust:
            cust = Customer(customer_id=customer_id, first_name="Simulated", last_name="User",
                             email=f"{customer_id.lower()}@simulator.local", country="US")
            db.add(cust); db.flush()
        acc = Account(account_id=account_id, customer_id=customer_id,
                       balance=Decimal("50000.00"), currency=currency, status="ACTIVE")
        db.add(acc); db.flush()
    return acc
```

The same idea appears independently in `accounts.py` (returns a mock `AccountBalanceResponse` if no row exists, rather than JIT-writing one), `cards.py` (returns a fabricated `CardResponse` on any exception), and inline in `main.py`'s `/api/login` and `/api/transfers` handlers. Notably, `execute_transfer()` in `transfers.py` also **auto-replenishes** a source account's balance with "Magic simulator money!" if it's about to go negative, rather than rejecting the transfer.

**Architectural inference.** This is not an accident or sloppiness — it's a deliberate design accommodation for the fact that the simulator (Chapter 10) generates traffic referencing accounts by ID *before* those accounts are guaranteed to exist in the database (e.g., an attack scenario targeting `ACC-GEN00003` doesn't first check whether that account was already seeded). JIT provisioning means the simulator can fire arbitrary attack traffic without needing to coordinate account creation timing with the API, at the cost of the API being extremely permissive about invalid input — a tradeoff that is completely appropriate for a synthetic-data-generation platform and would be a serious problem in a real bank's API (Chapter 13 revisits this).

### 3.3.5 Alembic migrations, alongside `create_all`

**Repository fact.** `api/alembic/versions/002_create_incident_management_tables.py` is a hand-written Alembic migration creating the `incidents`, `incident_alerts`, and `incident_audit_logs` tables with their enums, foreign keys, and indexes — the tables backing the incident-management workflow described in Chapter 2, §2.5. This coexists with the `Base.metadata.create_all()` call in the startup hook (§3.3.2), which handles the `domain.py` models instead. In other words: the "older" core-banking schema is created imperatively at every app boot; the "newer" incident-management schema is created explicitly via a versioned migration, run separately (`alembic upgrade head`).

### 3.3.6 Middleware: how a request becomes a Kafka event without any route handler knowing

This is worth walking through carefully, because it's a genuinely subtle piece of code with a documented, fixed bug in its history.

**Repository fact.** `KafkaRequestLoggerMiddleware` (`api/app/middleware/kafka_logger.py`) is a Starlette `BaseHTTPMiddleware` subclass. Its `dispatch()` method wraps every request: it calls `call_next(request)` to let the route handler run, captures the response, and — in a `finally` block, so this runs whether the handler succeeded or raised — builds a telemetry payload and schedules a background task to publish it:

```python
_background_tasks: set[asyncio.Task] = set()

class KafkaRequestLoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        ...
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            telemetry_payload = { "request_id": ..., "ip_address": ..., ... }
            task = asyncio.create_task(
                asyncio.to_thread(send_telemetry_event, telemetry_payload)
            )
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
```

The comment directly above `_background_tasks` explains why the module-level `set` exists at all:

> `asyncio.create_task()` only keeps a WEAK reference to the Task internally. If the caller doesn't hold its own strong reference somewhere, the task is eligible for garbage collection before the event loop ever runs it — this was the root cause of `api_requests` sitting at zero messages despite live traffic.

This is a real, documented incident in this repository's development history, and it's worth understanding as a concrete lesson in async Python: `asyncio.create_task(coro)` schedules `coro` to run, but if nothing keeps a reference to the returned `Task` object, Python's garbage collector is free to collect it — including *before it ever executes* — because `asyncio`'s internal bookkeeping only holds a *weak* reference. The fix is exactly what's shown above: keep tasks in a module-level, strongly-referenced `set`, and remove them via `add_done_callback` once they finish, so they're neither leaked forever nor collected prematurely.

`send_telemetry_event()` itself lives in `api/app/middleware/kafka_producer.py`. It loads the `api_request.avsc` Avro schema (walking up to four directory levels looking for it, to work in both local-dev and Docker layouts — see `find_schema_path()`), builds an `AvroSerializer` bound to the Schema Registry, and on any serialization failure, **fails open to a dead-letter queue** rather than dropping the event silently:

```python
except Exception as validation_error:
    dlq_payload = {"error_message": str(validation_error), "failed_at": ..., "raw_payload": json.dumps(payload)}
    producer.produce(topic="dead_letter_queue", key=..., value=string_serializer(json.dumps(dlq_payload)), callback=delivery_report)
```

### 3.3.7 The entity graph and ring-detection services

**Repository fact.** `GraphBuilderService.build_graph()` (`api/app/services/graph_builder.py`) queries `Account`, `Transaction`, `Device`, and `LoginEvent` rows (asynchronously) and constructs an in-memory `networkx.MultiDiGraph`, with typed nodes (`CUSTOMER`, `ACCOUNT`, `DEVICE`, `IP`) and typed edges (`OWNS`, `TRANSFERRED_TO`, `USED_DEVICE`, `LOGGED_IN_FROM`). `RingDetectorService.analyze_graph()` (`ring_detector.py`) then runs two graph algorithms over it: `networkx.simple_cycles()` to find bounded-length circular transfer chains (a classic money-laundering "smurfing" pattern — money moves in a loop back to its origin through intermediaries), and a shared-infrastructure scan that flags any `Device` or `IP` node connected to more customer accounts than a configurable threshold (a classic fraud-ring signature — many "different" customers who are actually all controlled by one attacker sharing a device or IP).

**Repository fact.** Both the graph construction (an I/O-bound DB query) and the graph algorithms (a CPU-bound traversal) are correctly separated in `api/app/api/graph.py`: the DB query runs on the async event loop directly (`await GraphBuilderService.build_graph(db, ...)`), while the CPU-bound NetworkX cycle search is explicitly offloaded with `await asyncio.to_thread(RingDetectorService.analyze_graph, ...)`. This distinction — I/O-bound work stays on the event loop, CPU-bound work goes to a thread pool — is the correct pattern for async Python and is applied consistently here.

## 3.4 Runtime Behavior

While the system is running, the API experiences three distinct traffic patterns simultaneously:

- **Baseline "legitimate" traffic** from `AsyncCustomerActor` and `AsyncEmployeeActor` instances in the simulator (Chapter 10), hitting `/api/login`, `/api/accounts/{id}/balance`, `/api/transfers`, `/api/beneficiaries` on lifecycle loops with randomized sleep intervals.
- **Attack-scenario traffic**, fired on a fixed tick schedule from `ScenarioManager` (Chapter 10), which deliberately produces failed logins, rapid sensitive-endpoint hits, high-value transfers to newly-created "mule" accounts, and other adversarial patterns.
- **Investigative traffic** from whatever dashboard or analyst tool is calling `/incidents`, `/analytics/metrics`, and `/api/graph/*` — none of which is simulated in this repository; these endpoints exist to be called by a human or a UI layer that isn't part of this codebase.

Every one of these requests, regardless of category, passes through the same middleware stack and contributes to the same `api_requests` topic — the API itself has no concept of "this looks suspicious"; that judgment happens entirely downstream, in Flink (Chapter 8). This separation of concerns — the API's job is to record what happened accurately and quickly, not to judge it — is itself a meaningful architectural choice worth internalizing.

## 3.5 Design Patterns Present

- **Dependency Injection**, via FastAPI's `Depends(get_db)` — every route declares its dependency on a database session rather than constructing one itself, which is what makes the sync/async split (§3.3.3) mechanically possible to route correctly per-endpoint, and makes routes independently testable.
- **Middleware / Chain of Responsibility**, via `BaseHTTPMiddleware` — `KafkaRequestLoggerMiddleware` and the Prometheus metrics middleware both wrap every request without any individual route handler needing to know they exist.
- **Repository-ish data access**, though not formally: services like `AnalyticsService` and `GraphBuilderService` are static-method classes that encapsulate query logic away from route handlers, which is the spirit of the Repository pattern (isolating persistence logic behind a service boundary) even though it isn't implemented as a strict interface/implementation pair.
- **Fail-open / graceful degradation**, appearing repeatedly: the DLQ fallback in `kafka_producer.py`, the JIT account creation in `transfers.py`, the mock-response fallbacks in `accounts.py` and `cards.py`. The consistent philosophy is: *never let a downstream or data problem produce a hard failure the simulator has to handle* — this is appropriate for a synthetic-traffic-generation platform, where the priority is generating rich fraud signal for the pipeline downstream, not strict data integrity.

## 3.6 Learning Concepts

- **ASGI and the async event loop.** FastAPI runs on Starlette/ASGI, meaning a single-threaded event loop can hold many in-flight requests concurrently by switching between them at `await` points (a DB query, an HTTP call), rather than needing one OS thread per connection.
- **Weak references and garbage collection in async code.** The `_background_tasks` fix (§3.3.6) is a specific instance of a general Python pitfall: fire-and-forget tasks need an explicit strong reference somewhere, or they can vanish before running.
- **Middleware / interceptor pattern.** Cross-cutting concerns (logging, metrics, auth) implemented once and applied to every request, rather than duplicated in every handler.
- **Schema-on-write validation with graceful fallback (dead-letter queue).** Rather than crashing or silently dropping malformed data, invalid events are routed to a separate topic for later inspection — a standard pattern in any pipeline that enforces a strict schema contract.
- **Offloading CPU-bound work from an async event loop.** `asyncio.to_thread()` for NetworkX graph algorithms in `graph.py` is the correct technique to avoid blocking the event loop with synchronous, CPU-heavy code.

## 3.7 Production Perspective

This API is good at what it needs to do for this project: generate rich, realistic banking activity and telemetry for a downstream fraud pipeline to consume. As a *banking* API specifically (rather than a telemetry generator), a production system serving real money would differ in several concrete ways:

- **Authentication.** There is no real authentication here — `X-User-Id` and `X-Employee-Id` headers are trusted at face value, and the simulator sets them itself. A production system needs real session/token verification (OAuth2/OIDC, mTLS between services, or at minimum signed, server-issued session tokens) rather than a client-supplied identity header.
- **Input validation vs. JIT provisioning.** The JIT-account-creation pattern (§3.3.4) is precisely backwards for a real bank: a transfer referencing a nonexistent account should be rejected, not silently create one with a $50,000 balance. This is entirely appropriate here because the goal is generating fraud *signal*, not protecting real funds — but it's the single most important thing to change if any part of this codebase were ever repurposed toward real transactions.
- **Sync/async unification.** Production teams would typically pick one data-access pattern (most likely fully async, given the I/O-bound nature of the workload) rather than maintaining two `Base` registries and two engine configurations, to reduce the cognitive overhead of "which session type does this router expect."
- **Idempotency.** `/api/transfers` has no idempotency key; a retried request (from a flaky client, a load balancer retry, etc.) would double-execute the transfer. Production payment APIs virtually always require a client-supplied idempotency key that the server deduplicates against.
- **Rate limiting and input size limits.** None are present here; a production gateway would sit behind rate limiting (often at an API gateway or service mesh layer, not hand-rolled in the application) to prevent exactly the kind of high-velocity abuse this platform's simulator deliberately generates for the *fraud engine* to detect, but which a real gateway should also reject at the edge.

---

*Next: [Chapter 4 — PostgreSQL & the Data Model](04-postgresql-and-data-model.md)*

# Chapter 4 — PostgreSQL & the Data Model

*Previous: [Chapter 3 — The Core Banking API](03-core-banking-api.md) · Next: [Chapter 5 — Change Data Capture: Debezium & Kafka Connect](05-cdc-debezium-kafka-connect.md)*

---

### Overview

PostgreSQL is the system of record for this platform — the one place where "what actually happened" is durably, transactionally true. Every other component in this handbook is, in one way or another, downstream of it: Kafka topics are derived from it (via CDC), OpenSearch indices are derived from those topics, and Flink's fraud scores are computed from events that ultimately originated as rows written here. If PostgreSQL's data is wrong, everything downstream is wrong in exactly the same way, propagated automatically. If PostgreSQL is unavailable, the API can't write transactions, and the CDC pipeline has nothing new to capture — but critically, as you'll see in this chapter, it does **not** mean the rest of the pipeline stops; Kafka keeps whatever was already captured, and downstream consumers keep working through their backlog.

**Repository fact.** The database runs as `postgres:15-alpine` (`docker-compose.yml`), a single container named `bank_postgres`, with no replica, no connection pooler in front of it, and a bind-mounted data volume for persistence across restarts.

### Why it Exists

It would be entirely possible to build a fraud-detection demo without a "real" relational database at all — you could generate synthetic events directly into Kafka and skip persistence altogether. This project deliberately doesn't do that, and the reason is central to what makes the whole system realistic rather than a toy: **real banking systems are relational-database-first**, and any fraud platform that wants to demonstrate CDC-based architecture needs a genuine source-of-truth database whose internal write-ahead log can be tailed. PostgreSQL specifically (rather than MySQL, or a NoSQL store) is the right choice here because its logical replication feature — covered in depth in the next chapter — is mature, well-documented, and is exactly what Debezium's PostgreSQL connector is built against.

There's a second, more subtle reason PostgreSQL's role matters: it's what makes the "two competing SQLAlchemy `Base` registries" situation from Chapter 3 possible without breaking anything. Because PostgreSQL enforces its schema at the database level — foreign keys, `NOT NULL` constraints, primary keys — both halves of the application code can write to the same tables through different SQLAlchemy engines (sync and async) without corrupting data, *as long as* both halves agree on what the schema actually looks like. That agreement is fragile (Chapter 3 already flagged the two independent schema-creation paths), and this chapter is where you see exactly what schema they're agreeing on.

### Implementation in This Repository

#### 4.1 The two sources of schema truth

**Repository fact.** There are, in this repository, three different places that describe the PostgreSQL schema, and they are not automatically kept in sync with one another:

1. **`api/app/models/domain.py`** — the SQLAlchemy ORM model classes (`Customer`, `Account`, `Transaction`, `LoginEvent`, `Device`, `Session`, `Employee`, `EmployeeAction`, `Alert`, `Case`, `AuditEvent`, `Card`), which is what `Base.metadata.create_all()` actually executes against the database at API startup (Chapter 3, §3.3.2).
2. **`api/alembic/versions/002_create_incident_management_tables.py`** — a hand-authored, versioned migration for the `incidents` family of tables, applied separately via `alembic upgrade head`, not by the API's own startup code.
3. **`schema.sql`** — a `pg_dump` snapshot of a real, running instance of the database, checked into the repository as a point-in-time reference (its header confirms `Dumped from database version 15.18`, i.e. this is a genuine dump, not a hand-written approximation).

Cross-referencing `schema.sql` against `domain.py` is a useful exercise, because it's exactly the discipline this handbook's earlier chapters describe being applied during the platform's own development — several comments throughout the Java Flink engine and the OpenSearch sink explicitly reference having done this same cross-check (Chapters 8 and 9 return to this repeatedly). Doing it here for `login_events` illustrates why it matters:

```text
schema.sql (a pg_dump snapshot, taken at some point in time):
  login_events: event_id, customer_id, device_id, country,
                 ip_address, success, timestamp
                              (NO latitude/longitude columns)

domain.py (the live ORM model, authoritative for what create_all() does today):
  class LoginEvent(Base):
      ...
      latitude = Column(Float, nullable=True)   # NEW
      longitude = Column(Float, nullable=True)  # NEW
```

**Architectural inference.** The dump doesn't have the geo columns; the live model does. This isn't a contradiction — it's a timestamped snapshot predating a later schema change, and it's exactly why `main.py`'s startup hook (Chapter 3, §3.3.2) runs an explicit `ALTER TABLE ... ADD COLUMN IF NOT EXISTS latitude, longitude` on every boot: it's a targeted, idempotent patch bridging exactly this kind of drift for any database that was originally provisioned before the geo columns existed, without requiring a full migration replay. It's a reasonable tactical fix for a single-column addition in a fast-moving project; Chapter 13 discusses why it doesn't scale as a general schema-evolution strategy.

#### 4.2 The full table catalogue

The following table consolidates what `domain.py`, the Alembic migration, and `schema.sql` collectively define. Columns are as declared in the live ORM models (`domain.py` / `incidents.py`), which is the schema every other chapter in this handbook treats as ground truth.

| Table | Key columns | Purpose | CDC-captured? |
|---|---|---|---|
| `customers` | `customer_id` (PK), `first_name`, `last_name`, `email`, `country`, `risk_level` | Identity record for every account holder, including system fallback identities | No |
| `accounts` | `account_id` (PK), `customer_id` (FK), `balance`, `currency`, `status` | The ledger — balances live here | **Yes** |
| `transactions` | `transaction_id` (PK), `from_account`, `to_account`, `amount`, `currency`, `transaction_type` | Money movement between accounts. No `customer_id` column at all — a fact that drives real design decisions in Chapter 8 | **Yes** |
| `login_events` | `event_id` (PK), `customer_id` (FK), `device_id`, `country`, `ip_address`, `success`, `latitude`, `longitude` | Authentication attempts, success or failure | **Yes** |
| `sessions` | `session_id` (PK), `customer_id` (FK), `device_id`, `ip_address`, `expires_at` | Active login sessions. No country/geo column — also drives a Flink design decision in Chapter 8 | **Yes** |
| `devices` | `device_id` (PK), `customer_id` (FK), `device_type`, `first_seen`, `risk_score` | Known device fingerprints per customer | No |
| `beneficiaries` | `beneficiary_id` (PK), `customer_id` (FK), `account_number`, `bank_name` | Saved transfer recipients | **Yes** |
| `employees` | `employee_id` (PK), `department`, `role` | Internal bank staff | No |
| `employee_actions` | `action_id` (PK), `employee_id` (FK), `customer_id` (FK, nullable), `action_type` | Staff audit trail — insider-threat detection surface | **Yes** |
| `alerts`, `cases`, `audit_events` | — | Legacy/simple alerting tables predating the `incidents` model | No |
| `cards` | `card_id` (PK), `customer_id` (FK), `account_id` (FK), `card_number`, `status` | Debit/credit card records | No |
| `incidents` | `id` (UUID PK), `identity_key`, `status`, `severity`, `alert_count`, `max_risk_score` | Deduplicated fraud cases (Chapter 2, §2.5) | No |
| `incident_alerts` | `id` (UUID PK), `incident_id` (FK), `alert_id`, `indicator`, `risk_score`, `raw_payload` (JSONB) | Individual alerts folded into an incident | No |
| `incident_audit_logs` | `id` (UUID PK), `incident_id` (FK), `previous_status`, `new_status`, `changed_by` | The incident state-machine's audit trail | No |

The "CDC-captured?" column is not incidental — it's copied directly from `table.include.list` in `debezium-postgres-connector.json` (Chapter 5), and it's the single most important lens for reading this table: **six tables are part of the streaming pipeline; the rest exist only for the API's own synchronous request/response needs** (case management, card display, legacy alerting) and are invisible to Flink, the OpenSearch sink, and every other stream consumer in this handbook. If you're extending this platform and want a new signal available to the fraud engine, the table it lives in needs to be added to that include-list — nothing here happens automatically just because a table exists in PostgreSQL.

#### 4.3 The one line of configuration everything depends on

**Repository fact.** `docker-compose.yml`'s `postgres` service passes three flags on the command line:

```yaml
command:
  - postgres
  - "-c"
  - "wal_level=logical"
  - "-c"
  - "max_replication_slots=4"
  - "-c"
  - "max_wal_senders=4"
```

`wal_level=logical` is not PostgreSQL's default (`replica`), and it is the single configuration change that makes everything in Chapter 5 possible. The next chapter explains exactly what the Write-Ahead Log is and why raising this setting is what allows Debezium to read structured, row-level change events out of it rather than just the physical bytes needed for streaming replication. `max_replication_slots` and `max_wal_senders` cap how many independent logical-replication consumers (like Debezium) and physical replicas can attach at once — set to 4 here, comfortably above the single slot this system actually uses, leaving headroom for a second connector or a future read replica without a configuration change.

#### 4.4 Foreign keys as a JIT-provisioning pressure valve

**Repository fact.** Nearly every table in `domain.py` has a `ForeignKey` constraint back to `customers` or `accounts` (e.g., `Account.customer_id`, `Transaction.from_account`, `LoginEvent.customer_id`). PostgreSQL enforces these at the database level — an `INSERT` referencing a nonexistent `customer_id` is rejected with a foreign-key-violation error, full stop, regardless of what the application layer intended.

**Architectural inference.** This is *why* the JIT-provisioning pattern from Chapter 3 (§3.3.4) has to cascade the way it does — `_ensure_account_exists()` can't just create an `Account` row referencing a nonexistent `Customer`; it has to create the `Customer` first, `flush()` it so PostgreSQL assigns it durably within the transaction, and only then create the `Account`. This is a direct, visible consequence of relational integrity constraints shaping application code: the database's foreign keys are not just a data-quality nicety here, they are an active constraint the API's write-path logic has to explicitly satisfy in the correct order every time.

#### 4.5 Numeric precision for money

**Repository fact.** `Account.balance` and `Transaction.amount` are both declared as `Numeric(15, 2)` in `domain.py` (confirmed identically in `schema.sql`: `numeric(15,2)`) — a fixed-point decimal type with up to 15 total digits and exactly 2 after the decimal point, not a floating-point `Float` or `Double`.

This is a deliberate and correct choice worth understanding even outside this specific repository: binary floating-point types (IEEE 754 `float`/`double`) cannot represent most decimal fractions exactly — `0.1 + 0.2` famously does not equal `0.3` in floating-point arithmetic — which is unacceptable for any system that has to sum, subtract, and reconcile money down to the cent. `Numeric` (PostgreSQL) / `Decimal` (Python's `decimal` module, used in `transfers.py`'s `Decimal("50000.00")` JIT balances) represent decimal values exactly, at the cost of somewhat slower arithmetic than native floats — a cost every serious financial system accepts as the obviously correct tradeoff. One place worth flagging: the Debezium connector configuration sets `"decimal.handling.mode": "double"` (`debezium-postgres-connector.json`), meaning once a `Numeric` balance crosses into the CDC pipeline as a JSON event, it is represented as a floating-point number in the Kafka message — a precision downgrade that's acceptable for fraud-scoring thresholds (Chapter 8 only ever compares amounts against a large round-number threshold) but would be a real problem if any downstream consumer needed to reconcile balances to the cent from CDC events rather than from PostgreSQL directly.

### Runtime Behaviour

At any given moment while the platform is running, PostgreSQL is doing three things concurrently, each on its own concern:

1. **Serving transactional reads and writes from the API** — the synchronous and asynchronous SQLAlchemy engines from Chapter 3, both connecting to the same database, the same tables, through separate connection pools.
2. **Streaming its Write-Ahead Log to a replication slot** that Debezium holds open continuously — this happens at the PostgreSQL server level, independent of and invisible to the API entirely; PostgreSQL doesn't distinguish "a write happened because the API committed a transaction" from any other kind of write when deciding what to put in the WAL.
3. **Accepting direct, out-of-band mutations** — the CDC operations walkthrough in Chapter 5 uses exactly this capability, running a raw `UPDATE accounts SET balance = ...` via `psql` from outside the API entirely, specifically to demonstrate that the CDC pipeline captures changes regardless of what wrote them.

This third point is worth dwelling on, because it's the clearest possible demonstration of *why* CDC-based architecture is more robust than application-level event logging (first raised in Chapter 1, §1.3): if fraud detection here worked by having the API publish an event every time it processed a transfer, that out-of-band `psql UPDATE` would be completely invisible to the fraud engine. Because detection instead watches the database's own WAL, it doesn't matter how a row changed — only that it did.

### Learning Concepts

- **System of record.** The one authoritative source of truth for a given piece of data, from which all derived copies (caches, search indices, materialized views, and — in this system's case — Kafka topics) are ultimately computed. Derived copies can lag behind the system of record, or be rebuilt from it, but they should never be treated as more authoritative than it.
- **Write-Ahead Logging (WAL).** A durability mechanism where every change is first appended to a sequential, append-only log before being applied to the actual data files — PostgreSQL uses this for crash recovery (replaying the WAL after an unclean shutdown reconstructs any change that was logged but not yet flushed to disk) and, when `wal_level=logical` is set, exposes this same log in a form external tools can decode into row-level insert/update/delete events. Chapter 5 goes considerably deeper into this.
- **Fixed-point decimal arithmetic vs. floating point.** Any system handling money needs exact decimal representation; `Numeric`/`Decimal` types exist specifically to avoid the rounding errors inherent to binary floating-point representations of decimal fractions.
- **Referential integrity as an architectural constraint, not just a data-quality feature.** Foreign keys don't just prevent bad data — as shown in §4.4, they actively shape the order of operations application code has to follow.
- **Schema drift.** The gap between what a schema *was* (a `pg_dump` snapshot) and what it *is now* (the live ORM model) is a universal problem in any system that evolves over time; §4.1 shows both a real instance of it in this repository and the ad-hoc patch used to bridge it.

### Production Perspective

Running PostgreSQL as this repository does — a single container, no replica, a host bind-mount for persistence, and both the transactional API workload and Debezium's logical-replication read competing for the same instance's resources — is entirely appropriate for a homelab or demo deployment and would need real attention before handling production traffic:

- **A dedicated logical-replication path.** Production Debezium deployments commonly read from a **read replica** with its own replication slot, or at minimum are given a dedicated PostgreSQL role with carefully scoped permissions (`REPLICATION` privilege plus `SELECT` on the included tables, nothing more) — separating the CDC connector's resource consumption from the primary's transactional throughput, and limiting the blast radius if the connector's credentials were ever compromised.
- **High availability.** A single-instance PostgreSQL container is a single point of failure for the entire platform — if it goes down, the API can't write, and (once the existing replication slot's WAL retention is exhausted) Debezium can permanently lose its place in the log. Production systems run PostgreSQL with streaming replication to standby replicas and automated failover (via tools like Patroni, or a managed service like AWS RDS/Aurora, Google Cloud SQL, or Azure Database for PostgreSQL that handles this natively).
- **Replication slot monitoring.** An unmonitored, disconnected replication slot is a classic PostgreSQL operational hazard: PostgreSQL retains WAL segments for as long as a replication slot exists and hasn't consumed them, specifically so a disconnected consumer (like a temporarily-down Debezium connector) can catch up later — but if that consumer never reconnects, WAL accumulates without bound and can eventually fill the disk, taking the primary database down with it. Production deployments alert on replication slot lag explicitly for this reason.
- **Schema management.** As flagged in §4.1, unifying schema evolution behind a single migration tool (Alembic, in this codebase's case, already used for the `incidents` tables) rather than a mix of `create_all()` and ad-hoc `ALTER TABLE IF NOT EXISTS` startup patches would give this project a single, auditable, rollback-capable history of every schema change — valuable even for a demo project as it grows, and non-negotiable for anything handling real financial data subject to audit requirements.
- **Connection pooling.** At real scale, a connection pooler (PgBouncer, or a managed equivalent) sitting between application services and PostgreSQL is standard, since PostgreSQL's per-connection process model makes a very large number of direct application connections expensive; this repository's single-instance API doesn't yet operate at a scale where this matters, but it's one of the first things added as traffic grows.

### Common Pitfalls

- **Treating a `pg_dump` snapshot as living documentation.** As §4.1 demonstrates concretely, a schema dump is a photograph, not a video feed — trusting it over the live ORM models (or, better, the running database itself) is exactly the mistake that several comments throughout this repository's Flink and OpenSearch code explicitly call out having made and corrected.
- **Forgetting that `wal_level=logical` is not the default.** A developer standing up a fresh PostgreSQL instance for a similar project, without copying this exact `command:` block, will find Debezium's connector registration fails outright — this single setting is easy to overlook because everything else about the database looks and behaves completely normally until a CDC connector tries to attach.
- **Using floating-point types for currency.** This repository gets it right (`Numeric(15,2)`), but it's one of the most common real-world mistakes in systems built quickly — a `Float`/`Double` balance column looks fine in testing and silently accumulates rounding drift under real volume.
- **Assuming foreign keys will "just work" with synthetic or JIT-generated data.** As shown in §4.4, any write path that generates its own IDs (a simulator, a data-seeding script, a JIT-provisioning helper) has to satisfy foreign-key ordering explicitly — parent row first, `flush()` or `commit()`, then the child row — or the write fails outright, not silently.
- **Assuming CDC will magically pick up a new table.** Adding a table to `domain.py` does not add it to the CDC pipeline; that requires an explicit change to `table.include.list` in the Debezium connector configuration, covered next in Chapter 5. It's an easy thing to forget when extending this system, because nothing errors — the new table simply never appears in Kafka.

### Summary

PostgreSQL is this platform's single system of record, and understanding its schema — which tables exist, which are wired into the CDC pipeline and which aren't, and the `wal_level=logical` setting that makes CDC possible at all — is a prerequisite for understanding everything that follows. This chapter also surfaced a real, useful lesson in schema management: this repository maintains its schema truth in three different places (a startup-time `create_all()`, a versioned Alembic migration, and a point-in-time `pg_dump` snapshot) that can and do drift apart, and the codebase's own ad-hoc `ALTER TABLE IF NOT EXISTS` patch for the geo-point columns is a real, live example of that drift being bridged. With the data layer now fully understood, the next chapter picks up exactly where `wal_level=logical` left off: how Debezium turns PostgreSQL's Write-Ahead Log into a stream of structured Kafka events, without a single line of application code being aware it's happening.

---

*Next: [Chapter 5 — Change Data Capture: Debezium & Kafka Connect](05-cdc-debezium-kafka-connect.md)*

# Chapter 5 — Change Data Capture: Debezium & Kafka Connect

*Previous: [Chapter 4 — PostgreSQL & the Data Model](04-postgresql-and-data-model.md) · Next: [Chapter 6 — Apache Kafka & Schema Registry](06-kafka-and-schema-registry.md)*

---

### Overview

Change Data Capture, or CDC, is the technique of turning a database's internal record of "what changed" into a stream of events that other systems can consume — without the database's own application ever having to publish those events itself. In this platform, CDC is implemented by **Debezium**, running as a plugin inside **Kafka Connect**, reading directly from PostgreSQL's Write-Ahead Log via **logical replication**. The result is that every `INSERT`, `UPDATE`, and `DELETE` on six specific tables (§4.2) becomes a structured JSON message on a Kafka topic, in real time, automatically.

This is the chapter where the "CDC Path" introduced in Chapter 1 stops being a diagram label and becomes a concrete, traceable mechanism, and it's worth reading closely: CDC is the architectural decision that most distinguishes this platform from a simpler "the API also happens to publish some Kafka events" design, and it's the reason the fraud engine in Chapter 8 can see database mutations it has no other way of knowing about.

### Why it Exists

Chapter 1 already made the high-level argument for CDC over application-level event publishing: an out-of-band database mutation (a direct SQL `UPDATE`, a future admin tool, a data-migration script) is invisible to any logging that only fires inside application code, but is *always* visible to CDC, because CDC doesn't watch the application — it watches the database itself. This chapter adds the second half of that argument, which only becomes clear once you understand *how* CDC actually works: it achieves this without adding any write-path latency or failure risk to the application at all.

Contrast this with the two most common alternatives to CDC for "publish an event whenever data changes":

- **Dual writes** — the application writes to the database *and* separately publishes to Kafka, in the same request. This is what `KafkaRequestLoggerMiddleware` effectively does for `api_requests` telemetry (Chapter 3). The problem: these are two independent operations against two independent systems, with no shared transaction. If the database commit succeeds but the Kafka publish fails (network blip, broker unavailable), the two systems permanently disagree about what happened, and nothing detects or corrects it. This exact risk is why `send_telemetry_event()` fails open to a dead-letter queue rather than silently swallowing the error — but even that only makes the *failure* visible, it doesn't make the write atomic.
- **The Outbox Pattern** — the application writes both the actual data change and an "event to publish" row into the *same* database transaction (an `outbox` table), and a separate poller or CDC process reads the outbox table and publishes from there. This solves the dual-write atomicity problem, but requires deliberate application-level plumbing: every write path has to remember to also write an outbox row.

**Repository fact.** This platform uses neither of those — it captures changes directly from the tables that already exist (`accounts`, `transactions`, `login_events`, etc.), with **zero application code changes required** to add CDC capture to a new field or a new write path on an already-included table. If `execute_transfer()` in Chapter 3 changes tomorrow to also update a `last_transfer_at` column on `Account`, that change appears in the CDC stream automatically, the very next time Debezium polls the WAL — no middleware to update, no outbox row to remember to write. This is CDC's central advantage: **the source of truth and the event stream are guaranteed to agree, because the event stream is derived directly from the source of truth's own internal log, not from a parallel path that could diverge from it.**

### Implementation in This Repository

#### 5.1 What "logical replication" actually captures

PostgreSQL's Write-Ahead Log, at the physical level, records changes as raw page-level byte deltas — enough information for PostgreSQL itself to reconstruct exact disk state after a crash, but not something a general-purpose consumer could meaningfully decode into "row X in table Y changed from this value to that value." **Logical replication** is a PostgreSQL feature (enabled by `wal_level=logical`, set in Chapter 4, §4.3) that adds a decoding layer on top of the physical WAL, translating those byte-level changes into a **logical** stream of row-level events: "table `accounts`, row with `account_id='ACC-GEN00003'`, column `balance` changed from 5000.00 to 4750.00."

**Repository fact.** Debezium's PostgreSQL connector attaches to this logical stream through a **replication slot** and a configured **output plugin** — `debezium-postgres-connector.json` specifies `"plugin.name": "pgoutput"`, which is PostgreSQL's own built-in logical decoding plugin (as opposed to older third-party plugins like `wal2json` or `decoderbufs`, both of which predate `pgoutput` being bundled with PostgreSQL itself and are now largely legacy choices). A replication slot is a durable, named PostgreSQL server-side object that tracks exactly how far a given consumer has read the logical stream — this is what makes the "PostgreSQL retains WAL until the slot has consumed it" behavior discussed in Chapter 4's Production Perspective possible: the slot is PostgreSQL's own bookkeeping of Debezium's read position, and PostgreSQL will not discard WAL segments the slot hasn't yet acknowledged, precisely so a temporarily-disconnected Debezium can resume from exactly where it left off without missing anything.

#### 5.2 The connector configuration, field by field

**Repository fact.** `debezium-postgres-connector.json` is the connector definition actually registered against this project's single-host Kafka Connect instance (a near-identical copy, `config/kafka-connect/postgres-source.json`, is registered by the CI/CD pipeline against the Swarm deployment — Chapter 12):

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

Reading this configuration field by field is the fastest way to actually understand what Debezium is doing, so it's worth walking through deliberately:

- **`table.include.list`** is the allowlist referenced repeatedly throughout this handbook — it is the *only* place in the entire repository that determines which of PostgreSQL's tables produce CDC events at all. Six tables are listed; every other table in `schema.sql` (`customers`, `devices`, `employees`, `cards`, `alerts`, `cases`, `audit_events`, and the entire `incidents` family) is completely invisible to Kafka, no matter what changes inside them.
- **`topic.prefix: "banking"`** combined with Debezium's own default topic-naming convention (`<topic.prefix>.<schema>.<table>`) means Debezium's *natural* output topic name for the `login_events` table is `banking.public.login_events` — note the `public` schema name embedded in the middle.
- **The `RouteTable` transform is the fix for exactly that.** A **Single Message Transformation (SMT)** is Kafka Connect's mechanism for applying a lightweight, stateless transformation to each message as it passes through the connector, before it's published. Here, a `RegexRouter` SMT rewrites the topic name using the regex `banking\.public\.(.*)` → `banking.$1`, stripping the `public.` segment so the actual published topic is the cleaner `banking.login_events` rather than `banking.public.login_events`. This is purely cosmetic (it doesn't touch the message payload at all) but it's the reason every other chapter in this handbook can refer to `banking.transactions`, `banking.accounts`, etc. as clean, predictable topic names.
- **`decimal.handling.mode: "double"`** was already flagged in Chapter 4, §4.5 as a deliberate precision tradeoff — PostgreSQL's exact `Numeric(15,2)` balances become IEEE 754 floating-point doubles the moment they cross into a CDC event.
- **`key.converter.schemas.enable` / `value.converter.schemas.enable`: `"false"`**, combined with `JsonConverter`, means Debezium emits plain JSON payloads on these topics — *not* Avro, and *not* registered against the Schema Registry. This is a real, meaningful distinction from the `api_requests` topic (Avro-encoded, Chapter 3) and the `fraud_alerts_v2`/`critical_alerts_v2` topics (also Avro, Chapter 8) — every consumer of a `banking.*` CDC topic has to parse plain JSON, while consumers of those other topics have to go through Avro deserialization against the Schema Registry. You'll see this distinction handled explicitly in both `RiskEventNormalizer.java` (Chapter 8) and `opensearch_sink.py`'s `AVRO_TOPICS` set (Chapter 9) — each maintains its own knowledge of which topics need which deserialization path, because Debezium's connector configuration is the only place that decision is actually made.
- **`schema.history.internal.kafka.bootstrap.servers`** points Debezium at a separate internal Kafka topic it uses to durably record the table *schema* history it has observed over time (column additions, type changes) — distinct from the data-change topics themselves, and necessary because Debezium needs to know a table's schema at the time of each historical WAL entry to decode it correctly, even if the live table schema has since evolved.

#### 5.3 The CDC envelope, and why every consumer has to unwrap it

**Repository fact.** Every message Debezium publishes follows the same envelope structure, regardless of source table:

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

| Field | Meaning |
|---|---|
| `before` | The row's prior state. `null` for inserts and for the initial snapshot. Populated for updates and deletes. |
| `after` | The row's new state. `null` for deletes (the row no longer exists). |
| `source` | Metadata about *where this event came from* — which table, which connector, the source database's own timestamp (`ts_ms` here is inside `source`, distinct from the envelope's top-level `ts_ms`). |
| `op` | The single-character operation code — see the table below. |
| `ts_ms` | The envelope-level timestamp, in true milliseconds, regardless of the source column's own precision. |

| `op` value | Meaning |
|---|---|
| `r` | Initial snapshot read — when a connector first starts, Debezium takes a consistent snapshot of every row already in an included table, and emits each one with `op: "r"` before switching over to live WAL streaming. |
| `c` | Create (`INSERT`) |
| `u` | Update (`UPDATE`) |
| `d` | Delete (`DELETE`) — note `after` is `null` here; the deleted row's last known state is in `before` instead. |

**Architectural inference.** Notice the `ts_ms` distinction called out in the table above: the envelope's top-level `ts_ms` (in this example, `1781920055239`, thirteen digits — genuine Unix epoch milliseconds) is different from `after.created_at` (`1781913231931852`, sixteen digits). This is not a formatting inconsistency to be fixed — it's PostgreSQL's own column value, at whatever time precision that column happens to store (`time.precision.mode` defaults to "adaptive," meaning a plain `TIMESTAMP` column round-trips as microseconds, not milliseconds), passed through completely unmodified inside `after`, versus Debezium's own envelope-level `ts_ms`, which is *documented* by Debezium to always be real milliseconds regardless of the source column's precision. Two independent parts of this codebase — `RiskEventNormalizer.java` in the Flink engine (Chapter 8) and `unwrap_cdc_envelope()` in the OpenSearch sink (Chapter 9) — both explicitly rely on the envelope's `ts_ms` rather than any row-level timestamp column for exactly this reason, and both carry code comments documenting having discovered and corrected this the hard way. This is a genuinely instructive real-world lesson: a schema that looks self-evidently "just a timestamp" can carry a subtle precision trap that only becomes visible once two independent consumers of the same data disagree about what a raw value means.

**Every consumer of a `banking.*` topic has to make the same set of decisions about this envelope**: skip or specially-handle delete events (since `after` is `null`), extract fields from inside `after` rather than the envelope root, and choose which timestamp to trust. Chapters 8 and 9 each implement this unwrapping independently, in two different languages, and it's worth watching for how closely their logic mirrors each other despite having no shared code — a natural consequence of both being correct solutions to the exact same problem.

#### 5.4 Connector registration and Kafka Connect's REST API

**Repository fact.** Debezium doesn't run as its own standalone process — it runs as a plugin *inside* Kafka Connect (the `debezium/connect:2.4` image, which bundles the Kafka Connect distributed worker plus the Debezium PostgreSQL connector plugin already installed). Kafka Connect exposes a REST API (port `8083`) for managing connectors as JSON configuration documents, entirely separately from starting or stopping the worker process itself — you register, update, pause, resume, and delete connectors by `POST`/`PUT`/`DELETE`-ing against this API, not by editing files on disk and restarting a process.

**Repository fact.** The `connect-init` service in `docker-compose.yml` is a small, purpose-built bootstrapper: it polls `http://connect:8083/connectors` until Kafka Connect responds, checks whether `banking-cdc-connector` is already registered (`GET /connectors/banking-cdc-connector`, looking for a `200`), and if not, `POST`s `debezium-postgres-connector.json` to register it — then exits. This is the same "one-shot init container" pattern used by `kafka-init` (Chapter 6) and `fraud-engine`'s job-submission step (Chapter 8): a container whose entire job is to run once, perform an idempotent setup action, and terminate, rather than running as a long-lived service. `docker compose ps` correctly shows this container as `Exited (0)` shortly after startup — that's success, not a crash, exactly as the comment in `submit-job.sh` (Chapter 8) makes explicit for its own analogous case.

### Runtime Behaviour

Putting the entire chapter into motion: when this platform starts from an empty PostgreSQL database, Debezium's very first action (after `connect-init` successfully registers it) is an **initial snapshot** — it takes a consistent read of every row currently in the six included tables and emits each one as an `op: "r"` event, exactly as if every row were being freshly inserted at that instant. Only once the snapshot completes does the connector transition to genuinely live logical-replication streaming, picking up from the replication slot's current WAL position. Any new `INSERT`/`UPDATE`/`DELETE` on an included table after that point — whether from the API's ordinary write paths, the simulator's JIT-provisioned rows, or a direct `psql` command run by an operator — appears on the corresponding `banking.*` Kafka topic within the same order-of-milliseconds latency, continuously, for as long as the connector runs.

The following commands, drawn directly from this repository's own operational reference, let you observe this live:

```powershell
# Register the connector (idempotent — connect-init already does this automatically)
curl.exe -X POST -H "Content-Type: application/json" -d @debezium-postgres-connector.json http://localhost:8083/connectors

# Confirm the connector created its topics
docker exec -it bank_kafka kafka-topics --bootstrap-server localhost:29092 --list

# Tail live CDC events on a specific topic
docker exec -it bank_kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic banking.accounts --from-beginning

# Force a mutation completely outside the API, to prove CDC doesn't care how a row changed
docker exec -it bank_postgres psql -U postgres_admin -d banking_db \
  -c "UPDATE accounts SET balance = balance + 250.00 WHERE account_id = 'ACC-GEN00000';"
```

Running the last command while the console consumer from the previous command is still attached is the single clearest way to *see* Chapter 1's central architectural claim proven in real time: a raw SQL mutation, issued from a `psql` shell with no HTTP request, no FastAPI route handler, and no Kafka producer call anywhere in its path, appears on the `banking.accounts` topic within moments — because Debezium was never watching the API. It was always watching PostgreSQL itself.

### Learning Concepts

- **Change Data Capture (CDC).** The general technique of deriving an event stream from a database's own internal change log, rather than from application-level instrumentation — introduced conceptually in Chapter 1, now grounded in a concrete implementation.
- **Logical replication and replication slots.** PostgreSQL's mechanism for decoding physical WAL bytes into row-level logical events, and the durable server-side bookmark (the slot) that lets a consumer resume exactly where it left off after a disconnection.
- **The Outbox Pattern (as a contrast, not as what this system does).** Understanding why CDC is often preferred *over* the outbox pattern — no application code changes required per write path — is easier once you've seen a real outbox-pattern alternative (`KafkaRequestLoggerMiddleware`'s dual-write-with-DLQ-fallback design in Chapter 3) sitting right next to it in the same codebase, addressing a similar problem with weaker atomicity guarantees.
- **Single Message Transformations (SMTs).** Kafka Connect's mechanism for lightweight, per-message, stateless transformation (here, topic-name rewriting via `RegexRouter`) applied inline as part of the connector pipeline, without a separate downstream processing job.
- **Schema history vs. data history.** Debezium's need to separately track *how a table's schema itself has changed over time* (via the internal schema history topic), distinct from tracking the data changes within that schema — necessary because decoding an old WAL entry correctly requires knowing what the table looked like at the time that entry was written.
- **Envelope-level metadata vs. payload-level data.** The general pattern of separating "information about this event" (`source`, `op`, envelope `ts_ms`) from "the actual data" (`before`/`after`), and why trusting the envelope's own guarantees (like `ts_ms` always being milliseconds) over payload-embedded values whose meaning depends on upstream configuration is usually the safer default.

### Production Perspective

Debezium is a genuinely production-grade, widely deployed piece of open-source software — companies running CDC at real scale (including several major fintechs and banks) commonly run exactly this connector, or a close commercial equivalent, in production. Where a production deployment of *this specific configuration* would typically go further:

- **`tasks.max` and partitioning.** This connector runs with `"tasks.max": "1"` — a single task handling all six tables sequentially. PostgreSQL's logical replication is inherently single-threaded per replication slot (there's exactly one ordered WAL stream to read), so this isn't a bottleneck at this data volume, but production Debezium deployments watching many high-volume tables sometimes split them across multiple connectors (each with its own replication slot) to parallelize independently, at the cost of losing cross-table ordering guarantees between the split groups.
- **Credentials.** The database password is plaintext, inline, in a JSON file checked into version control (`debezium-postgres-connector.json`). Production Kafka Connect deployments use **Connect's externalized secrets support** (a `ConfigProvider`, commonly backed by HashiCorp Vault, AWS Secrets Manager, or Kubernetes secrets) so connector configuration documents reference a secret by name rather than embedding it — this is one of the most common and most consequential differences between a demo configuration and a production one for *any* credentialed system, not just this one.
- **Dead-lettering and error tolerance.** This connector has no `errors.tolerance` configuration, meaning a single malformed or unexpectedly-shaped row (for example, a `NUMERIC` value that overflows the target type during decimal conversion) would, by Kafka Connect's default behavior, halt the entire connector task. Production configurations commonly set `errors.tolerance: "all"` paired with a dead-letter-queue topic, so one bad record doesn't stop the whole CDC pipeline for every table the connector handles.
- **Monitoring replication lag specifically.** Beyond the general replication-slot-disk-exhaustion risk already flagged in Chapter 4, production Debezium deployments specifically monitor the *lag* between the current WAL position and the slot's confirmed-flushed position (exposed via Kafka Connect's own JMX metrics, or PostgreSQL's `pg_replication_slots` view) as a first-class operational signal — rising lag is often the earliest indicator that a downstream consumer or the connector itself is falling behind, well before anything else in the system shows symptoms.
- **High availability for Kafka Connect itself.** This project runs a single Kafka Connect worker. Kafka Connect supports a distributed mode with multiple workers automatically rebalancing connector tasks across them if one worker fails — production deployments run at least three workers for exactly this resilience.

### Common Pitfalls

- **Forgetting `wal_level=logical`.** As flagged in Chapter 4, this is the single most common blocker when standing up a project like this from scratch — the connector registration will fail with an explicit error referencing logical replication not being enabled, but only once you actually try to register it, which can be well after the database itself appears to be running fine.
- **Assuming a table is captured just because it's in the database.** As emphasized in §5.2, `table.include.list` is an explicit allowlist. Adding a new table to the application's ORM models does nothing to the CDC pipeline until this list is updated *and the connector is restarted or reconfigured* to pick up the change.
- **Mishandling delete events.** A consumer that doesn't check `op == "d"` before reading `after` will crash or silently produce garbage on every delete, since `after` is `null` in that case. Both `RiskEventNormalizer.java` and `opensearch_sink.py`'s `unwrap_cdc_envelope()` handle this explicitly (Chapters 8 and 9) — it's worth noticing this is a recurring, easy-to-miss detail rather than a one-off.
- **Trusting a row's own timestamp column over the envelope's `ts_ms`.** As detailed in §5.3, this is a genuine, documented bug this repository's own developers hit and fixed — a strong signal that it's a natural mistake to make, not a hypothetical one.
- **Confusing plain-JSON CDC topics with Avro-encoded topics.** Because this connector is configured with `schemas.enable: false` and `JsonConverter`, `banking.*` topics require different deserialization code than the Avro-encoded `api_requests`/`fraud_alerts_v2`/`critical_alerts_v2` topics elsewhere in the same Kafka cluster — a consumer that assumes uniform serialization across every topic in the cluster will fail on whichever category it didn't anticipate.

### Summary

Debezium, running inside Kafka Connect and reading PostgreSQL's logical replication stream, is what turns this platform's system of record into a live event stream without a single line of application code needing to know CDC exists. This chapter walked through the connector configuration that makes that possible — the `table.include.list` allowlist that determines exactly what's visible downstream, the `RegexRouter` transform that produces the clean `banking.*` topic names used throughout the rest of this handbook, and the CDC envelope structure (`before`/`after`/`source`/`op`/`ts_ms`) that every downstream consumer has to unwrap — plus a real, documented timestamp-precision trap that two independent consumers of this exact data both had to discover and correct for independently. With events now flowing into Kafka from both the CDC path and the application-telemetry path, the next chapter turns to Kafka itself: how topics, partitions, and consumer groups actually work, and how the Schema Registry governs the Avro-encoded topics this chapter deliberately set aside.

---

*Next: [Chapter 6 — Apache Kafka & Schema Registry](06-kafka-and-schema-registry.md)*

# Chapter 6 — Apache Kafka & Schema Registry

*Previous: [Chapter 5 — Change Data Capture](05-cdc-debezium-kafka-connect.md) · Next: [Chapter 7 — Python Stream Processors](07-stream-processing-python.md)*

---

### Overview

Every event this handbook has discussed so far — API telemetry, CDC changes, and (in later chapters) fraud alerts — passes through exactly one piece of shared infrastructure: a single Apache Kafka broker, running in **KRaft mode** (Kafka's own internal consensus protocol, replacing the historical dependency on Apache ZooKeeper). Alongside it runs a **Schema Registry**, which governs the structure of the Avro-encoded messages that flow through some — not all — of Kafka's topics. This chapter is about Kafka and the Registry as pieces of infrastructure in their own right: what a topic actually is, how consumer groups let multiple independent services read the same data without interfering with each other, and how schema evolution is managed so that producers and consumers built at different times, in different languages, can still agree on what a message means.

### Why it Exists

Every previous chapter has treated "publish to Kafka" and "consume from Kafka" as a given, because understanding *why* a message broker sits at the center of this architecture required first understanding what feeds into it (Chapters 3 and 5) and, later, what reads from it (Chapters 7 through 9). Now that both ends of that pipe are motivated, it's worth stating plainly what Kafka itself contributes that a simpler mechanism — direct HTTP calls between services, or a traditional task queue — would not:

- **Multiple independent consumers, without coordination.** Chapter 2's fan-out diagram showed the same `banking.login_events` topic being read independently by the Flink engine, the OpenSearch sink, and (historically) the Python stream scripts. A traditional message queue (like a classic AMQP/RabbitMQ work queue) typically delivers each message to *one* consumer and then discards it — perfect for distributing work across a pool of identical workers, wrong for "many different systems each need to see every event for their own purposes." Kafka's log-based model, where messages are retained and consumers each track their own independent read position, is what makes this platform's fan-out architecture possible without the Flink engine, the OpenSearch sink, and any future consumer needing to know about each other at all.
- **Durable retention and replay.** As introduced in Chapter 1, §1.4, Kafka retains events for a configured period (or indefinitely, with compaction) rather than deleting them the instant they're delivered — meaning a brand-new consumer, or an existing one recovering from a crash, can replay history from any earlier point rather than only ever seeing events from the moment it happened to connect.
- **A natural backpressure buffer.** If the Flink cluster is temporarily slow or down, events keep arriving in Kafka and simply queue up, bounded by the topic's retention configuration — the API's write path (Chapter 3) is never blocked by, or even aware of, how quickly downstream consumers are keeping up.

### Implementation in This Repository

#### 6.1 KRaft mode: Kafka without ZooKeeper

**Repository fact.** The `kafka` service in `docker-compose.yml` runs `confluentinc/cp-kafka:7.4.0` configured with `KAFKA_PROCESS_ROLES: broker,controller` — a single container acting as both the data-handling broker *and* the metadata/consensus controller, using Kafka's own **KRaft** (Kafka Raft) protocol rather than an external ZooKeeper ensemble:

```yaml
KAFKA_NODE_ID: 1
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:29092
CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
```

For readers unfamiliar with the history here: Kafka historically depended on Apache ZooKeeper for cluster metadata (which broker is the controller, topic/partition assignments, consumer group coordination) — a second distributed system a Kafka deployment had to run and operate alongside Kafka itself. KRaft mode, which became production-ready starting with Kafka 3.3 and is what this project's `cp-kafka:7.4.0` image implements, moves that same metadata management *into* Kafka itself, using a Raft-based consensus protocol among a set of "controller" nodes. Running `broker,controller` on the same single node (as this configuration does) is specifically a single-node/demo topology; a multi-node production KRaft cluster would typically run a small, odd-numbered set of dedicated controller nodes (3 or 5) separately from the broker nodes that serve client traffic, discussed further in the Production Perspective below.

**Repository fact.** Note the three distinct listeners configured: `PLAINTEXT` (port `29092`, for other containers on the Docker network — e.g., `kafka:29092`, the address every other service in this handbook's code uses via `KAFKA_BOOTSTRAP_SERVERS`), `CONTROLLER` (port `29093`, internal KRaft consensus traffic only), and `PLAINTEXT_HOST` (port `9092`, mapped to `localhost:29092` on the host — wait, more precisely: the container's internal `9092` is advertised to host clients as `localhost:29092`, matching the `29092:9092` port mapping in `docker-compose.yml`). This is a completely standard, necessary pattern for any container that needs to be reachable both from *inside* its own Docker network (by its service name) and from *outside* it (from the host machine) — a client connecting via the wrong advertised address is one of the single most common Kafka connectivity failures newcomers encounter, because Kafka's protocol requires clients to follow whatever address the broker *advertises* for further connections, not just the address they initially dialed.

#### 6.2 The topic catalogue

**Repository fact.** The `kafka-init` service is a one-shot bootstrapper (the same pattern as `connect-init` from Chapter 5) that waits for the broker to become reachable, then creates every topic this platform uses, each with 3 partitions and a replication factor of 1:

```text
api_requests              dead_letter_queue          fraud_alerts
critical_alerts           risk_scores                risk_scores_v2
fraud_alerts_v2           critical_alerts_v2         enriched_transactions
model_features             banking.login_events        banking.sessions
banking.accounts             banking.transactions         banking.beneficiaries
banking.employee_actions      banking.unified_timeline
```

**Architectural inference.** Cross-referencing this list against what's actually produced and consumed elsewhere in the repository reveals a meaningful pattern worth naming explicitly: several of these topics — `fraud_alerts` (singular, no `_v2`), `critical_alerts` (singular), `risk_scores`, `risk_scores_v2`, `enriched_transactions`, `model_features` — are **provisioned but never produced to or consumed from anywhere in the live pipeline**. They're artifacts of the platform's own iterative history: `fraud_alerts` (singular) is written to by the legacy `rules_engine.py` script (Chapter 7) and read by the equally-legacy `risk_scoring_engine.py`, neither of which is wired into `docker-compose.yml`; `risk_scores`/`risk_scores_v2`/`enriched_transactions`/`model_features` appear to be provisioned ahead of features that were planned but never built. This is worth knowing not because it's a problem — topic creation is cheap and harmless — but because it's a genuinely useful skill to practice: when exploring an unfamiliar event-driven system, **the topic list a bootstrapper creates is not the same thing as the topic list the live pipeline actually uses**, and the only reliable way to tell them apart is tracing actual producer and consumer code, exactly as this handbook has been doing chapter by chapter.

The topics that *are* live, end to end, in the deployed system are: `api_requests` (Chapter 3 → Chapter 8), the six `banking.*` CDC topics (Chapter 5 → Chapter 8 and Chapter 9), `dead_letter_queue` (Chapter 3's DLQ fallback), and `fraud_alerts_v2`/`critical_alerts_v2` (Chapter 8 → Chapter 2's alert aggregator and Chapter 9).

#### 6.2.1 Partitions, keys, and ordering

**Repository fact.** Every topic here is created with **3 partitions**. A Kafka topic is not a single ordered log — it's a set of independently-ordered logs (partitions), and Kafka only guarantees message ordering *within* a single partition, not across a whole topic. Which partition a given message lands in is determined by its **key**: messages with the same key are always routed to the same partition (via a hash of the key), and therefore are always delivered to any single consumer of that partition in the exact order they were produced.

This matters enormously for correctness in this platform, and it's worth being precise about where it's actually relied upon. `KafkaRequestLoggerMiddleware` (Chapter 3) keys `api_requests` messages by `request_id` — a value that's different for every message, meaning ordering *between* requests is not preserved, which is fine, because nothing about fraud scoring depends on the relative order of two unrelated requests from two different users. `rules_engine.py` (Chapter 7) keys its alerts by `ip_address`, and the Flink engine (Chapter 8) explicitly `keyBy(RiskEvent::getIdentityKey)`s its entire stream before scoring — both of these choices exist specifically *because* per-identity ordering matters: two logins from the same customer need to be processed in the order they actually happened for indicators like `IMPOSSIBLE_TRAVEL` (which compares consecutive logins) to be computed correctly. Debezium's CDC topics, by contrast, key messages by the source table's primary key (its default behavior), which is what guarantees all changes to a single `account_id` arrive at any consumer in the order PostgreSQL actually applied them — a subtle but critical correctness property for the account-enrichment broadcast join covered in Chapter 8.

#### 6.3 Consumer groups: how five independent systems read the same topic without stepping on each other

**Repository fact.** Every Kafka consumer in this codebase declares a `group.id`: the Flink engine uses `fraud-engine-v2` (`RiskScoringJob.java`, set via `FLINK_RISK_GROUP_ID`), the OpenSearch sink uses `opensearch-sink-group` (shared identically across every one of its per-topic threads — Chapter 9 covers why this is deliberate and safe), the legacy `rules_engine.py` uses `fraud-phase1-velocity-check-v2`, and `alert_aggregator.py` uses `fraud_incident_aggregator`.

A **consumer group** is Kafka's mechanism for both of two related things at once: (1) allowing multiple consumer *instances* within the same group to divide a topic's partitions among themselves for parallel processing (each partition is read by exactly one consumer within a given group at a time), and (2) allowing multiple, entirely independent *groups* to each read the same topic's full contents completely independently of each other, each maintaining its own separately-tracked read offset. This second property is the one that matters most for this platform's architecture: the Flink engine's `fraud-engine-v2` group and the OpenSearch sink's `opensearch-sink-group` can both consume every message on `banking.login_events` in full, at their own pace, without either one affecting the other's progress or even being aware the other exists — which is exactly the fan-out behavior described in the "Why it Exists" section above, made concrete.

**Repository fact.** `rules_engine.py`'s consumer group is literally named `fraud-phase1-velocity-check-v2` and `alert_aggregator.py`'s is `fraud_incident_aggregator` — names that read as historical development-phase labels rather than permanent architectural identifiers, another small but genuine signal (alongside the unused-topic list in §6.2) of this platform's iterative build history being visible directly in its configuration.

#### 6.4 Schema Registry and Avro

**Repository fact.** The `schema-registry` service runs `confluentinc/cp-schema-registry:7.4.0`, exposed on host port `18081` (mapped from its internal `8081`) — a deliberate remap, not the default `8081:8081`, because port `8081` is subject to a Windows Hyper-V dynamic port exclusion range on the development machine this platform was built on, a real, concrete example of a host-environment quirk shaping a piece of infrastructure configuration.

A Schema Registry solves a specific problem that arises the moment more than one service needs to serialize and deserialize the same kind of message: if every producer and consumer independently embeds its own understanding of a message's structure, there is no shared, authoritative definition of what a valid message actually looks like, and no mechanism to catch a producer that starts sending a subtly incompatible payload before that payload reaches (and potentially breaks) a consumer. Confluent's Schema Registry solves this by centralizing schema definitions (here, in **Avro** format — a compact, binary, schema-driven serialization format, in contrast to the plain-JSON format Debezium's CDC topics use, per Chapter 5's distinction) under **subjects** (by default, one subject per topic, e.g. `api_requests-value`, `fraud_alerts_v2-value`), and enforcing **compatibility rules** on any new schema version registered under a subject — so a producer can't accidentally publish a breaking schema change without either the registry rejecting it outright, or the change being consciously classified as an intentional breaking change.

**Repository fact.** Three schemas are actively registered and used in this platform: `api_request.avsc` (Chapter 3's telemetry schema, a flat seven-field record), and the two Avro schemas the Flink engine's `FraudAlertAvroSerializationSchema` (Chapter 8) constructs programmatically in Java and registers against subjects `fraud_alerts_v2-value` and `critical_alerts_v2-value` respectively — notably, *not* loaded from a `.avsc` file on disk the way `api_request.avsc` is, but built as an inline Avro `Schema.Parser().parse(SCHEMA_JSON)` call directly in Java source. Both approaches are valid; the difference is again a visible signature of the same platform's two components (the Python API and the Java Flink engine) having been built independently, each choosing its own natural idiom for defining an Avro schema in its own language.

Wire-format-wise, every Avro message serialized through the Schema Registry client libraries used here (`confluent_kafka.schema_registry.avro` in Python, `ConfluentRegistryAvroSerializationSchema` in Java) follows the same **Confluent wire format**: a leading magic byte (`0x00`), followed by a 4-byte big-endian schema ID, followed by the Avro-encoded payload. This is exactly the format `alert_aggregator.py`'s `decode_message()` function (Chapter 2, §2.5) manually parses when it needs to be resilient to *either* Avro-or-JSON input:

```python
def decode_message(msg_value: bytes) -> dict:
    if msg_value[0] == 0:
        magic, schema_id = struct.unpack('>bI', msg_value[:5])
        if schema_id not in SCHEMA_CACHE:
            url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
            with urllib.request.urlopen(url) as response:
                schema_str = json.loads(response.read().decode())['schema']
                SCHEMA_CACHE[schema_id] = fastavro.parse_schema(json.loads(schema_str))
        bytes_reader = io.BytesIO(msg_value[5:])
        return fastavro.schemaless_reader(bytes_reader, SCHEMA_CACHE[schema_id])
    else:
        return json.loads(msg_value.decode("utf-8"))
```

This is a genuinely instructive piece of code for understanding the wire format at a level below what any client library abstracts away: it manually recognizes the magic byte, fetches the writer schema directly from the registry's REST API by ID (rather than through the higher-level `AvroDeserializer` class used elsewhere in this codebase), caches it locally to avoid a registry round-trip on every single message, and falls back to plain JSON parsing for anything that doesn't start with the magic byte at all — a defensive design choice that lets this one worker tolerate whichever of Kafka's two encoding conventions (Avro-with-registry, or plain JSON) happens to be in use on whatever topic it's pointed at.

### Runtime Behaviour

At startup, `kafka-init` blocks every dependent service (`connect-init`, `fraud-engine`, via `depends_on: kafka-init: condition: service_completed_successfully` in `docker-compose.yml`) until every topic in §6.2 exists — a real, working instance of Chapter 12's broader "serialize the startup chain" strategy, applied specifically to guarantee no consumer or producer ever races against topic auto-creation. (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'` is also set on the broker itself, as a secondary safety net — a producer sending to a not-yet-created topic would trigger its auto-creation with default settings rather than failing outright — but `kafka-init` ensures every topic is created deliberately, with the intended partition count, rather than relying on that fallback.)

Once running, each consumer group tracks its own committed offset per partition, independently. A slow or temporarily-down consumer (say, the OpenSearch sink restarting after a code deploy) does not lose any messages — it simply resumes from its last committed offset once it reconnects, re-reading anything it hadn't yet acknowledged. This is precisely the mechanism Chapter 9 relies on when it discusses the OpenSearch sink's manual, per-message commit strategy in detail, and precisely why a consumer crashing mid-message is a recoverable, "redeliver and retry" situation rather than a lost-data situation — as long as the consumer hasn't committed an offset past a message it never successfully processed.

### Learning Concepts

- **The publish-subscribe log, as distinct from a work queue.** Kafka's core primitive — a durable, ordered, replayable log, read by many independent consumer groups — versus the classic message-queue model where each message is claimed by exactly one worker and then gone.
- **Partitions and the ordering guarantee they provide.** Kafka guarantees order only within a partition; achieving "all events for this entity, in the order they happened" requires a deliberate keying strategy, not something Kafka provides automatically across an entire topic.
- **Consumer groups as the unit of both parallelism and isolation.** One mechanism serves two purposes: dividing work across instances *within* a group, and providing complete independence *between* groups.
- **Raft-based consensus (KRaft) vs. an external coordination service (ZooKeeper).** A broader distributed-systems pattern — moving cluster metadata management from an external dependency into the system's own consensus protocol — visible here as a specific, concrete instance in Kafka's own recent architectural evolution.
- **Schema governance and the Confluent wire format.** Centralizing schema definitions and compatibility enforcement in a registry, and the specific binary convention (magic byte + schema ID + payload) that lets a consumer resolve a message's schema without it being embedded in every single message.

### Production Perspective

- **Replication factor.** Every topic here is created with `--replication-factor 1` — meaning a single broker failure permanently loses any data not yet consumed and durably persisted elsewhere. Production Kafka clusters run with a replication factor of at least 3, spread across separate brokers (and often separate availability zones), so that the loss of any single broker doesn't lose data.
- **Multi-broker, dedicated-controller KRaft topology.** As noted in §6.1, this single-node `broker,controller` configuration is explicitly a demo/homelab topology. Production KRaft deployments typically run a small dedicated controller quorum (3 or 5 nodes) separate from a larger, independently-scalable set of broker nodes.
- **Managed Kafka.** As mentioned in Chapter 1, most companies running Kafka at real production scale today use a managed offering (Confluent Cloud, AWS MSK, Redpanda Cloud, or an equivalent) specifically to avoid needing deep in-house Kafka operations expertise — partition rebalancing, broker capacity planning, and upgrade orchestration are genuinely difficult operational disciplines.
- **Schema compatibility policy.** This platform's Schema Registry runs with whatever the image's default compatibility mode is (`BACKWARD`, by default, meaning a new schema version must be readable by consumers using the *previous* schema version) but nothing in this repository explicitly configures or tests against that policy. Production teams typically choose a compatibility mode deliberately (`BACKWARD`, `FORWARD`, or `FULL`) based on their deployment order guarantees (can consumers always be upgraded before producers, or vice versa?) and enforce it as a CI gate on schema changes, not just registry-side.
- **Topic retention tuning.** No topic here has an explicit retention policy configured, meaning every topic uses the broker's cluster-wide default (`log.retention.hours`, typically 168 hours / 7 days, unless overridden — this cluster doesn't override it). Production deployments tune retention per-topic based on actual replay and compliance needs — a fraud-alert topic, for regulatory reasons, might need much longer retention than a raw request-telemetry topic.

### Common Pitfalls

- **Confusing "topic exists" with "topic is on the live data path."** As §6.2 demonstrated concretely, a bootstrapper creating a topic says nothing about whether any live producer or consumer actually uses it — always trace real code, not configuration lists, to determine what's actually flowing.
- **Assuming ordering across an entire topic.** A very common mistake for anyone new to Kafka: assuming "topic" implies "one ordered stream," when the actual unit of ordering is the partition, and cross-partition ordering requires a deliberate keying decision.
- **Advertised-listener misconfiguration.** As discussed in §6.1, a client connecting to the wrong advertised address (e.g., a host-machine client trying to use the internal Docker network address) is one of the single most common Kafka connectivity failures for newcomers, and the error messages it produces (connection timeouts, or successful connects that then silently fail to consume) are often confusing enough to send someone debugging the wrong layer entirely.
- **Treating Avro and plain-JSON topics uniformly.** As shown by `alert_aggregator.py`'s dual-format `decode_message()` and the `AVRO_TOPICS` set in `opensearch_sink.py` (Chapter 9), this cluster genuinely mixes both encodings across different topics, and a consumer that assumes uniform serialization will fail — sometimes silently — on whichever category it didn't anticipate.
- **Under-provisioning consumer group parallelism relative to partition count.** Not directly an issue in this repository (every topic has 3 partitions and no consumer here runs more than one instance), but a common real-world Kafka mistake worth knowing: a consumer group can have at most as many *active* consuming instances as the topic has partitions — additional instances beyond that sit idle.

### Summary

Kafka, running here in KRaft mode without a ZooKeeper dependency, is the shared nervous system every other chapter in this handbook routes through — and this chapter grounded that abstraction in concrete mechanics: partitions and keys as the actual unit of ordering, consumer groups as the mechanism that lets Flink, the OpenSearch sink, and historical Python scripts all read the same topics independently, and the Schema Registry's Confluent wire format as the contract that governs the Avro-encoded subset of this platform's topics. The chapter also demonstrated a broader, transferable investigative skill: comparing what a bootstrapper *provisions* against what live code actually *produces and consumes* is often the fastest way to separate an event-driven system's current architecture from its historical residue. With the event backbone now fully understood, the next chapter turns to the first generation of consumers built against it — the lightweight Python stream-processing scripts that predate the Flink engine, and what their limitations teach about when a simple in-memory consumer stops being enough.

---

*Next: [Chapter 7 — Python Stream Processors](07-stream-processing-python.md)*

# Chapter 7 — Python Stream Processors

*Previous: [Chapter 6 — Apache Kafka & Schema Registry](06-kafka-and-schema-registry.md) · Next: [Chapter 8 — The Apache Flink Risk Engine (Java)](08-flink-risk-engine-java.md)*

---

### Overview

Before the Java Flink engine described in the next chapter existed, this platform's fraud-scoring logic was implemented in plain Python: two small, standalone scripts under `analytics/kafka-stream-scripts/`, plus two PyFlink prototypes under `analytics/apache-flink-scripts/`. None of these four files has a service definition anywhere in `docker-compose.yml`. This chapter documents them anyway — not despite their not being live, but *because of it*: this is one of the few places in the entire repository where you can directly observe an architecture's evolution, side by side, and see exactly what problems motivated the eventual move to a real stream-processing framework.

### Why it Exists

It's worth asking directly: why keep dead code in a repository at all, and why does a handbook about this platform's architecture spend a full chapter on code that isn't running?

The honest answer is that this is genuinely common, healthy engineering practice — building a fast, low-investment prototype first to validate that an idea works end-to-end, before committing to the much larger investment of a proper stream-processing framework — and understanding *why* the prototype was outgrown is one of the most concrete, transferable lessons this entire repository has to teach. Chapter 8's Java engine will make a great deal more sense once you've seen, specifically, which problems a hand-rolled Python consumer loop cannot solve no matter how carefully it's written — not because Python is a bad language for this, but because of structural properties of the *deployment model* (a single, un-checkpointed process holding state in local memory) that no amount of careful Python code can fix.

### Implementation in This Repository

#### 7.1 `rules_engine.py` — the velocity-check rule

**Repository fact.** `analytics/kafka-stream-scripts/rules_engine.py` consumes the `api_requests` topic (Avro-decoded against `api_request.avsc`, using the consumer group `fraud-phase1-velocity-check-v2`), and implements a single rule: if the same IP address hits a "sensitive" endpoint (`/transfers`, `/login`, `/beneficiaries`) three or more times within a 10-second window, it produces a `VELOCITY_VIOLATION` alert to the `fraud_alerts` topic (plain JSON, no Avro):

```python
ip_activity_state = defaultdict(list)
alert_cooldown_state = {}
ALERT_COOLDOWN_SEC = 30

def evaluate_velocity_rule(ip_address, endpoint, current_time_ms):
    if not any(sensitive in endpoint for sensitive in SENSITIVE_ENDPOINTS):
        return
    current_time_sec = current_time_ms / 1000.0

    if ip_address in alert_cooldown_state:
        if current_time_sec - alert_cooldown_state[ip_address] < ALERT_COOLDOWN_SEC:
            return  # still in the "penalty box"

    timestamps = ip_activity_state[ip_address]
    timestamps.append(current_time_sec)
    ip_activity_state[ip_address] = [t for t in timestamps if current_time_sec - t <= TIME_WINDOW_SEC]
    current_count = len(ip_activity_state[ip_address])

    if current_count >= VELOCITY_THRESHOLD:
        producer.produce('fraud_alerts', key=ip_address, value=json.dumps(alert_payload))
        producer.flush()
        alert_cooldown_state[ip_address] = current_time_sec
        ip_activity_state[ip_address] = []
```

**Repository fact.** The `alert_cooldown_state` dictionary and `ALERT_COOLDOWN_SEC = 30` constant — nicknamed "the penalty box" directly in the code's own comments — is a genuinely important addition to this script's history: without it, once an IP crosses the velocity threshold, *every single subsequent request* from that IP for as long as it remains over-threshold would re-trigger the alert condition and produce a new alert, flooding the `fraud_alerts` topic with duplicate signal for what is, semantically, one ongoing incident. The cooldown suppresses re-alerting for 30 seconds after a trigger and resets the IP's tracked activity window, so activity has to build back up from zero before the rule can fire again for that same IP.

**Architectural inference.** This "penalty box" is a hand-rolled, single-process precursor to exactly the same problem the Java engine's `COOLDOWN_MS` alert-suppression logic solves properly in Chapter 8 (with the crucial difference that the Java engine's cooldown state is checkpointed, keyed, and TTL-managed by Flink itself, not a bare Python `dict`) — and it's also a precursor, at a conceptual level, to the deduplication `alert_aggregator.py` performs on the *incident* side (Chapter 2, §2.5), folding repeat alerts for the same identity into one case within a time window. The same underlying problem — "a threshold, once crossed, tends to stay crossed for a while, and naively re-alerting on every subsequent event floods a human or downstream system with duplicate noise" — appears independently, and is independently solved, in at least three different places across this codebase's history. That's a good sign for the platform's engineering instincts even though it means the same lesson was learned three separate times rather than being centralized once.

#### 7.2 `risk_scoring_engine.py` — the multi-indicator aggregator

**Repository fact.** `analytics/kafka-stream-scripts/risk_scoring_engine.py` subscribes to four topics — `api_requests`, `transactions`, `login_events`, `fraud_alerts` — and maintains a per-`customer_id` in-memory dictionary tracking a cumulative score with time-based decay:

```python
risk_state = defaultdict(lambda: {"score": 0, "indicators": [], "last_updated": 0.0})

def process_event(event_type, customer_id, current_time):
    profile = risk_state[customer_id]
    if current_time - profile["last_updated"] > SCORE_DECAY_WINDOW_SEC:
        profile["score"] = 0
        profile["indicators"] = []
    weight = RISK_WEIGHTS[event_type]
    profile["score"] += weight
    profile["indicators"].append(event_type)
    profile["last_updated"] = current_time
    if profile["score"] >= ALERT_THRESHOLD:
        generate_structured_alert(customer_id, profile)
        profile["score"] = 0
        profile["indicators"] = []
```

Two details here matter more than they might first appear:

**Architectural inference #1 — topic names don't match reality.** This script subscribes to `transactions` and `login_events` — but as established in Chapter 5, the actual CDC topics produced by Debezium are `banking.transactions` and `banking.login_events`, with the `banking.` prefix. Similarly, it publishes its output to a topic called `high_risk_alerts`, which does not appear anywhere in `kafka-init`'s provisioned topic list (Chapter 6, §6.2). Taken together, this script — as written — would never actually receive any CDC-sourced events in this platform's real topology, and its output would land on an entirely unprovisioned topic nobody else reads. This is strong, direct evidence supporting the "superseded prototype" reading rather than "an alternate live consumer that just happens not to be in `docker-compose.yml`": the topic names themselves are stale, consistent with this script predating the `banking.` topic-prefix convention settled on in Chapter 5's connector configuration.

**Architectural inference #2 — state has no persistence, no partitioning, and no checkpointing.** `risk_state` is a plain Python `defaultdict`, living entirely in this one process's memory. If the process restarts — a code deploy, a crash, a host reboot — every customer's accumulated score and decay timer is gone, instantly and silently, with no recovery path. If you wanted to run two instances of this script for throughput (say, one per CPU core, or one per host), each instance would maintain its *own separate* `risk_state`, and a customer whose events happened to be processed by both instances (Kafka's consumer-group partition assignment offers no guarantee that all of one customer's events land on one specific consumer instance unless the *producer* deliberately keys them for that partition-count) would have their risk silently split across two independent, disagreeing tallies. Neither of these is a bug exactly — it's simply what a single, stateful, in-memory Python process *is*, structurally, and it is precisely the gap Apache Flink's managed, checkpointed, keyed state (Chapter 8) is purpose-built to close.

#### 7.3 The PyFlink prototypes

**Repository fact.** `analytics/apache-flink-scripts/flink_risk_engine.py` and `flink_cep_engine.py` are written against **PyFlink** — Flink's official Python API — rather than plain Kafka consumer loops. `flink_risk_engine.py` implements a `KeyedProcessFunction` subclass, `RiskScoringProcessor`, storing a JSON-serialized `{"score": ..., "indicators": [...]}` blob inside a single Flink `ValueState[str]`, consuming from a topic populated by "Phase 1" (the `rules_engine.py` output). `flink_cep_engine.py` goes further, using Flink's **Table API** (SQL-like declarative stream definitions) to build four Kafka-backed tables (`banking.login_events`, `banking.beneficiaries`, `banking.transactions`, `banking.accounts`), unions them into an intermediate `banking.unified_timeline` topic, and then runs a SQL `MATCH_RECOGNIZE` pattern-matching query against that unified stream — Flink's built-in **Complex Event Processing (CEP)** capability for expressing "find this specific sequence of event types, in order, within a time window" declaratively, rather than hand-coding the sequence-tracking state machine in imperative code.

**Architectural inference.** These two scripts represent a real, meaningful step up in sophistication from the plain Kafka consumer loops in §7.1-7.2 — using PyFlink means they already benefit from Flink's managed, checkpointed, keyed state rather than a bare Python dictionary, which directly addresses the persistence and partitioning gap identified in §7.2. What they don't yet reflect is the specific, detailed schema-correctness work visible throughout the Java engine (Chapter 8) — `flink_risk_engine.py` expects its upstream events pre-labeled with a `risk_indicator_type` field (the output shape of `rules_engine.py`, not of any live CDC topic), and neither script contains the kind of "confirmed against the real Transaction model, which has `from_account`/`to_account`, not a `beneficiary_id` column" verification comments that appear throughout `RiskEventNormalizer.java`. Read together, the four scripts in this chapter form a legible progression: bare Kafka consumer with local Python state → managed Flink state via PyFlink → (Chapter 8) a production Java Flink job with verified schema assumptions, broadcast-state enrichment, async I/O, and Schema-Registry-integrated Avro output. Each step solves a specific, identifiable limitation of the step before it.

### Runtime Behaviour

None of the four files in this chapter execute in the platform as deployed — there is no `docker-compose.yml` service, Dockerfile, or CI/CD step that builds or runs any of them. If a developer were to run `rules_engine.py` manually, connected to this platform's live Kafka broker, it would function exactly as described in §7.1: consuming real `api_requests` traffic, correctly Avro-decoding it (its schema-loading and deserialization logic is current and correct, unlike `risk_scoring_engine.py`'s stale topic names), and genuinely producing velocity-violation alerts to `fraud_alerts` — a topic that exists (Chapter 6, §6.2) but that no live consumer reads. Running it alongside the deployed system today would not break anything (it's an independent consumer group, per Chapter 6, §6.3), but it also would not visibly do anything useful, since its output has no live downstream reader.

### Learning Concepts

- **Prototype-first development in stream processing.** Validating a stream-processing idea's *logic* with a minimal, low-investment consumer loop before committing to a full framework is a legitimate and common practice — the mistake would be *staying* at the prototype stage once the workload outgrows what a single in-memory process can correctly support, not building the prototype in the first place.
- **In-memory state vs. managed, checkpointed state.** The core distinction this whole chapter builds toward: a plain Python dictionary living in one process's heap has no durability, no automatic partitioning across instances, and no recovery story, whereas a stream-processing framework's managed state (Flink's `ValueState`, `MapState`, etc., covered fully in Chapter 8) is checkpointed to durable storage, automatically partitioned by key across parallel task instances, and recoverable after a failure.
- **Alert suppression / deduplication as a recurring, independently-discovered pattern.** The "penalty box" in `rules_engine.py`, the cooldown logic in the Java engine (Chapter 8), and the incident-aggregation window in `alert_aggregator.py` (Chapter 2) are three separate implementations of the same underlying insight — worth recognizing as a generalizable pattern (rate-limit *re-notification* about an already-known condition, not just the underlying signal) rather than three unrelated features.
- **Complex Event Processing (CEP).** A specialized class of stream-processing query that looks for specific temporal sequences or patterns across multiple event types, typically expressed declaratively (Flink's `MATCH_RECOGNIZE` SQL extension, used in `flink_cep_engine.py`) rather than hand-coded as an imperative state machine.
- **Schema drift between prototype and production.** `risk_scoring_engine.py`'s stale `transactions`/`login_events` topic names versus the real `banking.transactions`/`banking.login_events` names is a small, concrete instance of a very common real-world problem: code that was correct when written can silently stop matching reality as the rest of a system evolves around it, with nothing forcing a re-check unless the code is actually run against the live system (or, as this handbook has been doing throughout, cross-referenced deliberately against current configuration).

### Production Perspective

None of the code in this chapter is what a production team would deploy, and that's a feature of this handbook's honesty, not a criticism of the code itself — every mature streaming platform has files exactly like these somewhere in its git history. The industry-standard progression this chapter's four files trace is a real one:

- **Bare consumer loop → Kafka Streams / PyFlink → a full framework deployment (Flink, as this platform ultimately chose, or Spark Structured Streaming).** The decision point is almost always state: the moment a stream-processing task needs correctness guarantees around partitioned, recoverable, potentially-large keyed state — exactly the gap identified in §7.2 — a bare consumer loop stops being viable, regardless of how well-written it is.
- **Kafka Streams** (not present in this repository, but worth naming as the most common lightweight alternative to a separate Flink cluster) embeds stream processing directly inside the application's own JVM process, with state stored in embedded RocksDB and changelog-topic-backed recovery — a meaningfully lighter operational footprint than running a separate Flink cluster, at the cost of being JVM/Kafka-specific and less expressive for complex windowing and CEP than Flink's dedicated engine. A team choosing between Kafka Streams and Flink for a workload like this one is really choosing between "one more embedded library dependency" and "one more cluster to operate," and the right answer depends heavily on whether the team already has other reasons to run and know Flink well.
- **Spark Structured Streaming** is the other commonly-discussed alternative, and the most important distinction to understand is that it's fundamentally **micro-batch** under the hood (even in its "continuous processing" mode, which has narrower operator support than the standard micro-batch engine) rather than Flink's true event-at-a-time processing model — meaning Spark Structured Streaming trades a small amount of latency (typically hundreds of milliseconds to low seconds of added batch latency) for an execution model many teams already have deep operational experience with from Spark's batch-processing use elsewhere in their stack.

### Common Pitfalls

- **Deploying a prototype's assumptions unchanged.** `risk_scoring_engine.py`'s stale topic names are exactly what happens when a script isn't re-validated against a system that has moved on around it — a strong argument for integration tests that exercise real topic names against a real (or realistic-fake) broker, rather than trusting that a script "still probably works."
- **Underestimating what "in-memory state" actually costs you.** It's easy to write a working demo with a plain dictionary and feel like the stream-processing problem is solved — the gaps (no recovery after a crash, no correctness under multiple parallel instances) are invisible until you actually hit them, often in production, often at the worst possible time.
- **Conflating "uses Kafka" with "is a real stream processor."** All four scripts in this chapter consume from and produce to Kafka, but only the PyFlink pair (§7.3) uses an actual stream-processing framework underneath — reading and writing Kafka messages in a `while True: poll()` loop is a valid, simple integration pattern, but it is not, on its own, stream processing in the sense the rest of this handbook uses the term.
- **Leaving dead infrastructure provisioned without documentation.** The unused topics this chapter's scripts reference (`fraud_alerts`, `high_risk_alerts`) cost nothing to leave provisioned, but without a handbook like this one cross-referencing them explicitly, a future engineer exploring this repository could easily lose real time trying to figure out whether they're supposed to be live.

### Summary

This chapter deliberately spent its full attention on code that doesn't run in the deployed platform, because doing so is the clearest possible way to understand *why* the Java Flink engine in the next chapter is built the way it is. `rules_engine.py` and `risk_scoring_engine.py` are correct, working demonstrations of the core scoring logic — weighted indicators, cumulative scores, cooldown-based alert suppression — implemented with nothing more than a Kafka client library and a Python dictionary; the PyFlink scripts are a real step toward managed state. What none of the four can fully provide is durable, partitioned, checkpointed state at the scale and reliability a real fraud-scoring workload needs — and that gap is exactly what Apache Flink, deployed properly with a real cluster, RocksDB-backed state, and checkpointing, was brought in to close. The next chapter is the longest in this handbook, because it's where every concept introduced so far — CDC envelopes, Kafka partitioning and keys, Avro and the Schema Registry, and the state-management gap this chapter just identified — comes together in a single, production-deployed system.

---

*Next: [Chapter 8 — The Apache Flink Risk Engine (Java)](08-flink-risk-engine-java.md)*

# Chapter 8 — The Apache Flink Risk Engine (Java)

*Previous: [Chapter 7 — Python Stream Processors](07-stream-processing-python.md) · Next: [Chapter 9 — OpenSearch: Threat Hunting & Search](09-opensearch-threat-hunting.md)*

---

This is the longest chapter in this handbook, and deliberately so. `analytics/flink-risk-engine-java/` is the single component every other chapter has been building toward: it is where the CDC path and the application-telemetry path (Chapter 1) actually converge, where Kafka's partitioning and keying (Chapter 6) get put to real use, and where the state-management gap identified at the end of Chapter 7 is finally closed properly. If you understand this chapter fully, you understand the architectural core of the entire platform.

### Overview

Apache Flink is a distributed stream-processing framework designed around one core idea: process events one at a time, as they arrive, while maintaining **large amounts of durable, partitioned, fault-tolerant state** — not as an afterthought bolted onto a stateless processing model, but as a first-class capability the entire engine is architected around. This is precisely what distinguishes it from the Python scripts in Chapter 7: where `risk_scoring_engine.py`'s `risk_state` dictionary vanishes the instant the process restarts, Flink's equivalent state survives broker restarts, task failures, and even entire job restarts, because it's periodically checkpointed to durable storage and automatically restored from the most recent checkpoint on recovery.

**Repository fact.** This platform's Flink deployment consists of a `flink-jobmanager` container (the cluster's coordinator — schedules tasks, tracks checkpoints, serves the web UI on port `8082`), one or more `flink-taskmanager` containers (the workers that actually execute the job's operators, configured here with `taskmanager.numberOfTaskSlots: 2`), and a `fraud-engine` container that builds and *submits* a Java job (`RiskScoringJob`) to that already-running cluster, then exits.

### Why it Exists

Chapter 7 already made the state-durability argument for Flink in the abstract. It's worth restating it here in the specific, concrete terms this codebase uses, because the "why" is what makes the implementation choices in the rest of this chapter legible rather than arbitrary:

- **Keyed state, partitioned automatically.** `RiskScoringProcessor` needs, for every customer identity, a running tally of recent activity — recent transaction timestamps, the last known device and country, an accumulating risk score. Flink's `keyBy(RiskEvent::getIdentityKey)` call (§8.3) guarantees that every event for a given identity is routed to the same parallel task instance, and that task's state for that key is isolated from every other key's state, entirely automatically — no hand-written partitioning logic, no risk of two parallel workers disagreeing about one customer's tally (the exact failure mode identified in Chapter 7, §7.2).
- **Exactly-once state consistency via checkpointing.** `env.enableCheckpointing(15_000L, CheckpointingMode.EXACTLY_ONCE)` (§8.3) means Flink periodically takes a consistent, distributed snapshot of every operator's state and its exact position in every input stream, atomically. If the job crashes, it restarts from the most recent checkpoint and reprocesses only what it needs to, arriving at *the same state it would have had if the crash never happened* — a guarantee no hand-rolled Python consumer loop can offer without reimplementing a meaningful fraction of what Flink already provides.
- **Correct event-time semantics under out-of-order arrival.** Real distributed systems don't deliver events in perfect global order — a `login_events` CDC event and a related `api_requests` telemetry event for the same real-world moment can arrive at Flink milliseconds or seconds apart, in either order, depending on Kafka Connect scheduling and network conditions. Flink's watermark mechanism (§8.3) gives this job a principled way to reason about "how out-of-order can this stream be before I stop waiting and process what I have," rather than either naively trusting arrival order or waiting forever.

### Implementation in This Repository

#### 8.1 The topology, top to bottom

**Repository fact.** `RiskScoringJob.java`'s `main()` method defines the entire processing pipeline as a chain of DataStream transformations. Reading it as a diagram makes the shape of the whole job much easier to hold in your head than reading the Java line by line:

```text
KafkaSource: banking.transactions, banking.login_events,   KafkaSource: banking.accounts
             banking.sessions, api_requests                              │
                       │                                                  │
                       ▼                                                  ▼
        RiskEventNormalizer::normalize (map)              AccountEventNormalizer::normalize (map)
        filter(event != null)                              filter(update != null)
                       │                                                  │
                       │                                                  ▼
                       │                                    .broadcast(ACCOUNT_STATE_DESCRIPTOR)
                       │                                                  │
                       └───────────────► .connect() ◄────────────────────┘
                                            │
                                            ▼
                         AccountEnrichmentFunction (BroadcastProcessFunction)
                         resolves account_id → customer_id for transactions
                                            │
                                            ▼
                          filter(event.getIdentityKey() != null)
                                            │
                                            ▼
                AsyncDataStream.unorderedWait(..., OpenSearchLookupFunction, ...)
                        non-blocking async lookup, 5s timeout, 100 in-flight
                                            │
                                            ▼
                assignTimestampsAndWatermarks(forBoundedOutOfOrderness(300s), idleness 60s)
                                            │
                                            ▼
                          keyBy(RiskEvent::getIdentityKey)
                                            │
                                            ▼
                       RiskScoringProcessor (KeyedProcessFunction)
                                            │
                                            ▼
                                  DataStream<FraudAlert>
                                    │                │
                         filter severity=="FRAUD"   filter severity=="CRITICAL"
                                    │                │
                                    ▼                ▼
                        fraud_alerts_v2 sink   critical_alerts_v2 sink
                        (Avro, Schema Registry)  (Avro, Schema Registry)
```

Every box in this diagram is discussed in its own subsection below, in the order the data actually flows through them — which is also, not coincidentally, the order in which this job's own extensive code comments narrate the reasoning behind each stage. This job's source code is unusually well-annotated with the *history* of its own design decisions (comments beginning "NEW", "CHANGED", or "CONFIRMED" appear throughout), and this chapter draws on that history directly rather than treating the final code as if it always looked this way.

#### 8.2 Sources: four topics, one broadcast

**Repository fact.** `RiskScoringJob` defines two separate `KafkaSource` instances:

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers(props.getProperty("bootstrap.servers"))
    .setTopics("banking.transactions", "banking.login_events", "banking.sessions", "api_requests")
    .setGroupId(props.getProperty("group.id"))
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

KafkaSource<String> accountsSource = KafkaSource.<String>builder()
    .setTopics("banking.accounts")
    .setGroupId(props.getProperty("group.id") + "-accounts")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
```

Both sources deserialize every message as a raw `String` (`SimpleStringSchema`) rather than a typed Avro or JSON deserializer at the Kafka-source level — the actual JSON parsing happens explicitly inside `RiskEventNormalizer`/`AccountEventNormalizer` (§8.3-8.4) using Jackson's `ObjectMapper`, giving the job full manual control over how it handles the CDC-envelope-vs-plain-telemetry distinction (Chapter 5, §5.3) rather than delegating that decision to a generic deserialization schema.

**Repository fact.** `banking.accounts` is deliberately kept on its own `KafkaSource`, with its own distinct consumer group id (`<group.id>-accounts`), separate from the main four-topic source. This isn't an oversight — it's what makes the broadcast pattern in §8.4 possible: the accounts stream is intentionally treated as an entirely separate stream with a different role (reference/lookup data) rather than being unioned into the main event stream Flink scores against.

#### 8.3 Normalization: turning four different shapes into one `RiskEvent`

**Repository fact.** `RiskEventNormalizer.normalize()` is the single point in this job where the heterogeneity of its four source topics gets resolved into one uniform shape, `RiskEvent`. Its class-level Javadoc is worth quoting directly, because it documents a real, specific verification process against the actual running schema, not an assumption:

> Field-name assumptions below have been checked against the real SQLAlchemy models (models.py):
>  - login_events: customer_id, device_id, country, ip_address, success ✓ confirmed correct
>  - transactions: from_account, to_account, amount, currency, transaction_type, timestamp — NO customer_id, NO beneficiary_id.
>  - sessions: session_id, customer_id, device_id, ip_address, created_at, expires_at — NO country/geo field.

This single comment block explains three separate, non-obvious design decisions that ripple through the rest of the engine:

1. **Why `transactions` needs the account-enrichment broadcast join at all (§8.4).** A `transactions` row genuinely has no `customer_id` column (confirmed identically in `schema.sql`, Chapter 4, §4.2) — only `from_account`/`to_account`. Without resolving `account_id → customer_id`, a transaction event has no way to be keyed by the same identity as that customer's logins.
2. **Why `IMPOSSIBLE_TRAVEL` is derived from consecutive `login_events`, not from `sessions`.** The `sessions` table has no country or geo column at all — it would be architecturally natural to expect session data to carry location, but this schema simply doesn't provide it, so the indicator has to be derived from a different signal entirely (§8.6 shows exactly how).
3. **Why the CDC-envelope-unwrapping logic here so closely mirrors `opensearch_sink.py`'s (Chapter 9).** Both files are independently solving the exact same underlying problem — extracting real column values from Debezium's `{before, after, source, op, ts_ms}` envelope — because CDC's envelope structure (Chapter 5, §5.3) is a shared contract every consumer has to honor, regardless of language.

**Repository fact.** `normalize()` builds an `identityKey` using an explicit, three-tier fallback:

```java
if (customerId != null) {
    identityKey = customerId;
} else if (ipAddress != null) {
    identityKey = "ip:" + ipAddress;      // api_requests carries no customer_id
} else if (fromAccountId != null) {
    identityKey = "acct:" + fromAccountId; // interim identity, until the accounts join resolves it
} else {
    identityKey = null;
}
```

This fallback chain is the concrete mechanism behind a subtlety flagged directly in the job's own comments: `api_requests` events (an anonymous IP hitting the API before any login has happened) get keyed by `"ip:<address>"`, and a `transactions` event whose paying account hasn't yet been resolved to a customer via the broadcast join gets a temporary `"acct:<from_account>"` key — **not dropped**, scored under a slightly different identity than its eventual "real" customer key, rather than being silently discarded while the join catches up. Events with no usable identity signal at all (`identityKey == null`) are the only ones actually filtered out, and only *after* the enrichment stage in §8.4 has had a chance to resolve them — filtering earlier, the code's own comment notes, "would permanently drop any transaction whose account_id → customer_id mapping hasn't been broadcast yet."

#### 8.4 Enrichment: the broadcast state pattern

This is one of the more advanced pieces of Flink machinery in this codebase, and it's worth slowing down for, because the pattern it demonstrates — joining a high-volume stream against a small, slowly-changing reference dataset — is extremely common in real streaming systems well beyond fraud detection.

**Repository fact.** `AccountEnrichmentFunction extends BroadcastProcessFunction<RiskEvent, AccountUpdate, RiskEvent>`. In `RiskScoringJob`, the `banking.accounts` CDC stream (normalized into `AccountUpdate` objects by `AccountEventNormalizer`) is turned into a `BroadcastStream` via `.broadcast(AccountEnrichmentFunction.ACCOUNT_STATE_DESCRIPTOR)`, and the main `RiskEvent` stream is joined against it via `.connect(accountsBroadcast).process(new AccountEnrichmentFunction())`.

**Broadcast state**, as a Flink concept, exists for exactly this situation: a dataset (here, the `account_id → customer_id` mapping) that is *small relative to the main event stream* and needs to be visible, in full, to *every parallel instance* of an operator — as opposed to Flink's ordinary keyed state, where each parallel instance only ever sees the slice of state belonging to the keys routed to it. Broadcasting the accounts stream, rather than trying to key-partition it the same way as transactions, means every parallel task processing transactions has an up-to-date, complete local copy of the account-to-customer mapping to consult, with no network round-trip needed at scoring time.

```java
public void processElement(RiskEvent event, ReadOnlyContext ctx, Collector<RiskEvent> out) {
    if (event.getCustomerId() == null && event.getFromAccountId() != null) {
        String resolvedCustomerId = ctx.getBroadcastState(ACCOUNT_STATE_DESCRIPTOR).get(event.getFromAccountId());
        if (resolvedCustomerId != null) {
            event.setCustomerId(resolvedCustomerId);
            event.setIdentityKey(resolvedCustomerId);
        }
        // else: mapping not yet known — keep the acct:<from_account> fallback identity
    }
    out.collect(event);
}

public void processBroadcastElement(AccountUpdate update, Context ctx, Collector<RiskEvent> out) {
    BroadcastState<String, String> state = ctx.getBroadcastState(ACCOUNT_STATE_DESCRIPTOR);
    if (update.isDeleted()) {
        state.remove(update.getAccountId());
    } else if (update.getCustomerId() != null) {
        state.put(update.getAccountId(), update.getCustomerId());
    }
}
```

**Repository fact — a documented, honestly-acknowledged limitation.** This class's own Javadoc states plainly:

> KNOWN LIMITATION, inherent to the broadcast state pattern (not something this implementation can fix on its own): updates to the account mapping and events on the main stream are not guaranteed to be processed in a strict global order relative to each other. In practice this means a transaction on a brand-new account could occasionally be scored before its corresponding banking.accounts row has propagated through this join.

This is a genuinely important, generalizable lesson about distributed stream processing, not a defect specific to this codebase: broadcast state does not guarantee that a broadcast update "wins the race" against a main-stream event that logically depends on it, because the two streams are physically separate Kafka topics, consumed and delivered independently, with no cross-stream ordering contract between them. The engineering response here — accepting the fallback `"acct:<from_account>"` identity for that rare race-condition case rather than either blocking to wait for the join or dropping the event — is a deliberate, reasoned tradeoff, explicitly favoring availability and forward progress over strict correctness for an edge case, and documenting that choice plainly rather than hiding it.

#### 8.5 Async I/O: looking outside the stream without blocking it

**Repository fact.** `OpenSearchLookupFunction extends RichAsyncFunction<RiskEvent, RiskEvent>`, and is invoked via `AsyncDataStream.unorderedWait(withIdentity, new OpenSearchLookupFunction(), 5000L, TimeUnit.MILLISECONDS, 100)`. For every event, it issues a non-blocking HTTP request (Java 11's `java.net.http.HttpClient`, using `sendAsync()`) to OpenSearch's `_count` API, asking whether this identity has appeared in the `login_events` index at any point *older* than Flink's own 24-hour state TTL — i.e., long-term historical context that falls outside what Flink's own in-state window can already answer for free.

This operator exists to demonstrate — and this codebase's own comments say so directly — "Async I/O practice implementation": Flink's `AsyncDataStream` API is specifically designed for exactly this scenario, where a per-event operation needs to make an external network call, and doing so *synchronously* (blocking each event's processing on the external call's response) would collapse the whole pipeline's throughput down to whatever that external system's per-request latency allows. `unorderedWait` (as opposed to `orderedWait`) is chosen specifically because this lookup's result has no ordering requirement relative to other events — the code comment makes this explicit: "events can complete out of order without affecting correctness, which gives Flink more freedom to pipeline concurrent requests." The `100` parameter bounds the number of concurrently in-flight requests, protecting the job from unbounded queue growth if OpenSearch becomes slow or unreachable; the `5000L` timeout matches the HTTP client's own configured request timeout.

**Repository fact — a deliberately unfinished feature.** The result of this lookup, `seenBeyondStateWindow`, is stored on the `RiskEvent` but — as both the `RiskEvent` field's own Javadoc and `OpenSearchLookupFunction`'s class comment state explicitly — **is not currently read anywhere in `deriveIndicators()` or the scoring logic**. This is documented as "a deliberate follow-up decision left for later, not an oversight, since it changes real alerting behavior and shouldn't happen as a side effect of adding this pattern." This is worth calling attention to as a mature engineering practice worth emulating: adding new plumbing (the async lookup, the field to hold its result) *before* wiring it into a behavior-changing decision, and explicitly documenting that gap rather than either rushing the wiring-in or silently leaving dead code with no explanation, is a genuinely good habit — it lets the infrastructure change and the business-logic change be reviewed, tested, and rolled back independently.

**Repository fact.** Both `OpenSearchLookupFunction`'s error branch and its `timeout()` override apply the exact same handling: set `seenBeyondStateWindow` to `null` (not `false`) and let the event continue through the pipeline regardless. The class comment explains why `null` specifically, rather than `false`: "`null` means 'not looked up yet, or the lookup failed/timed out' (fail-open), distinct from a real `false` meaning 'looked up, genuinely never seen before the state window.'" This is a careful, deliberate use of Java's nullable `Boolean` wrapper type (rather than a primitive `boolean`) specifically to distinguish "we don't know" from "we know the answer is no" — a distinction that matters enormously to correctness the moment this field *is* wired into scoring, and would be silently impossible to represent with a primitive `boolean`.

#### 8.6 Watermarks and event-time processing

**Repository fact.**

```java
DataStream<FraudAlert> alerts = withHistoricalContext
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<RiskEvent>forBoundedOutOfOrderness(Duration.ofSeconds(300))
            .withTimestampAssigner((event, ignored) -> event.getEventTime())
            .withIdleness(Duration.ofSeconds(60))
    )
    .keyBy(RiskEvent::getIdentityKey)
    .process(new RiskScoringProcessor())
```

Flink distinguishes **event time** (when something actually happened, per its own embedded timestamp — here, `RiskEvent.eventTime`, itself derived from the CDC envelope's `ts_ms`, per Chapter 5, §5.3's precision discussion) from **processing time** (when Flink itself happens to observe the event). This job uses event time, which is the correct choice for fraud detection: a scoring decision should be based on when a login or transaction *actually occurred*, not on however long it took for that event to travel through PostgreSQL's WAL, Debezium, and Kafka before reaching Flink.

A **watermark** is Flink's mechanism for answering the question "have I seen everything that's going to arrive up to time T, or should I keep waiting?" — since event-time streams can arrive out of order, Flink can never be *certain* no more late data is coming, so a watermark is a heuristic bound: `forBoundedOutOfOrderness(Duration.ofSeconds(300))` tells Flink "assume no event will ever arrive more than 300 seconds later than the latest event-time timestamp already seen." This directly governs how long the decay timer and cooldown logic in `RiskScoringProcessor` (§8.7) can trust that "now" (`event.getEventTime()`) genuinely reflects the furthest point the stream has progressed to. `withIdleness(Duration.ofSeconds(60))` handles a related but distinct problem: a low-traffic key (an identity that simply hasn't produced any new events in a while) shouldn't be allowed to stall the *entire* watermark's progress just because Flink is technically still "waiting" for that one quiet partition — after 60 seconds of no new events, that source is marked idle and excluded from watermark calculation until it produces something again.

#### 8.7 `RiskScoringProcessor`: the heart of the engine

This is the single most important class in the entire repository, so it's worth reading in the same order the code itself executes.

**Repository fact — state initialization (`open()`).** Three pieces of Flink-managed state are declared: `profileState` (a `ValueState<RiskProfileState>`, one object per key holding the customer's rolling profile — score, recent timestamps, last known device/country), `emittedAlertIds` (a `MapState<String, Long>` tracking already-emitted alert IDs, to prevent duplicate emission), and `seenEventKeys` (a `MapState<String, Long>`, a dedup guard against reprocessing the same event twice — relevant after a checkpoint-restore replay). All three have a **State TTL** configuration applied via `StateTtlConfig`, set to expire after `RiskScoringConfig.TTL_MS` (24 hours) of inactivity, cleaned up via `cleanupInRocksdbCompactFilter(1000)` — meaning expired state entries are physically purged during RocksDB's own background compaction process, checking wall-clock time after every 1000 processed entries, rather than needing a separate active cleanup job. A code comment here is candid about a real, fixed mistake: an earlier version of this method called a `cleanupInBackground()` method that doesn't actually exist on Flink 1.18's `StateTtlConfig.Builder` API — "that was my mistake in the previous patch and broke the build" — a small, honest, and genuinely useful data point about how easy it is to misremember a specific framework version's exact API surface, and a reminder that this handbook's "repository fact" labeling reflects code that has itself been debugged and corrected over time, not code that was correct on the first attempt.

**Why `MapState` instead of `ValueState<Map<...>>`?** A comment directly in the code answers this: "Using MapState instead of ValueState<Map> for O(1) RocksDB serialization." This is a meaningful and generalizable Flink performance lesson: a `ValueState<Map<K,V>>` requires Flink to (de)serialize the *entire* map on every single read or write to *any* key within it — expensive as the map grows. A `MapState<K,V>` is backed directly by RocksDB's own key-value structure, so reading or updating one entry touches only that entry, not the whole collection — a difference that matters enormously once `emittedAlertIds` or `seenEventKeys` grows to hold more than a handful of entries per key.

**`processElement()`, step by step:**

```java
public void processElement(RiskEvent event, Context ctx, Collector<FraudAlert> out) throws Exception {
    if (event == null || event.getIdentityKey() == null) return;
    RiskProfileState state = profileState.value();
    if (state == null) state = new RiskProfileState();
    long now = event.getEventTime();

    // 1. Dedup
    pruneOlderThan(seenEventKeys, now, RiskScoringConfig.TTL_MS);
    if (event.getDedupKey() != null) {
        if (seenEventKeys.contains(event.getDedupKey())) { profileState.update(state); return; }
        seenEventKeys.put(event.getDedupKey(), now);
    }

    // 2. Score decay
    applyDecay(state, now);

    // 3. Indicator derivation (see §8.8)
    List<String> indicators = deriveIndicators(event, state, now);
    if (!indicators.isEmpty()) {
        int weightSum = 0;
        for (String indicator : indicators) {
            weightSum += RiskScoringConfig.DEFAULT_WEIGHTS.getOrDefault(indicator, 10);
            state.addIndicator(indicator);
        }
        state.setCumulativeScore(state.getCumulativeScore() + weightSum);
        state.setLastScoreUpdateTs(now);
    }

    // 4. Ensure a decay timer is scheduled
    if (state.getCumulativeScore() > 0 && state.getNextDecayTimerTs() <= now) {
        long nextTs = now + RiskScoringConfig.DECAY_MS;
        ctx.timerService().registerEventTimeTimer(nextTs);
        state.setNextDecayTimerTs(nextTs);
    }

    // 5. Threshold check + cooldown-gated alerting
    boolean shouldAlert = state.getCumulativeScore() > RiskScoringConfig.ALERT_THRESHOLD;
    boolean shouldEscalate = state.getCumulativeScore() >= RiskScoringConfig.CRITICAL_THRESHOLD;
    if (shouldAlert) {
        long alertTs = state.getLastAlertTs();
        boolean cooldownExpired = alertTs == 0 || (now - alertTs) >= RiskScoringConfig.COOLDOWN_MS;
        if (cooldownExpired) {
            String severity = shouldEscalate ? "CRITICAL" : "FRAUD";
            String alertId = buildDeterministicAlertId(event.getIdentityKey(), severity, now);
            pruneOlderThan(emittedAlertIds, now, RiskScoringConfig.TTL_MS);
            if (!emittedAlertIds.contains(alertId)) {
                FraudAlert alert = new FraudAlert(alertId, ..., new ArrayList<>(state.getActiveIndicators()), ...,
                    shouldEscalate ? "LOCK_ACCOUNT" : "REQUIRE_MFA");
                state.setLastAlertTs(now);
                emittedAlertIds.put(alertId, now);
                out.collect(alert);
            }
        }
    }
    profileState.update(state);
}
```

**Deterministic alert IDs are worth pausing on specifically.**

```java
static String buildDeterministicAlertId(String identityKey, String severity, long eventTime) {
    long window = Math.max(1L, RiskScoringConfig.COOLDOWN_MS);
    long windowStart = (eventTime / window) * window;
    String safeIdentity = identityKey.replaceAll("[^a-zA-Z0-9]", "");
    return String.format("%s-%s-%d", safeIdentity, severity.toLowerCase(), windowStart);
}
```

This function computes an alert ID from *only* the identity, severity, and which fixed-length cooldown window the event time falls into — no random UUID, no incrementing counter. Two consequences follow directly from this, and both are deliberate: first, the same logical alert (same identity, same severity, same cooldown window) always produces the exact same ID no matter how many times it's computed, which is what makes the `emittedAlertIds.contains(alertId)` check in step 5 an effective safeguard against **duplicate emission after a checkpoint-restore replay** — if Flink recovers from a checkpoint and reprocesses a handful of already-seen events, it will recompute the *same* alert ID for anything it already alerted on, and the guard silently skips re-emitting it, rather than double-alerting a human analyst about something already in their queue. Second, this is a specific, concrete instance of **idempotent event production** as a design pattern — a topic revisited in Chapter 13 — achieved here without any external deduplication service, purely by making the ID a deterministic function of the event's own logical content.

#### 8.8 `deriveIndicators()`: turning raw fields into fraud signal

**Repository fact.** `deriveIndicators()` switches on `event.getSourceTable()` and computes different indicators per source:

| Source table | Indicator | Trigger condition |
|---|---|---|
| `login_events` | `NEW_DEVICE` | Current login's `device_id` differs from `state.lastKnownDeviceId` |
| `login_events` | `NEW_COUNTRY` | Current login's `country` differs from `state.lastKnownCountry` |
| `login_events` | `IMPOSSIBLE_TRAVEL` | A `NEW_COUNTRY` change *and* the time since the previous login is within `IMPOSSIBLE_TRAVEL_WINDOW_MS` (2 hours) |
| `login_events` | `MULTIPLE_FAILED_LOGINS` | 3 or more failed logins within a 5-minute rolling window (`FAILED_LOGIN_THRESHOLD`/`FAILED_LOGIN_WINDOW_MS`) |
| `transactions` | `LARGE_TRANSFER_AMOUNT` | `amount > LARGE_TRANSFER_THRESHOLD` (10,000) |
| `transactions` | `VELOCITY_VIOLATION` | 4 or more transactions within a 5-minute rolling window |
| `transactions` | `HIGH_RISK_RECIPIENT` | `to_account` appears in a configured allowlist (`HIGH_RISK_RECIPIENTS`, empty by default) |
| `api_requests` | `SUSPICIOUS_API_PATTERN` | 50 or more requests within a 60-second rolling window |
| `sessions` | *(none)* | Explicitly a no-op — see below |

The `sessions` case is worth quoting directly, because it's another instance of the same honest, schema-driven reasoning already seen in §8.3:

> "sessions" intentionally has no case: the real Session model ... carries none of the fields the original taxonomy needed (no country/geo). Sessions events still flow through dedup/state but contribute no indicator until a genuine geo-capable signal is available on that table.

Every one of these rolling-window counts (`recordFailedLogin`, `recordTransaction`, `recordApiRequest`) is backed by `RiskProfileState`'s `recordAndPrune()` helper — a bounded ring buffer (hard-capped at `MAX_RETAINED_TIMESTAMPS = 50` regardless of configured window size) that appends the new timestamp, prunes anything older than the window, and returns the resulting count. This hard cap is a deliberate "lean-state guarantee," in the code's own words: even under a pathological event burst that would otherwise make a per-key list grow without bound, this structure physically cannot exceed 50 entries — an important defensive property for state that gets checkpointed regularly and needs to stay bounded in size regardless of adversarial input.

**Repository fact — score decay.** `applyDecay()` halves the cumulative score for every full `DECAY_MS` (30-minute) interval that has elapsed since the state was last decayed, capped at 10 steps to avoid an unbounded loop for a key that's been idle for a very long time:

```java
int steps = (int) Math.min(elapsed / RiskScoringConfig.DECAY_MS, 10);
int decayed = state.getCumulativeScore();
for (int i = 0; i < steps; i++) { decayed = decayed / 2; }
state.setCumulativeScore(decayed);
```

This is an exponential decay function (a score halves every fixed interval, exactly like radioactive half-life), and it's what gives the whole scoring model its temporal character: a burst of five suspicious indicators within a few minutes accumulates a high score quickly and triggers an alert; the same five indicators spread naturally across several days, each one decayed to near-zero before the next arrives, never crosses the threshold at all. This is precisely the mechanism that operationalizes the "burst vs. spread-out" intuition Chapter 1 raised conceptually — a customer whose behavior looks unusual in a tight time window is different, in a way that matters, from a customer whose long history simply happens to include several unrelated unusual events.

### Runtime Behaviour

#### 8.9 How the job actually gets onto the cluster

**Repository fact.** `fraud-engine`'s `Dockerfile` uses a multi-stage build: a `maven:3.9.9-eclipse-temurin-11` stage compiles the job into a shaded JAR, then the *final* image is built from `flink:1.18-scala_2.12-java11` — the exact same base image `flink-jobmanager`/`flink-taskmanager` already run — specifically so this container has the `flink` CLI available, version-matched to the cluster it's about to talk to. The container's entrypoint is `submit-job.sh`, and its own comments document a real, fixed architectural mistake from earlier in the project's history:

> CHANGED (P1 resolution): this container no longer runs the job's `main()` directly. It builds/holds the JAR and SUBMITS it to the real flink-jobmanager/flink-taskmanager cluster via the flink CLI ... instead of falling back to Flink's embedded LocalStreamEnvironment inside its own process.

This is worth understanding precisely because the failure mode it describes is subtle and easy to reproduce accidentally in any Flink project: if you invoke a Flink job's `main()` method directly (`java -cp app.jar com.banking.fraud.RiskScoringJob`) rather than submitting it *to a running cluster* via the `flink` CLI, `StreamExecutionEnvironment.getExecutionEnvironment()` silently falls back to spinning up a **local, embedded mini-cluster inside that one JVM process** — the job genuinely runs, genuinely produces output, and *looks* completely correct in isolated testing, while being entirely disconnected from the `flink-jobmanager`/`flink-taskmanager` containers the rest of the platform assumes are doing the actual work. `submit-job.sh` fixes this by polling `flink list -m <jobmanager>:<port>` as a readiness probe, checking whether `banking-fraud-risk-engine-v2` (the job name set in `env.execute(...)`) is already running to avoid submitting a duplicate competing instance on the same consumer group, and then running `flink run -m <target> -d /app/app.jar` — the `-d` flag submitting it **detached**, meaning the job continues running on the cluster independently of this container's own lifecycle once submission succeeds. This is exactly why `docker compose ps` correctly shows `bank_fraud_engine` as `Exited (0)` shortly after startup, the same one-shot-init pattern already seen in `connect-init` and `kafka-init` (Chapters 5 and 6) — here applied to job submission rather than API-driven bootstrapping.

#### 8.10 What happens once the job is live

Once submitted, `RiskScoringJob` runs continuously on the Flink cluster, consuming from its five source topics (four in the main union, one broadcast) indefinitely. Every 15 seconds, a checkpoint barrier flows through the entire operator graph, and once every operator has acknowledged it, Flink commits that checkpoint as the new recovery point — meaning at any given moment, this job can be at most slightly more than 15 seconds of reprocessing away from a fully consistent recovery, regardless of failure. `env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10_000L))` governs what happens if a task does fail: up to 3 automatic restart attempts, 10 seconds apart, before the job is considered permanently failed and requires manual intervention.

Alerts, once emitted, flow to `fraud_alerts_v2`/`critical_alerts_v2` via `FraudAlertAvroSerializationSchema`, which delegates to Flink's `ConfluentRegistryAvroSerializationSchema` — registering (or resolving, if already registered) the alert's Avro schema against the live Schema Registry (Chapter 6, §6.4) and writing the standard Confluent wire format, exactly what `alert_aggregator.py` and `opensearch_sink.py` both expect to be able to decode on the consuming side.

### Learning Concepts

- **Keyed state and automatic partitioning.** Flink's `keyBy()` guarantees both routing (all events for one key go to one task instance) and state isolation (that task's state for that key is invisible to and unaffected by any other key), entirely automatically — the exact property Chapter 7 identified as missing from a bare Python consumer loop.
- **Checkpointing and exactly-once state consistency.** A periodic, distributed, consistent snapshot mechanism that lets a stream-processing job recover from failure without losing or double-counting state — genuinely one of the hardest problems in distributed systems, solved once, generically, by the framework, rather than needing to be solved per-application.
- **Broadcast state.** The correct Flink pattern for joining a high-volume keyed stream against a small, slowly-changing reference dataset that needs to be fully visible to every parallel task instance — and, as this codebase's own honest documentation shows, a pattern with a real, inherent cross-stream ordering limitation worth understanding rather than assuming away.
- **Asynchronous I/O in a streaming pipeline.** `AsyncDataStream` exists specifically to prevent a per-event external call (here, an OpenSearch lookup) from collapsing a job's throughput down to that external system's latency — `unorderedWait` versus `orderedWait` as a choice governed by whether a specific operation's result has any ordering dependency on other events.
- **Event time vs. processing time, and watermarks.** The distinction between "when something happened" and "when the system observed it," and watermarks as the mechanism that lets an event-time-based job reason about bounded out-of-order arrival without waiting indefinitely for arbitrarily late data.
- **Idempotent event production via deterministic IDs.** Deriving a message's own identity purely from its logical content (identity + severity + time window, here) rather than a random or incrementing value, so that reprocessing the same logical event — after a checkpoint restore, for instance — naturally produces the same ID and can be safely deduplicated downstream.
- **RocksDB as an embedded state backend.** `EmbeddedRocksDBStateBackend` stores Flink's keyed state in an embedded RocksDB instance on local disk (rather than purely in the JVM heap), enabling state sizes far larger than available memory, at the cost of serialization overhead per access — which is exactly why the `MapState`-vs-`ValueState<Map>` distinction in §8.7 matters as much as it does.

### Production Perspective

**Repository fact — the job runs at `env.setParallelism(1)`.** This is worth being direct about: this job does not currently take advantage of Flink's core scaling mechanism at all. Every operator runs as a single parallel instance, meaning the entire job's throughput is bounded by what one `flink-taskmanager` slot can process, and the two `taskmanager.numberOfTaskSlots: 2` configured in `docker-compose.yml` are not currently exploited by this specific job's parallelism setting. For a homelab deployment processing simulator-generated traffic, this is an entirely reasonable, deliberate simplification — it avoids the additional complexity of reasoning about partition-to-task-slot assignment while the job's core correctness was being built out. A production deployment handling real transaction volume would raise `setParallelism()` well above 1 (matched to the number of Kafka partitions on the source topics — recall each topic here has exactly 3 partitions, per Chapter 6, §6.2, which would itself need to increase for meaningfully higher parallelism to help) and would correspondingly scale out the number of TaskManager instances and slots available to run those parallel operator instances across.

**Kafka Streams, as the lighter-weight alternative raised in Chapter 7.** For a workload of this specific shape — moderate volume, keyed aggregation, no complex windowed joins beyond what's implemented here — Kafka Streams is a completely legitimate alternative architecture, embedding the exact same conceptual model (keyed state, checkpointing via changelog topics, exactly-once processing) directly inside the application's own JVM process rather than as a job submitted to a separately-operated cluster. The tradeoff is operational, not conceptual: Kafka Streams removes an entire piece of infrastructure (no JobManager, no TaskManager, no separate deployment step) at the cost of tying the processing logic's scaling and deployment lifecycle directly to a JVM application process, and losing access to Flink-specific capabilities this job actually uses — the broadcast state pattern (§8.4) and Flink's native `AsyncDataStream` API (§8.5) don't have precise Kafka Streams equivalents, so migrating this specific job to Kafka Streams would require re-architecting both of those pieces, not just swapping a dependency.

**Spark Structured Streaming**, the other alternative named in Chapter 7, would be a meaningfully worse fit for this specific job's requirements, and it's worth being concrete about why rather than treating the three frameworks as interchangeable: Spark Structured Streaming's execution model is fundamentally micro-batch (grouping events into small batches and processing each batch as a mini Spark job), which adds a batch-interval's worth of latency to every event by design — directly working against this platform's near-real-time fraud-detection goal (Chapter 1, §1.5). Flink's true event-at-a-time processing model, used throughout this job, is the better-matched choice specifically *because* latency is a first-class requirement here, not an incidental preference.

**Scoring in-line, before the transfer completes.** Chapter 2, §2.2 raised this possibility and deferred it here. Architecturally, moving fraud scoring fully in-line with the API request (blocking a transfer's HTTP response on a synchronous Flink lookup) would require a fundamentally different design than a Kafka-consuming background job — most realistically, a **synchronous request-response call from the API directly into a low-latency scoring service** backed by the same kind of state this Flink job maintains (frequently implemented as a separate, purpose-built low-latency service reading from the same underlying state store, sometimes via Flink's own queryable state feature, rather than the streaming job itself serving synchronous requests). This platform's current design — score asynchronously, alert as a fast-follow, and let the *incident* workflow (Chapter 2, §2.5) drive any account-locking action after the fact — is a real, common production pattern (many fraud systems genuinely operate this way, accepting a short window of exposure in exchange for much simpler, more scalable architecture), but it is a deliberate choice with a real tradeoff, not the only correct answer, and Chapter 13 revisits this tradeoff explicitly.

### Common Pitfalls

- **Running a Flink job's `main()` directly instead of submitting it to a cluster.** As detailed in §8.9, this is a real, documented mistake this exact codebase made and fixed — the job silently falls back to an embedded local mini-cluster that looks correct in isolation but is completely disconnected from the real cluster, and the failure is easy to miss because nothing obviously errors.
- **Filtering out events too early in a multi-stage enrichment pipeline.** §8.3 showed the `identityKey != null` filter deliberately placed *after* the account-enrichment broadcast join rather than before it — filtering earlier would permanently discard events the join was specifically designed to rescue.
- **Assuming broadcast state provides cross-stream ordering guarantees.** §8.4's documented race condition is a real, inherent property of the broadcast state pattern in any Flink job, not something specific to sloppy implementation — any engineer using this pattern needs to explicitly decide how to handle the "reference data hasn't arrived yet" case, because Flink itself won't resolve that ambiguity for you.
- **Choosing `ValueState<Map<K,V>>` over `MapState<K,V>` for large or frequently-updated collections.** As explained in §8.7, this is a real, meaningful performance difference under RocksDB, not a stylistic preference — one requires whole-collection (de)serialization on every access, the other doesn't.
- **Forgetting that watermark idleness handling matters for low-traffic keys.** Without `withIdleness()`, a single quiet partition can stall an entire job's watermark progress indefinitely, silently delaying processing for every *other*, active key in the stream — an easy thing to miss until traffic patterns become uneven in practice.
- **Wiring in a new signal (like `seenBeyondStateWindow`) before deciding, deliberately, how it should affect behavior.** §8.5's "plumbing before behavior" discipline is presented here as a pitfall in reverse — the mistake to avoid is doing it the other way around, wiring a new signal directly into a threshold or scoring decision without first validating the signal's own correctness in isolation.

### Summary

`RiskScoringJob` is where every architectural thread from Chapters 1 through 7 converges: CDC events and application telemetry (Chapters 3 and 5) are normalized into one shape, keyed and enriched using Kafka's partitioning and Flink's broadcast state (Chapter 6, and §8.4 above), scored using durable, checkpointed, partitioned state that closes the exact gap Chapter 7 identified in the platform's earlier Python prototypes, and emitted as Avro-encoded alerts back onto the Kafka backbone via the Schema Registry (Chapter 6, §6.4). This chapter also deliberately surfaced this job's own documented history — a fixed job-submission bug, an honestly-acknowledged broadcast-state race condition, a deliberately unfinished async-lookup feature — because a production-grade stream-processing job's value lies as much in how carefully its edge cases and limitations are understood and documented as in its core logic being correct. With alerts now flowing out of Flink, the next chapter follows them to their two destinations: the OpenSearch sink that makes this platform's data searchable and dashboard-ready, and (already covered in Chapter 2, §2.5) the incident-aggregation worker that turns a stream of alerts into a human-manageable case queue.

---

*Next: [Chapter 9 — OpenSearch: Threat Hunting & Search](09-opensearch-threat-hunting.md)*

# Chapter 9 — OpenSearch: Threat Hunting & Search

*Previous: [Chapter 8 — The Apache Flink Risk Engine](08-flink-risk-engine-java.md) · Next: [Chapter 10 — The Simulator & Attack Scenario Engine](10-simulator-and-attack-scenarios.md)*

---

### Overview

Everything the platform has produced so far — API telemetry, CDC-captured database changes, and Flink's fraud alerts — exists as Kafka messages: fast to move, cheap to fan out to multiple consumers, but not something a human analyst can meaningfully browse, filter, or visualize. OpenSearch is where those event streams become **searchable, dashboard-ready data**: a distributed search and analytics engine (a fork of Elasticsearch, maintained by Amazon and the broader open-source community following Elastic's 2021 licensing change) that indexes documents and supports fast, complex queries — full-text search, range filters, aggregations, and geospatial queries — over potentially very large volumes of data.

**Repository fact.** This platform runs a single-node OpenSearch cluster (`opensearchproject/opensearch:2.11.0`), an accompanying OpenSearch Dashboards UI (`opensearchproject/opensearch-dashboards:2.11.0`, exposed on port `5601`), and a purpose-built Python daemon (`analytics/opensearch/opensearch_sink.py`) whose entire job is reading from Kafka and writing into OpenSearch, continuously.

### Why it Exists

Kafka is deliberately a poor fit for the kind of question a fraud analyst actually needs to ask: "show me every failed login from Russia in the last hour," "which employee accessed the most customer records this week," "plot every login attempt on a map." Kafka topics support sequential, offset-based reading — a consumer processes messages roughly in the order they were produced, optionally filtered by partition, but there is no native way to ask Kafka itself "give me every message where `country = 'RU'`" without reading and filtering the entire topic client-side. OpenSearch exists specifically to answer exactly these kinds of ad hoc, filtered, aggregated questions efficiently, over data that's already been indexed.

This is a specific instance of a very general architectural principle worth naming explicitly: **the storage and query engine best suited to *producing* an event (a durable, ordered, replayable log) is rarely the same engine best suited to *analyzing* it after the fact (an inverted-index-backed search engine).** Rather than trying to make one system do both jobs, this platform uses Kafka for what it's good at (durable, ordered, replayable transport) and OpenSearch for what *it's* good at (fast, flexible, ad hoc querying), with a dedicated sink process bridging the two — a pattern often referred to as **CQRS** (Command Query Responsibility Segregation) at the infrastructure level: the systems that produce and mutate data (PostgreSQL, the API) are architecturally separate from the systems optimized for querying and reading it (OpenSearch), connected by an asynchronous pipeline rather than being the same system wearing two hats.

### Implementation in This Repository

#### 9.1 One consumer thread per topic — and a real, documented incident about why

This is the single most instructive piece of this chapter, and it's worth reading `opensearch_sink.py`'s own comments closely, because they describe a genuine production-style incident, diagnosed with real tooling, not a hypothetical.

**Repository fact.** `run_opensearch_sink()` spawns one dedicated Python thread *per topic*, each running its own independent `confluent_kafka.Consumer` instance, all sharing the same consumer group id (`opensearch-sink-group`):

```python
TOPIC_INDEX_MAP = {
    "api_requests": "api_requests",
    "banking.login_events": "login_events",
    "banking.employee_actions": "employee_actions",
    "fraud_alerts_v2": "alerts",
    "critical_alerts_v2": "alerts",
}

def run_opensearch_sink():
    initialize_indices()
    threads = []
    for topic, target_index in TOPIC_INDEX_MAP.items():
        t = threading.Thread(target=consume_topic, args=(topic, target_index), daemon=True, name=f"consumer-{topic}")
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
```

`consume_topic()`'s own docstring documents exactly why this design exists, rather than the simpler-looking alternative of one `Consumer` subscribed to all five topics at once:

> CHANGED: previously one Consumer subscribed to ALL FOUR topics at once, processing whatever poll() returned, serially. Confirmed via `kafka-consumer-groups --describe` that this caused real starvation, not just theoretical risk: banking.login_events had ~77,000 messages of lag on a single partition ... and fraud_alerts_v2/critical_alerts_v2 showed CURRENT-OFFSET "-" — meaning the consumer had NEVER once committed an offset for either alert topic, despite Flink having already produced 132 real alert messages between them.

This is a real, general lesson about Kafka's consumer poll loop worth understanding on its own terms, independent of this specific repository: `Consumer.poll()`, when subscribed to multiple topics, does not guarantee fair rotation across them — it returns whatever the client library's internal fetch logic happens to have ready, and a topic receiving a much higher message volume (here, `banking.login_events`, driven continuously by the simulator's baseline traffic) can dominate every single `poll()` call, **starving** lower-volume topics (the two alert topics) of any processing time at all, indefinitely, even though the starved topics have real, waiting data. The fix applied here — one `Consumer`, one thread, one topic each — completely eliminates the possibility of one topic starving another, because there is no shared poll loop left to dominate. The comment's closing note explains why sharing the same `group.id` across all five threads is still safe despite this: "Kafka tracks committed offsets per (group, topic, partition) independently, and group members are allowed to have different subscriptions, so there's no partition-assignment conflict between threads subscribed to disjoint topics."

**Architectural inference.** This incident and its fix are worth connecting directly back to Chapter 6, §6.3's discussion of consumer groups: it demonstrates that consumer-group membership and topic-starvation risk are two genuinely separate concerns. Sharing a `group.id` is about *offset tracking and rebalancing coordination*; the starvation bug was entirely about *how a single process's poll loop allocates its own attention across the topics it happens to be subscribed to*, and no amount of consumer-group configuration alone would have fixed it — the fix required restructuring *which topics share a poll loop at all*, which is an application-level threading decision, not a Kafka-level one.

#### 9.2 Deserialization: per-topic, not uniform

**Repository fact.** `deserialize_message()` branches explicitly on topic name:

```python
AVRO_TOPICS = {"fraud_alerts_v2", "critical_alerts_v2", "api_requests"}

def deserialize_message(topic, raw_key, raw_value):
    if topic in AVRO_TOPICS:
        return _avro_deserializer(raw_value, SerializationContext(topic, MessageField.VALUE))
    return json.loads(raw_value.decode("utf-8"))
```

This mirrors, independently and in a different language, the exact same Avro-vs-plain-JSON distinction Chapter 6, §6.4 traced through `alert_aggregator.py`'s manual wire-format parsing — here handled through the higher-level `confluent_kafka.schema_registry.avro.AvroDeserializer` class instead of manual magic-byte parsing, but solving precisely the same underlying problem: this Kafka cluster genuinely mixes Avro-encoded topics (`api_requests`, produced by the API's Avro-serializing middleware in Chapter 3; the two alert topics, produced by Flink's `ConfluentRegistryAvroSerializationSchema` in Chapter 8) with plain-JSON CDC topics (Debezium's `banking.*` output, per Chapter 5, §5.2's `schemas.enable: false` connector setting), and any single consumer that needs to read across both categories has to make this distinction explicitly, on a per-topic basis, because nothing about the wire bytes themselves is self-describing about which category they belong to without first checking for the Confluent magic byte.

#### 9.3 Unwrapping the CDC envelope — and a second, independently-discovered timestamp bug

**Repository fact.** `unwrap_cdc_envelope()` performs the same envelope-unwrapping job Chapter 8's `RiskEventNormalizer` performs in Java — extract the `after` block, skip deletes — but its docstring documents an additional, distinct correctness bug found and fixed specifically in *this* consumer, independent of the Flink side:

```python
def unwrap_cdc_envelope(payload: dict):
    if payload.get("op") == "d":
        return None
    after = payload.get("after")
    if after is None:
        return payload
    ts_ms = payload.get("ts_ms")
    if ts_ms is not None:
        after = dict(after)
        after["timestamp"] = ts_ms
    return after
```

> FIXED: also overrides `after.timestamp` with the envelope's own `ts_ms`. The raw column value Debezium puts in `after` depends on time.precision.mode (default "adaptive" -> microseconds for a plain TIMESTAMP column, confirmed against a real login_events row: a 16-digit value like 1783395584407664, not milliseconds). OpenSearch's "date" field type assumes milliseconds, so indexing the raw column value directly silently produced a wrong (or at least differently-scaled) date.

This is the exact same underlying precision trap Chapter 5, §5.3 introduced conceptually — a raw `TIMESTAMP` column value round-trips through Debezium at microsecond precision, not millisecond precision — but discovered and fixed *here*, independently, as a concrete downstream symptom: OpenSearch's `date` field mapping assumes millisecond epoch values, so indexing a microsecond value directly (sixteen digits instead of thirteen) doesn't error at all — it silently produces a timestamp many years in the future, wrong in a way that's completely invisible until someone notices a document's date field looks implausible. The fix is identical in spirit to Chapter 8's — trust the envelope's own documented-milliseconds `ts_ms` over the row's own column value — arrived at completely independently, in a different language, by tracing a different symptom (a Java `RiskEvent`'s scoring logic in one case, a visibly wrong OpenSearch document date in the other). This is worth treating as a genuinely important lesson rather than a coincidence: **a subtle, shared upstream data contract (Debezium's timestamp precision behavior) produces the same class of bug in every independent consumer that doesn't know to guard against it**, which is exactly why Chapter 5 flagged this specific detail as one worth remembering regardless of which language or framework a future consumer of this CDC pipeline happens to use.

**Repository fact.** `apply_geo_point()` performs a small, related transform specific to `login_events`: OpenSearch's `geo_point` field type expects a single `{lat, lon}` object, not two separate scalar columns, so this function combines the `latitude`/`longitude` columns (added to the schema via the migration discussed in Chapter 4, §4.1) into the shape the `login_events` index mapping expects, no-op-ing safely if either coordinate is missing — for example, rows written before that schema migration ran.

#### 9.4 Index mappings: correcting invented fields against the real schema

**Repository fact.** `initialize_indices()` defines explicit OpenSearch index mappings for `api_requests`, `login_events`, `employee_actions`, and `alerts`, and its comments document at least three separate corrections made by cross-referencing these mappings against the platform's *actual* running schemas, rather than assumed ones:

- The `api_requests` mapping originally included `customer_id`/`action`/`amount` fields — removed, because "none of those exist on `api_requests`; that schema carries no `customer_id` at all" (confirmed against `api_request.avsc`, Chapter 3).
- The `login_events` mapping's `status`/`location` fields were renamed to `success` (boolean) and `country` (keyword), "to match the real login_events columns confirmed in schema.sql" (Chapter 4).
- The `employee_actions` mapping previously invented `target_customer_id`/`account_id`/`ip_address`/`details` fields that "Debezium will ever actually send," and — critically — had renamed the real `customer_id` column to `target_customer_id`, which the comment notes "would have silently broken the Phase 4 mass-exfiltration query, since that query needs a cardinality aggregation on exactly this field."

**Architectural inference.** This third correction is worth sitting with, because it's the clearest illustration in this entire handbook of *why* the "verify against the real schema, don't assume" discipline visible throughout Chapters 4, 5, 8, and now here actually matters operationally, not just stylistically. An OpenSearch **cardinality aggregation** (counting distinct values of a field — here, presumably "how many distinct `customer_id`s did this one employee access") is exactly the kind of query a fraud analyst investigating insider threat needs to run, and it depends entirely on querying the *correct field name*. A field silently renamed to something plausible-sounding but wrong wouldn't produce an error — the aggregation would simply run against a field that either doesn't exist (returning zero results) or exists but means something else, and a human analyst trusting that query's output could draw a completely wrong conclusion about whether an employee's access pattern was suspicious, with no visible indication anything was wrong at all. This is a genuinely important, generalizable lesson about data pipelines feeding investigative or decision-making tools: a silent field-naming mistake several layers upstream of a query can corrupt a downstream analytical conclusion without ever throwing an exception anywhere in the pipeline.

#### 9.5 Manual offset commits and fail-open indexing

**Repository fact.** Every `consume_topic()` consumer is configured with `"enable.auto.commit": False`, and commits are issued **manually, per message**, only after a successful `os_client.index()` call:

```python
response = os_client.index(index=target_index, body=payload)
consumer.commit(message=msg, asynchronous=False)
```

If `os_client.index()` raises (a malformed document, a transient OpenSearch unavailability), the `except` block logs the error and explicitly does **not** commit — "this message gets redelivered from the last good offset on a sink restart rather than being skipped." This is Kafka's standard **at-least-once delivery** pattern, applied correctly: by committing only *after* confirmed successful processing, a crash or restart between "consumed the message" and "committed the offset" simply results in that message being redelivered and reprocessed on restart — safe here because indexing the same document twice into OpenSearch (same source data, same resulting document) is **idempotent** in the sense that matters for this specific data (each write simply produces an equivalent document; OpenSearch's default document-ID-less indexing does mean it would technically produce a *second*, duplicate document, discussed in the Common Pitfalls section below).

**Repository fact — a second, related fix.** A comment on the CDC-delete-event branch flags a distinct, previously-real bug: "FIXED: previously this `continue`d without committing, so delete events were redelivered forever on every sink restart instead of being acknowledged once and moving on." A `DELETE` CDC event has nothing to index (there's no `after` block), but "nothing to do" and "failed to do something" are different outcomes and need different offset-commit handling — this fix explicitly commits the offset for a legitimately-skipped delete event, rather than leaving it uncommitted (which, given the at-least-once semantics above, would have caused that same delete event to be redelivered and re-skipped on every single sink restart, forever, never actually making progress past it).

**Repository fact — dropping a synchronous refresh for a real, observed reliability cost.** A further comment explains the removal of `refresh=True` from the index call: forcing OpenSearch to synchronously refresh its Lucene segments after every single document write was slow enough, under `login_events`' real volume, to "blow past the consumer's `session.timeout.ms`," triggering repeated Kafka consumer group rebalances (confirmed via a `"SESSTMOUT... revoking assignment and rejoining group"` warning observed in the sink's own logs). Removing it and relying on OpenSearch's default ~1-second background refresh interval keeps the index near-real-time for a dashboard while no longer blocking the consumer loop long enough to fall out of its Kafka session.

### Runtime Behaviour

At startup, `initialize_indices()` creates any of the four index mappings (§9.4) that don't already exist, then five daemon threads spin up concurrently, each independently polling its own topic, deserializing per §9.2, unwrapping and transforming CDC payloads per §9.3 where applicable, indexing into OpenSearch, and manually committing its own offset — entirely independently of the other four threads, exactly as §9.1's starvation fix intends. From OpenSearch's perspective, documents simply accumulate continuously across all four indices as the simulator (Chapter 10) and Flink (Chapter 8) produce traffic and alerts, immediately available for querying and dashboarding via OpenSearch Dashboards on port `5601`, or programmatically via the `/incidents` and `/analytics/metrics` API endpoints covered in Chapter 3, which query PostgreSQL's incident tables rather than OpenSearch directly, but represent the same underlying alert data from a different angle.

### Learning Concepts

- **CQRS at the infrastructure level.** Separating the systems optimized for writing/producing data from the systems optimized for reading/querying it, connected by an asynchronous pipeline — here, PostgreSQL/Kafka on the write side, OpenSearch on the read side, bridged by this sink.
- **Consumer poll-loop starvation.** A single consumer subscribed to multiple topics of very different volume can have its attention monopolized by the highest-volume topic, indefinitely — a real, observable failure mode (confirmed here via `kafka-consumer-groups --describe` output, not assumed) rather than a theoretical concern, and one whose fix is architectural (separate poll loops), not configurational.
- **At-least-once delivery via manual, post-processing offset commits.** The standard pattern for ensuring no message is ever lost due to a crash between consumption and processing, at the cost of needing the processing step itself to be safely repeatable (idempotent, or tolerant of duplicates) for messages that get redelivered.
- **Idempotency in a data-sink context.** What "safe to reprocess" actually means for a search index specifically — distinct from, but related to, the idempotent alert-ID generation discussed in Chapter 8, §8.7.
- **Cross-referencing derived-system schemas against ground truth.** §9.4's three corrected index mappings are a direct, repeated demonstration of the same discipline seen throughout Chapters 4, 5, and 8 — treating an assumed or inherited schema as unverified until checked against the actual running system, because a plausible-looking but wrong field name fails silently, not loudly.

### Production Perspective

- **Single-node OpenSearch.** `discovery.type=single-node` means there is no replica shard for any index — a hardware failure on this one container loses all indexed data (though it remains re-derivable by replaying the source Kafka topics from their retained history, up to Kafka's own retention window, per Chapter 6). Production OpenSearch deployments run multi-node clusters with configured replica counts specifically so a single node's failure doesn't cause data loss or a search outage.
- **At-most-once duplicate documents on redelivery.** As flagged in §9.5, this sink's at-least-once-plus-manual-commit strategy is safe against message *loss*, but a message reprocessed after a crash-before-commit will be indexed as a **second, distinct document** (OpenSearch, by default, assigns each `index()` call its own auto-generated document ID unless one is explicitly supplied) — meaning this pipeline currently tolerates rare duplicate documents in exchange for guaranteeing no data loss. Production pipelines that need strict duplicate-free indexing typically supply a deterministic, content-derived document ID to `index()` (analogous to Chapter 8's deterministic alert IDs) so a redelivered message overwrites the same document rather than creating a new one — a genuinely straightforward improvement this specific sink doesn't yet make.
- **Security posture.** OpenSearch here runs with its security plugin enabled but `verify_certs=False` on the Python client side, against a self-signed certificate — appropriate for a homelab environment reachable only within its own Docker network, and explicitly flagged in this codebase's own comments as needing real certificate validation "before any production use" (the identical posture, and the identical caveat, also appears in Flink's `TrustAllSslContext`, Chapter 8, §8.5 — both consumers of the same OpenSearch instance independently making, and independently documenting, the same appropriate-for-now, not-appropriate-forever tradeoff).
- **Index lifecycle management.** No index rollover, retention, or archival policy is configured here — every index grows indefinitely as long as the sink runs. Production OpenSearch deployments typically configure Index State Management (ISM) policies to automatically roll over to new indices past a size or age threshold and eventually delete or archive old ones, both for storage cost control and for query performance (very large single indices degrade search latency over time).

### Common Pitfalls

- **Assuming Kafka's `poll()` fairly rotates across subscribed topics.** As §9.1 demonstrated with real, measured evidence (77,000 messages of lag on one topic, zero committed offsets on two others), this assumption is false, and the failure mode it produces — one topic silently starving another — can run for a long time before anyone notices, because the starved consumer group still looks "healthy" in the sense that it's actively polling and processing *something*.
- **Trusting a raw database column's timestamp value over a CDC envelope's documented-precision field.** This chapter's §9.3 is the second independent instance, in this same repository, of exactly the mistake Chapter 5 flagged generally — worth treating as strong evidence that this specific trap is genuinely easy to fall into, not a one-off oversight.
- **Inventing or guessing field names for a downstream schema (index mapping, dashboard query, or otherwise) instead of verifying them against the real, live upstream schema.** §9.4's insider-threat cardinality-aggregation example shows precisely how expensive this mistake can be — not a crash, but a silently wrong analytical conclusion.
- **Forgetting that "at-least-once" delivery implies "your write needs to tolerate being repeated."** A sink that isn't explicitly designed with this in mind (either via idempotent writes, deterministic document IDs, or an application-level dedup step) will accumulate duplicate data over time from ordinary, expected consumer restarts — not from any unusual failure.
- **Enabling synchronous, per-document index refreshes without load-testing the consequence.** §9.5's `refresh=True` removal is a specific, measured instance of a common OpenSearch/Elasticsearch performance trap: `refresh=True` (or `?refresh=wait_for`) feels like the "safe," immediately-consistent choice, but it can silently degrade indexing throughput enough to cause much less obvious downstream failures (here, Kafka consumer group rebalancing) that look, at first glance, completely unrelated to the actual root cause.

### Summary

The OpenSearch sink is where this platform's Kafka-native event streams become something a human analyst can actually search, filter, and visualize — implementing the CQRS-at-infrastructure-level pattern of separating write-optimized systems from read-optimized ones. This chapter's real value, though, lies in three separate, independently-discovered and independently-fixed bugs documented directly in this sink's own code: a Kafka poll-loop starvation incident diagnosed with real consumer-group tooling, a second, independent instance of the CDC timestamp-precision trap first introduced in Chapter 5, and a set of index-mapping corrections that demonstrate concretely how a silently-wrong field name can corrupt an analyst's conclusion without ever raising an exception. Each of these is worth remembering well beyond this specific repository — they're the kind of lesson that's far more durable, and far more transferable to your own systems, than any individual line of code. With data now flowing all the way from PostgreSQL through Kafka and Flink into a searchable index, the next chapter turns to the system that generates all of this traffic in the first place: the simulator and its attack-scenario engine.

---

*Next: [Chapter 10 — The Simulator & Attack Scenario Engine](10-simulator-and-attack-scenarios.md)*

# Chapter 10 — The Simulator & Attack Scenario Engine

*Previous: [Chapter 9 — OpenSearch: Threat Hunting & Search](09-opensearch-threat-hunting.md) · Next: [Chapter 11 — Observability: Prometheus & Grafana](11-observability.md)*

---

### Overview

Every chapter up to this point has traced what happens *after* a piece of banking activity occurs. This chapter is about where that activity actually comes from: `simulator/`, a standalone Python service with no purpose other than generating realistic, continuous, adversarial traffic against the Core Banking API — enough of it, and varied enough, to give every downstream component in this handbook (the CDC pipeline, the Flink engine, OpenSearch) something genuinely interesting to process.

**Repository fact.** The simulator is composed of two cooperating pieces: a **baseline traffic engine** (`AsyncCustomerActor`/`AsyncEmployeeActor` in `simulator/app/actors/`, orchestrated by `simulator/app/main.py`) that continuously generates plausible, everyday banking behavior, and an **attack scenario engine** (`ScenarioManager`, `simulator/app/attacks/scenarios.py`) that deliberately injects fifteen distinct, named fraud patterns on a fixed schedule, plus an on-demand API surface (`/api/demo/scenarios/run`, Chapter 3, §3.3) for triggering any of five of them explicitly.

### Why it Exists

It's worth being direct about a fact that's easy to overlook: **a fraud-detection system with nothing to detect is untestable.** Every indicator this handbook has traced through Flink (Chapter 8) — `NEW_DEVICE`, `IMPOSSIBLE_TRAVEL`, `VELOCITY_VIOLATION`, `LARGE_TRANSFER_AMOUNT` — is a hypothesis about what fraud looks like in event data, and a hypothesis is only as good as the evidence it's been checked against. Real banks validate fraud rules against real historical fraud cases (labeled, after the fact, by investigators) — data this platform obviously doesn't have and can't ethically fabricate from real incidents. The simulator solves this by manufacturing its own ground truth: it knows, by construction, exactly which of its own traffic is "legitimate" and exactly which is "the credential-stuffing attack I just launched at 14:32:07," which means every alert the Flink engine produces in response can be checked against a known answer — did the engine catch the attack it was supposed to catch, and did it avoid alerting on the legitimate traffic surrounding it?

This makes the simulator's role in the platform considerably more important than "test data generator" suggests. It's closer to a **synthetic ground-truth oracle**, and its existence is precisely what makes claims like "this fraud engine successfully detects account takeover" checkable at all, rather than aspirational.

### Implementation in This Repository

#### 10.1 Baseline traffic: persona-driven, weighted, continuous

**Repository fact.** `simulator/app/main.py`'s `main_engine()` provisions `TARGET_NUM_CUSTOMERS = 20` `AsyncCustomerActor` instances, each assigned one of four persona types (`STUDENT`, `BUSINESS`, `PROFESSIONAL`, `RETIREE`, chosen randomly, with `RETIREE` fixed to `CA` as its home country) and two `AsyncEmployeeActor` instances (a teller and a compliance manager), then launches every actor as an independent, concurrent `asyncio.Task` running its own indefinite lifecycle loop:

```python
async def customer_lifecycle_loop(actor, client):
    while True:
        try:
            await actor.perform_action(client)
            await asyncio.sleep(random.uniform(15.0, 90.0))
        except Exception:
            await asyncio.sleep(10)
```

Each `AsyncCustomerActor` (`simulator/app/actors/customer.py`) is constructed with a fixed, persistent `device` fingerprint and `network` profile (IP, country, and — via `generate_geo_point()`, Chapter 3's supporting `generators.py` module — a jittered latitude/longitude derived deterministically from that country) generated once at actor creation and reused for the actor's entire lifetime, unless a scenario deliberately overrides it. On each cycle, the actor logs in if not already authenticated, then picks one of three actions — checking its balance, making a transfer, or adding a beneficiary — using **persona-specific probability weights**:

| Persona | Balance check | Transfer | Add beneficiary | Transfer amount range |
|---|---|---|---|---|
| `STUDENT` | 75% | 20% | 5% | $5 – $45 |
| `BUSINESS` | 30% | 55% | 15% | $500 – $8,500 |
| `PROFESSIONAL` | 45% | 45% | 10% | $40 – $600 |
| `RETIREE` | 55% | 40% | 5% | $50 – $300 |

**Architectural inference.** This weighted-persona design is doing real, load-bearing work for the fraud engine downstream, not just cosmetic realism: Chapter 8's `VELOCITY_VIOLATION` indicator fires at 4 or more transactions within a 5-minute window, and `LARGE_TRANSFER_AMOUNT` fires above $10,000. A `STUDENT` persona's transfer range ($5–$45) sits nowhere near either threshold under normal behavior, meaning the simulator's own baseline traffic is deliberately constructed to *not* trip the fraud engine's indicators on its own — which is exactly what's needed for the ground-truth-oracle property described above to hold. If baseline traffic routinely tripped fraud thresholds by accident, every alert's meaning would become ambiguous (is this a real detection, or noise from the simulator's own legitimate traffic randomly crossing a threshold?), undermining the entire premise of using this simulator to validate the engine's precision.

#### 10.2 The attack scenario catalogue

**Repository fact.** `ScenarioManager` (`simulator/app/attacks/scenarios.py`, 436 lines) defines fifteen `async` static methods, each simulating a distinct, named fraud pattern by issuing a deliberate sequence of HTTP requests against the same API every legitimate actor uses — there is no separate "attack" code path in the API itself (Chapter 3); attacks are indistinguishable from legitimate traffic at the API layer, exactly as they would be against a real bank, and detection is entirely the fraud engine's responsibility, not the API's.

| Scenario | What it does | Indicators it's designed to trip |
|---|---|---|
| `run_credential_stuffing` | 25 rapid login attempts against random `cust_gen_*` usernames, all from one attacker IP/device in `CN`, all with a deliberately wrong password | `MULTIPLE_FAILED_LOGINS` |
| `run_account_takeover` | Logs in as a victim from `RU` on a brand-new device, adds a mule beneficiary, transfers $980 to it | `NEW_DEVICE`, `NEW_COUNTRY`, possibly `IMPOSSIBLE_TRAVEL` |
| `run_wire_fraud` | Adds a "Shadow Clearance Corp" mule beneficiary and wires $25,000 to it | `LARGE_TRANSFER_AMOUNT` |
| `run_insider_threat_scraping` | A rogue employee ID views 40 random customer accounts in rapid succession | Insider-threat pattern surfaced via `employee_actions` cardinality (Chapter 9, §9.4) |
| `run_auth_abuse_campaign` | Password-spraying / MFA-fatigue style abuse across many accounts | `MULTIPLE_FAILED_LOGINS` (spread across identities) |
| `run_mule_and_structuring_network` | Constructs a fan-in/fan-out transfer network across many accounts | `VELOCITY_VIOLATION`, circular-transfer ring detection (Chapter 3, §3.3.7) |
| `run_api_abuse_and_enumeration` | High-frequency probing across API endpoints | `SUSPICIOUS_API_PATTERN` |
| `run_social_engineering_chain` | A SIM-swap-style chain: profile update, device change, then a transfer | `NEW_DEVICE`, transfer indicators |
| `run_cyber_infrastructure_attack` | Simulated DDoS-style traffic burst | `SUSPICIOUS_API_PATTERN` |
| `run_known_device_threat_attack` | Traffic from a device fingerprint deliberately reused across many distinct customer identities | Shared-infrastructure ring detection (Chapter 3, §3.3.7) |
| `run_privilege_abuse` | An employee accesses a VIP/executive account outside normal patterns | Insider-threat pattern |
| `run_malware_infected_customer` | A customer's device/browser fingerprint mutates mid-session | `NEW_DEVICE` |
| `run_money_laundering` | Structured, layered transfers designed to obscure fund origin | `VELOCITY_VIOLATION`, ring detection |
| `run_fraud_ring` | Coordinated activity across a set of colluding accounts | Ring/shared-infrastructure detection |
| `run_ransomware` | A distinct extortion-style behavioral pattern | Varies |

**Repository fact.** Every attack method wraps its HTTP calls in a bare `except httpx.RequestError: pass` — a deliberate, blanket "don't let a single failed request abort the whole scenario" tolerance, consistent with the API's own JIT-provisioning philosophy from Chapter 3: the simulator is optimized for *generating signal reliably*, not for strict error handling, because a partially-failed attack scenario that still generates most of its intended signal is more useful to this platform's goals than a scenario that aborts entirely on the first transient failure.

**Repository fact.** `run_credential_stuffing()` and `run_account_takeover()` both deliberately assign the attacker a specific foreign country (`CN` and `RU` respectively) via `generate_geo_point()` — the same geo-point generation utility used for legitimate baseline traffic (§10.1) — meaning attack traffic and legitimate traffic share the exact same code path for producing realistic-looking geographic data. This is a small but meaningful design consistency: the fraud engine has no special "this is attack-shaped data" signal to key off — it has to derive `NEW_COUNTRY`/`IMPOSSIBLE_TRAVEL` purely from the same fields, generated the same way, that legitimate traffic also produces.

#### 10.3 Two ways to trigger a scenario: scheduled and on-demand

**Repository fact.** `main_engine()`'s outer loop ticks every 5 seconds and fires different scenarios at different fixed intervals — `run_credential_stuffing()` every 4th tick (20 seconds), `run_wire_fraud()` every 12th tick (60 seconds), `run_cyber_infrastructure_attack()` every 30th tick (150 seconds), and so on through all fifteen scenarios, each `asyncio.create_task()`-launched rather than awaited, so scenarios can genuinely overlap in time rather than queuing behind each other.

**Repository fact.** Separately, `/api/demo/scenarios/run` (Chapter 3, §3.3) exposes five of these scenarios (mapped through a `ScenarioType` enum: `SHARED_INFRASTRUCTURE`, `MULE_STRUCTURING`, `KNOWN_DEVICE_THREAT`, `WIRE_FRAUD`, `SYNTHETIC_IDENTITY`) for on-demand, API-driven triggering — dispatched as a FastAPI `BackgroundTasks` job that attempts to dynamically import a `simulator.scenario_manager.ScenarioManager` class. This import path does not match `simulator.app.attacks.scenarios.ScenarioManager` — the actual, real module path for the class this chapter has been describing throughout §10.2.

**Architectural inference.** This is a genuine, observable gap rather than a stylistic quirk: `_execute_scenario_task()`'s own code explicitly wraps the import in a `try/except ImportError`, falling back to `sm = None` and logging "ScenarioManager not loaded. Mock scenario ... execution logged" if the import fails — meaning the API layer's own code was written defensively, anticipating that this cross-service import might not resolve, and handles that gracefully rather than crashing. Whether this reflects an intentional decoupling (the API and the simulator are separate containers in `docker-compose.yml`, with no shared Python path between them at runtime, so a live cross-import could never actually succeed in the deployed topology regardless of the module path being correct) or an unfinished wiring-up of on-demand scenario triggering is not fully determinable from the repository alone — but either reading leads to the same practical conclusion: **the reliable, always-functioning mechanism for generating attack traffic in this platform is the simulator's own internal scheduled loop (§10.1, §10.3's first paragraph), not the `/api/demo/scenarios/run` endpoint**, which — as currently wired — falls back to a no-op mock log line in this platform's actual deployed topology.

#### 10.4 The Dockerized runtime

**Repository fact.** `simulator/Dockerfile` is a minimal `python:3.12-slim` image running `python -m app.main` as its entrypoint, with no exposed ports — the simulator is a pure outbound client, never a server, communicating with the API exclusively via `httpx` calls to `BANKING_API_URL`/`API_URL` (both env vars checked, defaulting to `http://127.0.0.1:8000`, overridden in `docker-compose.yml`'s topology — though notably, the `simulator` service itself has no explicit entry in the root `docker-compose.yml`'s service list at all; it *does* appear, correctly wired to `API_URL: http://api:8000`, in `infra/docker-compose.yml`, the Swarm deployment file covered in Chapter 12). This is worth flagging directly: **the single-host `docker-compose.yml` this handbook has used as its primary reference throughout does not itself run the simulator as a managed service** — generating traffic against a locally-run stack requires either running `simulator/app/main.py` manually (as the Quick Start instructions in Chapter 12 describe) or deploying via the Swarm-oriented `infra/docker-compose.yml` instead.

### Runtime Behaviour

Once running (whichever way it's launched), the simulator behaves as twenty-two independent, concurrently-executing asyncio tasks (twenty customers, two employees) plus one outer scheduling loop continuously spawning attack-scenario tasks on top of them — all sharing a single `httpx.AsyncClient` configured with a generous connection pool (`max_keepalive_connections=300, max_connections=3000`) specifically sized to support this level of concurrency without connection-pool exhaustion becoming an artificial bottleneck. From the API's perspective (Chapter 3), this traffic is entirely indistinguishable in *kind* from what a real population of banking customers, staff, and attackers would produce — every request goes through the same routes, the same middleware, the same JIT-provisioning logic, exactly as if the simulator didn't exist as a special case at all. This is precisely the point: nothing downstream of the API needs to know or care that its data is synthetic, which is what makes the entire platform a legitimate architectural demonstration rather than a toy with hard-coded fake events.

### Learning Concepts

- **Synthetic ground truth as a testing strategy.** For any detection or classification system, having a data generator that *knows* the correct answer by construction is what makes precision/recall (or, as computed in Chapter 3's `AnalyticsService`, precision and false-positive rate against confirmed incidents) a checkable, meaningful metric rather than a guess.
- **Persona-based synthetic data generation.** Modeling distinct behavioral archetypes with different statistical parameters (§10.1's weighted action distributions) produces materially more realistic aggregate traffic than a single uniform random generator — a technique used well beyond fraud detection, in any domain needing synthetic but statistically plausible data (load testing, recommendation system evaluation, A/B test simulation).
- **Attacks as ordinary, well-formed requests.** A recurring and important security lesson visible throughout §10.2: none of these attack scenarios exploit an API vulnerability or malformed input — every one is a sequence of completely valid, well-formed API calls that happen to be *contextually* suspicious (wrong password repeated, unfamiliar device, unusual amount, unusual velocity). This is what real-world fraud actually looks like at the network/API layer, and it's precisely why fraud detection has to live in a behavioral, stateful analysis layer (Flink, Chapter 8) rather than in request validation (which, by definition, can only ever judge one request at a time, in isolation).
- **Defensive dynamic imports and graceful fallback.** §10.3's `try/except ImportError` pattern for a cross-service dependency that may not be resolvable at runtime is a reasonable defensive technique, though — as discussed there — it can also mask a genuine wiring gap behind a silent, easy-to-miss fallback log line, which is worth weighing when deciding whether to use this pattern versus failing loudly.

### Production Perspective

A simulator of this kind has real analogues in industry, under a few different names depending on context — **synthetic transaction monitoring**, **chaos engineering for fraud systems**, or simply **red-team/adversarial testing infrastructure** — and companies building fraud or security detection systems do genuinely invest in exactly this kind of continuous, automated adversarial traffic generation, for the same reason this platform does: a detection system that has never been tested against realistic attack traffic has an unknown, unverified detection rate. A production version of this simulator would typically add:

- **Ground-truth labeling fed directly into precision/recall dashboards**, rather than left as an implicit "I know which scenario I ran" — this platform's `AnalyticsService.compute_metrics()` (Chapter 3) already computes precision/false-positive-rate from *human-labeled* incidents (`CONFIRMED_FRAUD` vs. `FALSE_POSITIVE`), but a tighter production loop would have the simulator itself automatically tag which alerts *should* correspond to which scenario run, enabling fully automated detection-rate regression testing on every code change to the fraud engine — a meaningful step beyond what exists here today.
- **Configurable intensity and realism tuning**, so the same simulator can be dialed from "generate obvious, easy-to-catch signal for a smoke test" to "generate subtle, borderline signal specifically designed to probe the engine's precision boundary" — useful for stress-testing threshold tuning (Chapter 8's `ALERT_THRESHOLD`/`CRITICAL_THRESHOLD` config) without needing new code for each intensity level.
- **Chaos-style fault injection alongside behavioral attacks** — not just fraudulent-looking traffic, but deliberate infrastructure disruption (killing Kafka mid-stream, delaying OpenSearch responses) to validate the pipeline's resilience claims from Chapters 6 through 9, rather than only validating its detection logic.

### Common Pitfalls

- **Assuming synthetic traffic and real traffic need visibly different handling somewhere in the pipeline.** As emphasized throughout this chapter, the correct design — visible consistently across this entire platform — is the opposite: synthetic traffic should be indistinguishable from real traffic to every system except the generator itself, or the resulting detection-rate validation is meaningless.
- **Letting baseline "legitimate" traffic accidentally cross detection thresholds.** §10.1 showed this platform deliberately tuning persona transfer ranges well clear of the fraud engine's thresholds — a simulator that doesn't do this produces false positives from its own normal operation, corrupting the interpretability of every alert downstream.
- **Trusting a defensively-caught `ImportError` fallback without checking whether it's actually firing.** §10.3's silent mock-logging fallback is exactly the kind of gap that can go unnoticed for a long time — a scenario endpoint that "works" (returns `200 DISPATCHED`) while its actual execution silently no-ops is a particularly easy trap to fall into when testing an API surface only by checking HTTP status codes rather than tracing whether the intended side effect actually happened.
- **Under-provisioning HTTP connection pool limits for high-concurrency synthetic load generation.** This simulator's generous `httpx.Limits` configuration is a deliberate, necessary accommodation for running twenty-plus concurrent actors continuously — a smaller default pool size would silently serialize requests that were intended to run concurrently, changing the actual traffic pattern being generated without any obvious error.

### Summary

The simulator is the platform's synthetic ground-truth oracle: a persona-driven baseline traffic generator that deliberately stays clear of the fraud engine's thresholds, paired with a fifteen-scenario attack catalogue that deliberately trips them, all issued as ordinary, well-formed API requests indistinguishable in kind from real banking activity. This chapter also surfaced a genuine, observable gap between the simulator's reliable internal scheduling loop and its on-demand API-triggered path — worth remembering as you explore this platform hands-on, since it directly determines which mechanism you should reach for if you want to reliably generate a specific attack pattern on demand. With traffic generation now fully understood, the next chapter turns to how this platform watches *itself* — the Prometheus and Grafana observability stack that monitors the health of every component this handbook has covered so far.

---

*Next: [Chapter 11 — Observability: Prometheus & Grafana](11-observability.md)*

# Chapter 11 — Observability: Prometheus & Grafana

*Previous: [Chapter 10 — The Simulator & Attack Scenario Engine](10-simulator-and-attack-scenarios.md) · Next: [Chapter 12 — Docker Compose, Infrastructure & Deployment](12-docker-compose-and-deployment.md)*

---

### Overview

Every chapter so far has explained what this platform *does* — how a transaction becomes a CDC event, how an event becomes a fraud score, how a score becomes a searchable document. This chapter is about something different: how the platform reports on its own health while all of that is happening. **Prometheus** is a time-series metrics database that periodically *pulls* numeric measurements from every instrumented service; **Grafana** is a visualization layer that turns those time series into dashboards a human can actually read at a glance. A third small component, the **Kafka Exporter**, exists purely to translate Kafka's own internal broker state into a format Prometheus can scrape, since Kafka doesn't expose Prometheus-compatible metrics natively.

**Repository fact.** `docker-compose.yml` runs `prom/prometheus` (port `9090`), `grafana/grafana` (port `3000`), and `danielqsj/kafka-exporter` (port `9308`) as three additional containers alongside the fifteen already covered in earlier chapters, configured respectively by `prometheus/prometheus.yml` and `grafana/provisioning/`.

### Why it Exists

Every other chapter in this handbook has, at some point, described a failure mode that would be invisible from the outside without deliberate instrumentation: Chapter 5's replication-slot lag, which grows silently until it either resolves or eventually threatens disk space; Chapter 6's per-partition consumer lag, which is exactly what the OpenSearch sink's own historical starvation incident (Chapter 9, §9.1) was diagnosed through, using `kafka-consumer-groups --describe`; Chapter 8's Flink checkpoint duration, which — left unmonitored — could silently grow past the 15-second checkpoint interval itself, a classic precursor to a job falling permanently behind its input. None of these conditions announce themselves with an exception or a crash. They are all **gradual degradations**, visible only as a trend in a number over time — which is precisely the class of problem a pull-based metrics system with a time-series query language and dashboarding is built to surface, and precisely the class of problem that's invisible to a system that only logs discrete events.

This is worth stating as a general principle, not just a fact about this repository: **logs tell you what happened at a specific instant; metrics tell you how a system is trending.** A production system needs both, and this platform — appropriately for its scope — implements the metrics half deliberately, while relying on Docker's own container logs for the discrete-event half.

### Implementation in This Repository

#### 11.1 What gets scraped, and what doesn't

**Repository fact.** `prometheus/prometheus.yml` defines exactly three scrape jobs, each polling its target every 15 seconds (the global `scrape_interval`):

```yaml
scrape_configs:
  - job_name: 'banking-api'
    metrics_path: '/metrics'
    static_configs:
      - targets: ['bank_api:8000']

  - job_name: 'flink-jobmanager'
    static_configs:
      - targets: ['bank_flink_jobmanager:9249']

  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['bank_kafka_exporter:9308']
```

**Architectural inference.** This list is worth reading as carefully as Chapter 6's topic-provisioning list, because — in exactly the same spirit as that earlier "provisioned but not necessarily live" distinction — the set of things *configured to be scraped* is not the same as the set of things this platform's earlier chapters actually depend on for correctness. Three components covered extensively elsewhere in this handbook are **not** scraped at all: PostgreSQL (no `postgres_exporter` is configured, meaning replication-slot lag — the exact operational signal Chapter 4's Production Perspective flagged as critical to monitor — has no Prometheus visibility in this deployment), OpenSearch (no metrics endpoint scraped, despite OpenSearch's `9600` performance-analyzer port being explicitly exposed in `docker-compose.yml`'s port mapping for exactly this purpose), and Kafka Connect itself (no scrape target for the Debezium connector's own task-level metrics, which would surface connector-specific lag distinct from what the Kafka Exporter reports about the broker side). This isn't a defect so much as a natural boundary of a homelab-scale observability build-out: the three services that *are* scraped — the API, Flink's JobManager, and Kafka via the exporter — are the three most central, highest-value components to have visibility into first, and the Production Perspective section below discusses what a more complete rollout would add.

#### 11.2 The FastAPI metrics endpoint

**Repository fact.** `bank_api:8000/metrics` is scraped directly — meaning the API itself, not a separate exporter sidecar, exposes Prometheus's native text exposition format. This is `prometheus-fastapi-instrumentator` (or an equivalent hand-instrumented middleware — the exact library is an implementation detail; what matters architecturally is the pattern) registered in `api/app/main.py` alongside `KafkaRequestLoggerMiddleware` (Chapter 3, §3.3.6), wrapping every request with counters and histograms:

- **`banking_api_requests_total`** — a `Counter`, labeled by HTTP method, endpoint path, and status code. A `Counter` in Prometheus's data model is a value that only ever increases (until the process restarts, at which point it resets to zero — a detail Prometheus's query functions like `rate()` and `increase()` are specifically designed to handle correctly across resets).
- **`banking_api_request_duration_seconds`** — a `Histogram`, measuring how long each request took to complete, bucketed into predefined latency ranges.

**Architectural inference.** It's worth being precise about why a `Histogram`, rather than something simpler like a running average, is the correct instrument for latency. A running average of request duration actively hides the exact information an operator most needs during an incident: it can look perfectly healthy (a low, stable average) even while a meaningful fraction of requests are taking far longer than acceptable, if enough fast requests dilute the average. A `Histogram` instead buckets observed durations into ranges (for example, "how many requests completed in under 100ms, under 250ms, under 1s...") and lets Prometheus's query language compute **percentiles** after the fact — a `p95` or `p99` latency, meaning "95% (or 99%) of requests completed faster than this value" — which is what actually answers the operationally important question ("how bad is the worst-case experience for a meaningful slice of traffic"), not "what's the average." This same `banking_api_request_duration_seconds` histogram is exactly what the platform's own README references when it describes Grafana tracking "API latency percentiles (p95, p99)" (§11.4 below) — a concrete link between an instrumentation choice made in application code and a specific class of query it enables downstream.

#### 11.3 Kafka Exporter: bridging a system with no native Prometheus support

**Repository fact.** Kafka itself exposes metrics via JMX (Java Management Extensions) — the standard Java platform mechanism for exposing runtime metrics and management operations — not via a Prometheus-compatible HTTP endpoint natively. `kafka-exporter` (`bank_kafka_exporter`, port `9308`) is a small, dedicated Go binary whose entire job is connecting to the Kafka broker as an ordinary client, querying broker, topic, and consumer-group metadata through Kafka's own client protocol (not JMX, in this exporter's specific implementation — it uses the Kafka wire protocol directly, via the `sarama` client library, to enumerate topics, partition counts, log-end-offsets, and every consumer group's committed offsets), and re-exposing all of it in Prometheus's text format.

This is a specific, common instance of a broader and very useful pattern worth naming: the **exporter pattern**, where a piece of infrastructure that predates or simply doesn't natively support Prometheus's pull-based, text-format scraping convention gets a small, purpose-built adapter process sitting in front of it, translating that system's own native metrics interface (JMX, in Kafka's case; a database's own `SHOW STATUS`-style command, for many database exporters) into the format Prometheus expects. This is exactly the same architectural shape as the OpenSearch sink from Chapter 9 — a dedicated bridge process translating one system's native interface into another system's expected format — applied here to metrics rather than to event data.

**Repository fact.** The specific metrics this exporter surfaces are what make **consumer lag** — the single most operationally important Kafka health signal, and the exact metric Chapter 9, §9.1's starvation incident was diagnosed with, manually, via the `kafka-consumer-groups --describe` CLI — available continuously, as a time series, rather than only as a point-in-time snapshot an operator has to remember to check manually. `kafka_consumergroup_lag`, computed as the difference between a partition's log-end-offset and a consumer group's last committed offset for that partition, is exactly the number that would have caught the OpenSearch sink's starvation bug automatically, as a graphable trend crossing a concerning threshold, rather than requiring a human to notice something was wrong and go run a diagnostic command by hand.

#### 11.4 Grafana: provisioning without dashboards

**Repository fact.** `grafana/provisioning/datasources/ds-prometheus.yml` provisions Prometheus as Grafana's default data source automatically at container startup — a genuinely useful pattern worth naming explicitly: Grafana's provisioning system lets data sources (and, normally, dashboards) be defined as version-controlled YAML/JSON files that Grafana reads and applies on boot, rather than requiring a human to click through Grafana's UI to configure a data source connection by hand after every fresh deployment. `grafana/provisioning/dashboards/dashboards.yml` similarly configures a dashboard *provider* — pointing Grafana at a folder (`/var/lib/grafana/dashboards`) it should watch and auto-load any dashboard JSON files it finds there.

**Architectural inference.** This is a real, visible gap worth being direct about, in the same spirit as this handbook's treatment of the unused Kafka topics (Chapter 6, §6.2) and the mismatched scenario-import path (Chapter 10, §10.3): the dashboard *provider* is configured, but no dashboard JSON files actually exist anywhere in this repository under that watched path. This platform's own README references specific dashboard content — "HTTP API request rates," "API latency percentiles (p95, p99)," "Kafka ingestion rates," "Debezium CDC lag," "Fraud alerts triggered per minute by severity" — describing dashboards that are architecturally fully supported by every metric this chapter has traced (the API's own histogram and counter, the Kafka Exporter's lag metrics), but that don't yet exist as committed, provisioned artifacts. This is a good example of the difference between an observability *stack* being correctly wired (data sources connected, metrics genuinely flowing, the query language available to build any of those described panels on demand) and an observability *practice* being complete (curated, saved, version-controlled dashboards that don't need to be rebuilt from scratch by whoever happens to open Grafana next) — the former is fully in place here; the latter is left as an exercise for whoever operates this platform hands-on.

### Runtime Behaviour

Once running, Prometheus's scrape loop ticks every 15 seconds, independently pulling from each of the three configured targets — meaning Prometheus is always the initiator of the connection (a **pull** model), in contrast to how every other data path in this handbook works, where a producer pushes data toward a consumer (Kafka producers pushing to brokers, the OpenSearch sink pushing documents into an index). This distinction matters operationally: a target that's completely down doesn't cause Prometheus itself to fail or block — it simply fails to scrape that target on that cycle, which Prometheus surfaces as its own `up{job="..."}` metric dropping to `0`, itself a genuinely useful signal (a target being unreachable is, in a pull model, distinguishable and directly queryable, rather than silently absent the way a crashed push-based producer's data would simply stop appearing with no explicit "this stopped" signal at all).

Grafana, once its Prometheus data source is provisioned, can execute PromQL (Prometheus's query language) queries against any metric Prometheus has scraped, at any time — including metrics from services that have since been scraped hundreds of times since a dashboard might be built, since Prometheus retains a configurable history of every scraped value as a genuine time series, not just the latest snapshot.

### Learning Concepts

- **Pull-based vs. push-based metrics collection.** Prometheus's defining architectural choice — the monitoring system initiates every scrape, rather than instrumented services pushing metrics outward — with real operational consequences (a down target is directly observable as a failed scrape, rather than silently absent) distinct from this platform's event-driven, push-oriented data pipeline (Kafka) covered in every earlier chapter.
- **Counters vs. Histograms (vs. Gauges).** Prometheus's core metric types each answer a different kind of question: a `Counter` (§11.2's request counter) answers "how many, cumulatively, and at what rate"; a `Histogram` answers "what's the distribution, and specifically the tail, of observed values"; a `Gauge` (not deeply used in this platform's own instrumentation, but the natural type for something like `kafka_consumergroup_lag`, which can go up or down) answers "what's the current value of something." Choosing the wrong type for a given signal (for example, using a `Gauge` for something that should be a monotonically increasing `Counter`) silently breaks the correctness of `rate()`/`increase()`-style queries built on top of it.
- **The exporter pattern.** A small, dedicated adapter process translating a system's native metrics or event interface into the format a different tool expects — the same architectural shape observed in Chapter 9's OpenSearch sink, here applied to Kafka Exporter's JMX-to-Prometheus (via direct wire-protocol polling) translation.
- **Consumer lag as a leading indicator.** The single metric this chapter returns to repeatedly, because it's the clearest possible illustration of "a number trending in the wrong direction, well before anything actually breaks" — exactly the kind of signal a metrics and dashboarding system exists to surface early, versus a log line that only appears once something has already gone wrong.
- **Provisioning-as-code for observability tooling.** Defining data sources and dashboard providers as version-controlled configuration (§11.4) rather than manual UI clicks — the same underlying philosophy as Alembic migrations (Chapter 4) or the Debezium connector's JSON definition (Chapter 5): infrastructure and configuration expressed as reviewable, repeatable artifacts rather than tribal knowledge or one-off manual setup.

### Production Perspective

- **Closing the scrape-target gap.** As identified in §11.1, a production rollout of this exact stack would add a `postgres_exporter` (surfacing replication-slot lag, connection counts, and query performance directly, rather than requiring the manual `pg_replication_slots` inspection Chapter 4 describes), scrape OpenSearch's already-exposed performance-analyzer endpoint, and add a scrape target for Kafka Connect's own REST API metrics (surfacing per-connector and per-task status and throughput, distinct from what the broker-focused Kafka Exporter reports).
- **Committing real dashboards, not just a provisioning path.** As identified in §11.4, the single most impactful low-effort addition to this exact repository would be actually exporting and committing the dashboard JSON this platform's own documentation already describes — turning a correctly-wired-but-empty observability stack into one where a fresh deployment is immediately useful to an operator without manual dashboard-building.
- **Alerting.** This chapter's entire scope has been metrics *collection and visualization* — nothing in this platform configures **Alertmanager** (Prometheus's companion component for turning a PromQL threshold breach into a notification — Slack, PagerDuty, email) or Grafana's own built-in alerting. A production deployment would define alert rules directly on the highest-value signals this chapter already surfaces — for example, `kafka_consumergroup_lag` exceeding a threshold for a sustained period, or Flink checkpoint duration approaching the checkpoint interval itself — so a human is notified proactively rather than needing to be actively watching a dashboard when a problem develops.
- **Long-term metrics retention and cardinality management.** Prometheus's default local storage is not designed for indefinite retention at high scale; production deployments running Prometheus at meaningful scale typically pair it with a long-term remote-storage backend (Thanos, Cortex, Mimir, or a managed equivalent) specifically to retain historical metrics well beyond Prometheus's own local retention window, and to manage the cardinality explosion that can occur if metric labels (like a raw customer ID, which this platform correctly avoids using as a label anywhere) are chosen carelessly.
- **The RED and USE methods.** Two widely-used, complementary frameworks for deciding *what* to instrument in the first place, both of which this platform's existing API metrics already implicitly satisfy in part: **RED** (Rate, Errors, Duration — the right lens for request-driven services like the API, and directly mapped onto `banking_api_requests_total`'s rate, its status-code-labeled error rate, and the latency histogram) and **USE** (Utilization, Saturation, Errors — the right lens for resources like Kafka brokers, PostgreSQL, or a Flink TaskManager's task slots, better suited to the infrastructure-level components this chapter's Production Perspective flags as not yet fully instrumented).

### Common Pitfalls

- **Confusing "the dashboard provider is configured" with "dashboards exist."** As §11.4 demonstrates directly, a correctly wired provisioning path with an empty target folder produces a Grafana instance that looks fully configured, and is fully *capable*, but shows nothing useful to an operator on first login — a distinction easy to overlook until someone actually opens the Grafana UI expecting to see the panels described in this platform's own documentation.
- **Using an average instead of a percentile for latency.** As discussed in §11.2, an average can look healthy while meaningfully hurting a real slice of users — one of the single most common and consequential observability mistakes, independent of this specific repository.
- **Choosing the wrong Prometheus metric type.** A `Gauge` used where a `Counter` belongs (or vice versa) silently breaks `rate()`/`increase()` semantics, producing charts that look plausible but are subtly, confidently wrong — a mistake that's easy to make and hard to notice without deliberately checking a metric's type documentation.
- **Monitoring the application layer while leaving the data layer dark.** This chapter's own scrape-target gap (§11.1) is a live, concrete instance of a very common real-world pattern: it's natural to instrument the service you're actively writing code in (the API) more thoroughly than the infrastructure you're merely operating (PostgreSQL, OpenSearch) — but the infrastructure layer is frequently where the earliest warning signs of a systemic problem (replication lag, disk pressure, index bloat) actually appear first.
- **Treating consumer lag as a single, undifferentiated number.** `kafka_consumergroup_lag` is reported per topic-partition-group, and — as Chapter 9, §9.1's real starvation incident demonstrated — the aggregate lag across a whole consumer group can look moderate even while one specific partition or topic is catastrophically behind and another is perfectly healthy; dashboards and alerts built only on a summed or averaged lag figure can hide exactly this kind of localized problem.

### Summary

Prometheus, Grafana, and the Kafka Exporter give this platform continuous, queryable visibility into the trends that matter most — request latency and error rate at the API layer, and consumer lag at the Kafka layer — using the same pull-based scraping and exporter-adapter patterns that underpin production observability practice broadly, not just in this repository. This chapter also applied the same investigative discipline used throughout this handbook to the observability stack itself, surfacing a real gap between what's *configured* (a dashboard provisioning path, a set of scrape targets) and what's *actually present or complete* (no committed dashboard JSON, no scrape coverage for PostgreSQL, OpenSearch, or Kafka Connect) — worth internalizing as a habit that generalizes well beyond this specific system: an observability stack, like any other subsystem in a distributed architecture, deserves the same "verify against what's actually there, not what's assumed to be there" scrutiny this handbook has applied everywhere else. With every layer of the running system now covered — from PostgreSQL through Kafka, Flink, and OpenSearch, to the traffic that feeds it and the metrics that monitor it — the next chapter steps back to the infrastructure and process that brings all of it up in the first place: Docker Compose's startup-ordering machinery, the Swarm-based multi-node deployment path, and the CI/CD pipeline that automates both.

---

*Next: [Chapter 12 — Docker Compose, Infrastructure & Deployment](12-docker-compose-and-deployment.md)*

# Chapter 12 — Docker Compose, Infrastructure & Deployment

*Previous: [Chapter 11 — Observability](11-observability.md) · Next: [Chapter 13 — Design Patterns & Architecture Decisions](13-design-patterns-and-architecture-decisions.md)*

---

### Overview

Every chapter so far has treated `docker-compose.yml` as a source of ground truth to cite — a healthcheck here, a `depends_on` there — without examining it as a subject in its own right. This chapter closes that gap, and extends outward to the two things that sit around it: `infra/`, a parallel, Swarm-oriented deployment definition for running this platform across multiple physical or virtual nodes, and `.github/workflows/deploy.yml`, the CI/CD pipeline that automates exactly that multi-node deployment on every push to `main`.

### Why it Exists

A distributed system with sixteen interdependent containers has a problem that a single-service application simply doesn't: **most of these containers are useless, or actively harmful, if started in the wrong order relative to their dependencies.** Kafka Connect starting before Kafka itself is reachable will fail its registration attempts; the fraud-engine's job submission (Chapter 8, §8.9) will fail outright if the Flink cluster isn't yet accepting connections; the API's startup-time schema provisioning (Chapter 3, §3.3.2) will fail if PostgreSQL isn't ready to accept connections yet. Docker Compose's `depends_on` with health-check conditions exists specifically to encode this dependency graph declaratively, so the orchestration tool — not a human running commands in the right order by hand, and not brittle in-application retry loops alone — is responsible for sequencing startup correctly.

### Implementation in This Repository

#### 12.1 The dependency graph, as `depends_on` actually encodes it

**Repository fact.** Compose's `depends_on` supports three distinct condition types, and this file uses all three deliberately, not interchangeably:

- **`condition: service_healthy`** — wait until the target's own `healthcheck:` block reports healthy, not merely until the container process has started. This is the strongest guarantee Compose offers, and it's used for every dependency where "the process exists" is meaningfully weaker than "the process is actually ready to serve requests" — for example, `api` depends on `postgres` with `service_healthy`, not `service_started`, because a PostgreSQL container can be running for several seconds before it's actually accepting connections.
- **`condition: service_started`** — wait only until the target's container process has started, with no readiness guarantee at all. Used where a genuine health check either isn't warranted or isn't practical — for instance, a dependency on `kafka-init` (a one-shot bootstrapper with no long-running process to health-check in the conventional sense) uses `service_completed_successfully` instead (below), while looser dependencies elsewhere in the graph accept `service_started` as sufficient.
- **`condition: service_completed_successfully`** — wait until the target container has *exited with code 0*. This is the condition used for every one-shot init container this handbook has already encountered — `kafka-init` (Chapter 6), `connect-init` (Chapter 5), and `fraud-engine`'s job-submission step (Chapter 8) — and it's the mechanism that makes the "exited containers are successes, not failures" pattern actually enforceable at the orchestration level, not just documented in a comment: any service depending on `kafka-init` with this condition will not even attempt to start until `kafka-init` has genuinely finished successfully, full stop.

Consolidating every `depends_on` block in `docker-compose.yml` into a single ordering diagram:

```text
                              postgres (healthy)
                                    │
                    ┌───────────────┼────────────────┐
                    ▼                                  ▼
                  api                           kafka (healthy)
                                                      │
                              ┌────────────────────────┼──────────────────────┐
                              ▼                          ▼                      ▼
                        schema-registry            kafka-init              (kafka itself)
                              │                    (completed)
                              ▼                          │
                        connect (healthy) ◄───────────────┘
                              │
                              ▼
                        connect-init (completed)


      kafka (healthy) + schema-registry (healthy) + opensearch (healthy) + kafka-init (completed)
                                    │
                                    ▼
                           flink-jobmanager (healthy)
                                    │
                                    ▼
                           flink-taskmanager (healthy)
                                    │
                                    ▼
                              fraud-engine (completed)


                        kafka (healthy) + schema-registry (healthy)
                                    │
                                    ▼
                              opensearch-sink


                              opensearch (healthy)
                                    │
                                    ▼
                        opensearch-dashboards
```

**Architectural inference.** The most operationally significant edge in this whole graph is `flink-jobmanager`'s dependency on `opensearch` being *healthy* — not merely started — before the JobManager itself is considered ready to accept job submissions from `fraud-engine`. This is a direct, structural consequence of Chapter 8, §8.5's async OpenSearch lookup being compiled directly into the job's operator graph: even though that lookup doesn't block per-event processing at runtime (it's genuinely asynchronous), the job still needs OpenSearch's endpoint to exist and be reachable at the moment `RichAsyncFunction.open()` initializes its HTTP client. A weaker `service_started` condition here would create a real, if narrow, race window where the Flink cluster reports healthy and `fraud-engine` successfully submits the job, but the job's first several async lookups fail with connection errors until OpenSearch genuinely finishes its own (comparatively slow, per §12.2 below) startup sequence.

#### 12.2 Long `start_period` values as a direct response to a real resource constraint

**Repository fact.** Several services carry unusually generous healthcheck `start_period` values — `kafka` at `300s`, `schema-registry` at `300s`, `opensearch` at `300s` — meaning Compose will not count a failed healthcheck against a service's `retries` budget at all until five full minutes have elapsed since that container started, regardless of how quickly the underlying image would ordinarily become healthy on more generously-resourced hardware.

**Architectural inference.** This is best read as a direct, deliberate accommodation for exactly the kind of constrained development environment this platform was actually built and iterated on — a single physical host running sixteen containers, several of them JVM-based (Kafka, Schema Registry, Kafka Connect, both Flink components) and therefore each independently paying the JVM's own startup and JIT-warmup cost, competing for the same limited CPU and memory simultaneously during the platform's cold-start window. A `start_period` this generous is the correct tool for exactly this situation: it isn't relaxing the *steady-state* health requirement (once past the grace period, the same `interval`/`retries`/`timeout` cadence applies as it would with a shorter `start_period`) — it's specifically preventing Compose from prematurely declaring a slow-but-genuinely-fine cold start a failure, which would otherwise cascade into every downstream `service_healthy` dependency failing to proceed and the entire stack's bring-up stalling out on a false negative. Genuinely undersized or crash-looping services would still fail their healthchecks well past this grace period and correctly surface as unhealthy; this setting only buys patience for a slow-but-honest startup, not tolerance for an actually-broken one.

The three resource-limited services visible in `docker-compose.yml`'s `deploy.resources.limits` blocks (`cpus: '0.25'`, `'0.20'`, `'0.10'` on three of the lighter-weight services) point at the same underlying constraint from a different angle: on a host without abundant spare CPU, deliberately capping a handful of less latency-sensitive services' CPU shares is a reasonable way to leave more headroom for the JVM-heavy services that need it most during that same cold-start window — a small, concrete illustration of resource contention being a real, felt constraint during this platform's development, not merely a theoretical concern raised in retrospect.

#### 12.3 `check_port_8000.ps1` and `kill_orphans.ps1`: a documented host-level failure mode

**Repository fact.** These two PowerShell scripts, sitting at the repository root rather than inside any service's own directory, exist to diagnose and remediate a specific, previously-real incident. `check_port_8000.ps1`'s own header comment describes it precisely:

> a leftover host-native `uvicorn --reload` process binding specifically to `127.0.0.1:8000`, which Windows prefers over Docker's `0.0.0.0:8000` wildcard listener for any client connecting via the literal address "127.0.0.1" — which is exactly what the simulator does by default. When this happens, ALL simulator traffic silently lands on the host process instead of `bank_api`, and the container logs stay completely empty with no error anywhere.

This is a genuinely instructive, non-obvious networking failure mode worth understanding on its own terms, independent of this specific repository: on Windows, when two separate listeners are bound to the same port — one specifically to the loopback address `127.0.0.1`, another to the wildcard address `0.0.0.0` (which is what Docker's port-forwarding sets up for a mapped container port) — the operating system's socket-binding rules will route a client's connection to `127.0.0.1:<port>` toward the more *specific* listener (the literal loopback binding) rather than the wildcard one, even though both are technically listening on the same port. A developer running the FastAPI application directly on the host (`uvicorn app.main:app --reload`, for local iteration outside Docker — a genuinely reasonable workflow) and then separately starting Docker Compose's own `bank_api` container, without first stopping the host process, ends up with exactly this silent traffic-hijacking scenario: the containerized API looks completely healthy (nothing about it is actually broken), the simulator reports successful HTTP responses (because *something* answered on port 8000 — just the wrong something), and yet `api_requests` sits at zero messages, because every request the simulator sent was quietly served by the host-native process instead, which has no Kafka telemetry middleware wired into a locally-invoked `uvicorn` session running outside the Compose network. `check_port_8000.ps1` diagnoses this precisely by checking whether a `127.0.0.1`-specific listener exists on port 8000 and identifying its owning process by PID and command line; `kill_orphans.ps1` is the accompanying remediation, terminating stray `python.exe` processes on the host (with an explicit allowlist for PIDs the operator wants preserved — e.g., an intentionally-running simulator instance) so a fresh `docker compose up` starts from a clean slate.

**Architectural inference.** This pair of scripts is worth treating as a genuine case study in operational tooling born directly from an actual incident, in the same spirit as Chapter 9's documented consumer-starvation postmortem: the fix isn't a code change to the API or the simulator at all (neither is "wrong" in any way this failure mode would suggest) — it's a small, purpose-built diagnostic utility that encodes exactly what was learned about a specific, non-obvious, platform-specific (Windows socket-binding behavior) failure mode, so the next time it happens, it's a five-second script run instead of a from-scratch debugging session starting from "why is `api_requests` stuck at zero even though the simulator reports success."

#### 12.4 `infra/`: the Swarm-oriented multi-node deployment

**Repository fact.** `infra/docker-compose.yml` is a second, independent Compose file — not a variant loaded alongside the root file via Compose's override mechanism, but a self-contained deployment definition meant to be used with `docker stack deploy`, which interprets a small but meaningful superset of ordinary Compose syntax specific to **Docker Swarm mode**. It correctly includes the `simulator` service (absent from the root `docker-compose.yml`, per Chapter 10, §10.4) with `API_URL: http://api:8000` wired in explicitly, and defines `deploy.placement.constraints` pinning specific services to specific named Swarm nodes:

```text
k3s-master:      PostgreSQL, Core Banking API, Attack Simulator, Kafka Connect
k3s-worker-2:    Apache Kafka (KRaft broker), Schema Registry
k3s-worker-3:    OpenSearch cluster, Flink JobManager & TaskManager
```

**Architectural inference.** Despite the `k3s-*` node naming (a naming convention associated with Kubernetes, specifically k3s, a lightweight Kubernetes distribution), this deployment definition is genuine Docker Swarm syntax (`deploy.placement.constraints` with `node.hostname==...`, `docker stack deploy`), not a Kubernetes manifest — the node names most plausibly reflect the underlying physical or virtual host naming convention of a homelab cluster that may have been used for, or originally provisioned with, Kubernetes/k3s in mind, repurposed here for Swarm. This placement strategy itself reflects a deliberate, sensible resource-distribution decision distinct from the single-host topology's implicit "everything competes for the same CPU and memory" constraint discussed in §12.2: spreading the JVM-heavy services (Flink, OpenSearch on one node; Kafka and Schema Registry on another) across separate physical nodes directly addresses the exact resource-contention problem that `start_period` tuning was compensating for *within* a single host — at multi-node scale, the better fix is genuinely separating the contending workloads onto separate hardware, rather than only tuning healthcheck patience on shared hardware.

#### 12.5 Building and publishing images for a multi-node deployment

**Repository fact.** A single-host Compose deployment can build images locally, in place, because every container runs on the same machine `docker compose build` runs on. A genuinely multi-node Swarm deployment cannot rely on this — a node that isn't where `docker build` ran has no local copy of a freshly-built image. This platform's documented deployment flow (also reflected identically in the CI/CD workflow, §12.6) addresses this by building each custom image locally, tagging it for the **GitHub Container Registry (GHCR)**, authenticating, and pushing:

```bash
docker build -t banking-api:latest ./api
docker tag banking-api:latest ghcr.io/$OWNER/banking-api:latest
docker push ghcr.io/$OWNER/banking-api:latest
```

Every Swarm node then pulls the same, identically-tagged image from GHCR rather than needing a local build step of its own — the standard pattern for any multi-node container orchestration, Swarm or otherwise: a shared, centrally-addressable registry as the single source of truth for "what image, exactly, is version X of this service," rather than trusting that N separately-built local images across N nodes are actually identical.

#### 12.6 CI/CD: `.github/workflows/deploy.yml`

**Repository fact.** This GitHub Actions workflow triggers on pushes to `main` and performs, in sequence: building the API and simulator images, tagging and pushing both to GHCR, running `docker stack deploy -c infra/docker-compose.yml` against the Swarm manager (over SSH, using a repository secret holding deployment credentials), and — as its final step — registering the Debezium CDC connector by `POST`-ing `config/kafka-connect/postgres-source.json` against the newly-deployed Kafka Connect's REST API, with a retry/poll loop waiting for Kafka Connect to become reachable first.

**Architectural inference.** That final connector-registration step is worth calling out specifically, because it demonstrates a subtlety that's easy to miss when designing CI/CD for a system with CDC in it: **redeploying the stack does not automatically re-establish CDC capture on its own.** Chapter 5 explained that `connect-init` handles this registration for the single-host Compose topology, but `infra/docker-compose.yml`'s Swarm-oriented service list has no equivalent one-shot init container performing the same job for this deployment path — so the CI/CD pipeline itself takes on that responsibility explicitly, as its own dedicated pipeline step, rather than relying on an init-container pattern that doesn't exist in this specific topology. This is a legitimate, if less self-contained, way to solve the same problem: the guarantee "the CDC connector is registered after every deployment" still holds, it's just enforced by the deployment pipeline's own script rather than by an in-cluster bootstrapper container, and it's worth recognizing as a real architectural choice (external orchestration vs. self-contained init container) rather than assuming one approach is simply "more correct" than the other — each fits the deployment model it's paired with.

### Runtime Behaviour

On a single-host `docker compose up`, the dependency graph in §12.1, combined with the generous `start_period` grace windows from §12.2, produces a startup sequence that — under real, resource-constrained conditions — can genuinely take several minutes end-to-end before every service reports healthy: PostgreSQL first, Kafka and its dependents next (with Kafka itself potentially taking most of its 300-second grace window on a loaded host), OpenSearch and Schema Registry proceeding in parallel with Kafka's own startup, and only once all of *those* report healthy does the Flink cluster begin its own startup, followed finally by `fraud-engine`'s job submission — meaning the fraud-scoring pipeline described in Chapter 8 genuinely isn't live and consuming events until essentially the entire rest of the stack has already proven itself healthy first. This is a direct, observable consequence of the dependency graph traced in §12.1: Flink's position at the very end of that graph isn't incidental — it's the correct ordering, because a Flink job submitted before its five source topics and its OpenSearch dependency were reliably ready would fail or behave incorrectly.

On the Swarm path, `docker stack deploy` reads `infra/docker-compose.yml`'s placement constraints and schedules each service onto its designated node, and the CI/CD pipeline's own explicit CDC-registration step (§12.6) runs only after the deployed stack's Kafka Connect service is confirmed reachable — an equivalent, if externally-orchestrated rather than self-contained, guarantee to what `connect-init` provides on the single-host path.

### Learning Concepts

- **Declarative dependency ordering vs. hand-written startup scripts.** `depends_on` with health-check conditions lets the orchestration layer itself understand and enforce a service dependency graph, rather than requiring bespoke `wait-for-it.sh`-style scripts embedded inside each container's own entrypoint — the same broader "let the infrastructure own the concern, rather than the application" philosophy already seen in Chapter 6's `kafka-init` and Chapter 5's `connect-init`.
- **The three shades of "ready": started, healthy, and completed.** A genuinely useful distinction for anyone designing multi-service orchestration — process existence, application-level readiness, and one-shot task completion are three different guarantees, and conflating them (using `service_started` where `service_healthy` was actually needed) is a common, subtle source of race-condition startup failures.
- **Healthcheck grace periods as a deliberate tuning knob, not a default to leave untouched.** §12.2's generous `start_period` values are a direct, evidence-grounded response to a real resource constraint, illustrating that healthcheck configuration is itself an architectural decision worth making deliberately, tied to the actual hardware and workload a system runs on, rather than left at whatever an image's documentation happens to suggest.
- **Host-level networking edge cases that have nothing to do with application code.** §12.3's port-8000-squatting incident is a clean illustration of a failure mode entirely outside the application layer — no amount of debugging the FastAPI application or the simulator's HTTP client would ever have surfaced this, because both were behaving completely correctly; the bug lived entirely in how the host operating system routes connections between two coexisting listeners.
- **A shared image registry as the backbone of any multi-node deployment.** §12.5's GHCR build-tag-push flow is the standard solution to a problem every multi-node containerized deployment eventually has to solve — one build, one canonical artifact, many consuming nodes — regardless of whether the orchestrator is Swarm, Kubernetes, or something else entirely.

### Production Perspective

- **Infrastructure as Code beyond Compose/Swarm.** This platform's `infra/docker-compose.yml` is a legitimate, working multi-node deployment definition, but production teams operating at real scale most commonly reach for Kubernetes (given the `k3s`-suggestive node naming already discussed in §12.4, an eventual migration in that direction would be a very natural next step for this exact platform) or a managed container orchestration service, precisely because Kubernetes's ecosystem offers considerably more mature primitives for exactly the kinds of problems this chapter has surfaced — native StatefulSets for ordered, stable-identity deployment of stateful services like Kafka and PostgreSQL, richer readiness/liveness probe semantics than Compose's three-condition model, and a much larger ecosystem of Kafka/Flink/OpenSearch-specific operators that encode much of this chapter's dependency-ordering and resource-tuning knowledge as reusable, purpose-built controllers rather than hand-maintained YAML.
- **Immutable infrastructure and golden images.** This platform's `start_period` tuning (§12.2) is a reasonable adaptation to a specific, known hardware constraint; a production deployment running on adequately-provisioned, dedicated hardware for each service (rather than sixteen containers time-sharing one host) would likely need far less aggressive grace-period tuning in the first place, since the underlying resource contention driving the need for it would no longer exist.
- **Blue/green or rolling deployment strategies.** `docker stack deploy` on an already-running Swarm stack performs a rolling update by default, but nothing in this platform's CI/CD pipeline (§12.6) currently configures deployment-order guarantees, health-check-gated rollout pacing, or automatic rollback on a failed health check during a deploy — all standard, expected additions for a production release pipeline handling a system with real user traffic, where a bad deploy needs to be caught and reverted automatically, not manually.
- **Secrets management in CI/CD.** The CI/CD pipeline's SSH-based deployment credential is stored as a GitHub Actions repository secret — a reasonable baseline, but production pipelines handling genuinely sensitive infrastructure typically integrate a dedicated secrets manager (matching the same recommendation already made for the Debezium connector's database credentials in Chapter 5) with short-lived, automatically-rotated credentials rather than a long-lived static secret checked once into a CI platform's own secret store.

### Common Pitfalls

- **Using `service_started` where `service_healthy` was actually needed.** As discussed in §12.1, this is the single most consequential `depends_on` mistake to make in a system like this one — it looks correct (the dependency graph appears fully specified) while leaving a real race-condition window open, exactly the kind of bug that only manifests intermittently, under specific timing conditions, making it disproportionately hard to reproduce and diagnose after the fact.
- **Copying a `start_period` value from one environment to another without reconsidering it.** A 300-second grace period tuned for a specific, resource-constrained development host may be needlessly slow (delaying legitimate failure detection) on better-provisioned hardware, or still insufficient on even more constrained hardware — this value is a function of the actual deployment environment, not a universal constant to carry forward unexamined.
- **Running a service both inside and outside its intended container topology simultaneously.** §12.3's port-8000 incident is exactly this mistake, and it's a genuinely easy one to make during active development, where running a service natively for fast iteration (skipping a container rebuild) is a completely reasonable workflow choice — the failure only appears when that native process isn't cleaned up before also starting the containerized version.
- **Assuming a redeployed stack automatically restores every stateful integration.** §12.6 showed this platform's own CI/CD pipeline explicitly re-registering the CDC connector after every deployment, precisely because that registration doesn't happen automatically on the Swarm path — a system with any external, stateful registration step (a CDC connector, a webhook subscription, a scheduled job registration) needs an explicit, deliberate answer to "does this survive a redeploy, and if not, whose job is it to restore it?"
- **Treating a multi-node deployment file as a drop-in replacement for the single-host one, without re-verifying its service list.** As Chapter 10, §10.4 already flagged, the `simulator` service exists in `infra/docker-compose.yml` but not the root `docker-compose.yml` — a genuine, meaningful difference between the two files that's easy to miss if you assume they're simply two flavors of the same deployment rather than two independently-maintained definitions.

### Summary

Docker Compose's `depends_on` health-check conditions encode this platform's real dependency graph as enforceable orchestration logic rather than tribal startup-order knowledge, and this chapter traced that graph in full, alongside two genuinely instructive pieces of operational history: the generous `start_period` tuning that's a direct, evidence-grounded response to real hardware resource contention, and the port-8000-squatting incident that's a clean demonstration of a failure mode living entirely outside the application layer, in host-level network routing behavior. The Swarm-oriented `infra/` deployment and its accompanying CI/CD pipeline extend the same architecture across multiple physical nodes, solving the same "what needs to happen, in what order, and whose responsibility is it" questions this chapter opened with, adapted to a topology where images have to be built once and shared via a registry rather than built locally on every node. With the platform's runtime architecture, data flow, and deployment machinery now all fully covered, the next chapter steps back from any single component to look across the whole codebase at once — the recurring design patterns, and the handful of places where two different patterns compete for the same job, that this handbook has been naming individually, chapter by chapter, all along.

---

*Next: [Chapter 13 — Design Patterns & Architecture Decisions](13-design-patterns-and-architecture-decisions.md)*

# Chapter 13 — Design Patterns & Architecture Decisions

*Previous: [Chapter 12 — Docker Compose, Infrastructure & Deployment](12-docker-compose-and-deployment.md) · Next: [Chapter 14 — Appendix](14-appendix.md)*

---

### Overview

Every previous chapter examined one component in depth and named the patterns local to it as they came up — the Repository-ish services in Chapter 3, broadcast state in Chapter 8, the exporter pattern in Chapters 9 and 11. This chapter does something different: it steps back across the *entire* codebase and looks for the patterns that recur in more than one place, independently, in different languages, written at different times — because a pattern an engineer reaches for once might be incidental, but a pattern that shows up three or four times, unprompted, across a system's history is evidence of something more durable: a real, load-bearing engineering instinct the platform's builders kept returning to.

### Why it Exists

It would be possible to document this repository purely file by file, and much of this handbook has done exactly that. But a *handbook*, as distinct from a reference manual, exists to teach transferable judgment — and the clearest way to teach judgment is to show the same underlying decision being made correctly, independently, more than once, in different contexts. This chapter is where those threads, pulled individually throughout Chapters 1–12, are tied together.

### Implementation in This Repository

#### 13.1 Fail-open, everywhere, deliberately

**Repository fact.** This is the single most pervasive pattern in the entire codebase, and it's worth listing its instances side by side to see how consistently it's applied:

| Component | What "failure" looks like | What happens instead of an error |
|---|---|---|
| `kafka_producer.py` (Ch. 3, §3.3.6) | Avro serialization fails | Falls back to a JSON payload on `dead_letter_queue` |
| `transfers.py` (Ch. 3, §3.3.4) | Referenced account doesn't exist | JIT-creates it |
| `transfers.py` (Ch. 3, §3.3.4) | Source balance about to go negative | Auto-replenishes it |
| `accounts.py`/`cards.py` (Ch. 3, §3.3.4) | No matching row in the database | Returns a fabricated, plausible mock response |
| `OpenSearchLookupFunction` (Ch. 8, §8.5) | The async lookup errors or times out | Sets the result field to `null` (unknown, not false) and lets the event continue |
| `AccountEnrichmentFunction` (Ch. 8, §8.4) | The broadcast join hasn't resolved a customer ID yet | Keeps the event under a fallback `acct:` identity rather than dropping it |
| `opensearch_sink.py` (Ch. 9, §9.5) | An index write fails | Logs, doesn't commit the offset, lets Kafka redeliver it |
| `ScenarioManager` (Ch. 10, §10.2) | An individual attack-scenario HTTP call fails | Swallows it and continues the rest of the scenario |
| `_execute_scenario_task()` (Ch. 10, §10.3) | The `ScenarioManager` import fails | Logs a mock-execution line instead of raising |

**Architectural inference.** Naming this pattern once, at the system level, makes something visible that's easy to miss when each instance is encountered separately: this isn't nine unrelated defensive-coding habits — it's one consistent philosophy, applied with real judgment about *where* it belongs. Every single instance above sits on a path where the priority is **generating continuous, realistic signal for a downstream fraud-detection pipeline to consume** — and where a hard failure would break that continuity for no corresponding safety benefit, because there's no real money and no real customer at risk. This is precisely why Chapter 3's Production Perspective was careful to flag JIT provisioning as the *one* pattern in this list that would need to be inverted, not merely hardened, before any part of this codebase touched real funds — fail-open is the right default for a synthetic-data-generation platform and the wrong default for a system of record, and the value of naming the pattern explicitly is that it makes exactly that boundary easier to reason about deliberately, rather than having to rediscover it independently for each of these nine call sites.

#### 13.2 Idempotency via deterministic, content-derived identity

**Repository fact.** Three independent mechanisms across this codebase solve "how do I safely process the same logical thing twice" by deriving an identifier from the content itself, rather than from a random UUID or an auto-incrementing counter:

- `RiskScoringProcessor.buildDeterministicAlertId()` (Ch. 8, §8.7) — `identity + severity + cooldown-window` → the same alert, recomputed after a checkpoint restore, produces the same ID and is silently skipped by the `emittedAlertIds` guard.
- `rules_engine.py`'s "penalty box" and the Java engine's `COOLDOWN_MS` (Ch. 7, §7.1; Ch. 8, §8.7) — not identity-based deduplication exactly, but the same underlying goal: suppress re-emission of a signal about a condition that's already known and already alerted on.
- `alert_aggregator.py`'s incident-folding window (Ch. 2, §2.5) — a *different* identifier (the `identity_key` plus a rolling time window) used to decide whether an incoming alert belongs to an *existing* open incident, rather than always minting a new one.

**Architectural inference.** These three mechanisms operate at three different layers of the same pipeline (Flink's own internal alert emission, a legacy prototype's request-level suppression, and the incident-management layer consuming Flink's output) and were almost certainly designed independently, at different points in the platform's history — yet they converge on the same underlying technique. This is worth generalizing beyond this repository: **in any event-driven system with at-least-once delivery semantics (which Kafka provides by default, and which this entire platform relies on throughout), "might this message get redelivered or reprocessed?" is never a hypothetical question — it's a certainty over a long enough time horizon**, and deriving identity from content rather than from a fresh, one-time-only value is the standard, low-overhead way to make reprocessing safe without needing an external, stateful deduplication service.

#### 13.3 The one-shot init container

**Repository fact.** `kafka-init` (Ch. 6), `connect-init` (Ch. 5), and `fraud-engine`'s job-submission role (Ch. 8, §8.9) are all structurally identical: a container whose entire purpose is to perform one idempotent setup action against an already-running dependency, then exit with code `0` — a *success*, verified explicitly via Compose's `service_completed_successfully` condition (Ch. 12, §12.1), not a crash to be alarmed about.

**Architectural inference.** This pattern is doing something subtler than "run a setup script" — it's cleanly separating **the lifecycle of a piece of configuration** (a set of Kafka topics; a registered Debezium connector; a submitted, running Flink job) from **the lifecycle of the container that establishes it**. None of these three things need a long-running process babysitting them once they exist — Kafka topics persist on the broker regardless of whether `kafka-init` is still running; a Debezium connector, once registered via Kafka Connect's REST API, is owned and run by the `connect` service itself; a submitted Flink job, per the `-d` detached flag discussed in Chapter 8, §8.9, runs entirely independently of the container that submitted it. Recognizing this pattern explicitly is useful well beyond this repository: any time a piece of infrastructure needs a one-time, idempotent bootstrapping action against something that will itself outlive the actor performing that action, a one-shot init container (rather than folding that logic into another service's own startup script, or requiring a human to run it manually) is very often the cleanest place to put it.

#### 13.4 The exporter/sink/bridge pattern

**Repository fact.** Three components across this platform share the same shape: a small, dedicated process whose only job is translating one system's native interface into a format a different system expects, sitting entirely outside the systems on either side of it.

| Bridge | Translates from | Translates to |
|---|---|---|
| `opensearch_sink.py` (Ch. 9) | Kafka's consumer protocol | OpenSearch's document index API |
| Kafka Exporter (Ch. 11, §11.3) | Kafka's own client/wire protocol metadata | Prometheus's text exposition format |
| `alert_aggregator.py` (Ch. 2, §2.5) | Kafka alert messages | PostgreSQL incident rows |

**Architectural inference.** What unifies these three, despite operating on completely different data (search documents, metrics, database rows), is a shared design commitment: **neither system on either side of the bridge needs to know the other exists.** Kafka has no concept of OpenSearch, Prometheus, or PostgreSQL's `incidents` table; OpenSearch, Prometheus, and PostgreSQL each have no concept of Kafka. The bridge process owns the entire translation, in both directions of vocabulary (what a Kafka offset means to a consumer; what a document ID or a metric name means to its target system), which is precisely what makes it possible to add, remove, or reimplement any one of these three integrations without touching either endpoint — a direct, practical benefit of the fan-out, decoupled architecture Chapter 2 introduced at the very start of this handbook, now visible as a concrete, recurring implementation shape rather than only as an abstract diagram.

#### 13.5 Schema verification as a recurring discipline, not a one-time check

**Repository fact.** This is less a single design pattern than a documented *practice*, visible directly in code comments across multiple, independently-written components: `RiskEventNormalizer.java`'s Javadoc explicitly cross-references the real `models.py` (Ch. 8, §8.3); `opensearch_sink.py`'s index-mapping corrections explicitly cross-reference the real running schema (Ch. 9, §9.4); this handbook's own Chapter 4 traced the same discipline applied to `schema.sql` versus the live ORM models.

**Architectural inference.** The pattern worth naming here is the *practice* of treating an inherited or assumed schema as unverified until checked against the system that's actually authoritative for it, and — just as importantly — **documenting that verification directly in the code**, as a comment a future reader (or a future version of the same engineer) can trust, rather than leaving the verification as a one-time, undocumented mental exercise that has to be redone from scratch the next time someone touches the same code. Chapter 9's insider-threat cardinality-aggregation example (§9.4) is the sharpest illustration in this entire handbook of *why* this discipline matters: a wrong field name doesn't crash anything — it silently corrupts a downstream analytical conclusion, and the only real defense against that class of failure is exactly this habit, applied consistently, everywhere data crosses a schema boundary.

#### 13.6 Where two patterns compete for the same job

Not every recurring theme in this codebase converges cleanly. Two places are worth naming explicitly as genuine, unresolved tensions, in the same spirit of honesty this handbook has applied throughout:

**Sync vs. async data access (Chapter 3, §3.3.3).** The API maintains two independent SQLAlchemy `Base` registries and two engine configurations, split roughly along "older core-banking surface" vs. "newer analytical/investigative surface." Neither approach is wrong in isolation — FastAPI legitimately supports mixing both — but the coexistence means a developer extending this codebase has to learn and correctly apply *two* data-access idioms depending on which router they're working in, rather than one. This is the clearest example in this handbook of **incremental, organic growth producing a real, ongoing maintenance cost** that a single, unified pattern chosen from the start would have avoided — worth naming not to criticize a decision made under real time constraints, but because recognizing exactly this kind of drift, early, is a skill worth deliberately practicing.

**Prototype-and-graduate vs. build-once-correctly (Chapter 7 vs. Chapter 8).** The Python stream scripts and the Java Flink engine represent two different, both-legitimate philosophies toward the same problem — validate quickly with a lightweight tool, then rebuild properly once the approach is proven, versus investing in the "right" tool from the start. This repository chose the former, and Chapter 7 discussed at length what specifically was gained by doing so (a working, demonstrable scoring concept, fast) and what specifically had to be solved on the way to a production-grade version (durable, partitioned, checkpointed state). Neither philosophy is universally correct; the right choice depends on how confident a team is, up front, in the target design, and how expensive premature commitment to the "proper" tool would be if the concept turned out to need significant rework anyway.

### Runtime Behaviour

Because this chapter is a synthesis rather than a walkthrough of a single running component, "runtime behavior" here means something slightly different than in earlier chapters: it means recognizing, while operating or extending this platform, *which* of these patterns is active at any given failure point, so that an observed symptom can be mapped back to the right mental model quickly. A silently-created account with a suspiciously round balance is fail-open JIT provisioning (§13.1), not a bug. A duplicate-looking alert that never actually appears twice is deterministic ID deduplication working correctly (§13.2), not evidence that deduplication is broken. An `Exited (0)` container is a completed one-shot init job (§13.3), not a crash. Recognizing the pattern in play is very often the fastest path to correctly interpreting what's actually happening.

### Learning Concepts

- **Recognizing patterns as system-level properties, not file-level accidents.** A pattern observed once might be a local decision; the same pattern observed independently, three or four times, across a codebase's history, is a signal about the engineering values that actually shaped the system — a genuinely useful diagnostic to apply when exploring any unfamiliar codebase, not just this one.
- **Fail-open vs. fail-closed as a deliberate, context-dependent choice**, not a universal default in either direction — the correct choice depends entirely on what's actually at stake on the failure path, which is why the same codebase can, correctly, choose differently in different places (fail-open throughout the synthetic-data pipeline; the Production Perspective sections throughout this handbook consistently flag where a real-money deployment would need to flip that choice).
- **Idempotency as a first-class design requirement in any at-least-once system**, not an edge case to handle defensively after the fact — every event-driven architecture with durable, replayable transport (Kafka, here) needs an explicit answer to "what happens if this gets processed twice," and deterministic, content-derived identity is one of the most broadly applicable answers.
- **Decoupled bridge processes as the mechanism that actually delivers on a fan-out architecture's promise.** Chapter 2's diagrams showed independent consumers reading the same topics; this chapter's §13.4 shows the concrete implementation shape (a small, dedicated translator process) that makes each of those consumers genuinely independent in practice, not just on paper.
- **Technical debt as a legible, traceable artifact, not a vague complaint.** §13.6's two examples are both fully explainable in terms of specific decisions made at specific points in this platform's history, each with real, articulable tradeoffs — a more useful and more actionable framing than simply labeling either one "debt" without explaining why it exists or what it costs.

### Production Perspective

Design-pattern consistency is, in a real sense, itself a production-readiness signal, and it's worth stating this directly as this handbook moves toward its conclusion: a codebase where the same well-reasoned pattern (fail-open error handling, deterministic idempotency, decoupled bridge processes, schema verification against ground truth) shows up repeatedly, independently, and is used *correctly* in each instance is a strong positive signal about the judgment behind it — considerably more informative than any single file examined in isolation could be. Conversely, the two genuine tensions identified in §13.6 are exactly the kind of finding a technical due-diligence review or an onboarding engineer's first few weeks on a real codebase would surface, and are worth treating as a template for that kind of review rather than as criticisms specific to this repository: **look for what recurs, and look separately for what competes**, because both carry real signal about how deliberately, versus how organically, a system actually grew.

### Common Pitfalls

- **Copying a pattern without understanding the boundary condition that makes it appropriate.** Fail-open error handling is exactly right for the JIT-provisioning paths in this codebase and exactly wrong for a real payment system — the pattern itself isn't good or bad in the abstract; its correctness is entirely a function of what's actually at stake on the failure path, and applying it reflexively, without that analysis, is a real and common mistake.
- **Treating every instance of code duplication as accidental.** The three independent idempotency mechanisms in §13.2 look, superficially, like duplicated effort — but each solves the deduplication problem at a genuinely different layer of the pipeline, with different guarantees and different failure modes, and collapsing them into one shared mechanism would likely make the system *less* robust, not more, by introducing a single point of failure for a concern that currently has defense in depth.
- **Assuming architectural consistency is free.** §13.6's sync/async split is the direct, ongoing cost of *not* unifying a pattern early — worth remembering as a counterweight to this chapter's celebration of recurring good patterns: consistency has to be actively maintained, and a codebase that grows quickly, correctly, under real constraints, will still accumulate exactly this kind of drift unless someone deliberately budgets time to prevent or correct it.
- **Mistaking "this pattern appears often" for "this pattern is beyond scrutiny."** Even a pattern used consistently and well, like fail-open error handling throughout this platform, still needs its boundary explicitly re-examined any time the system's actual stakes change — exactly the re-examination this handbook's Production Perspective sections have performed, chapter by chapter, rather than assuming a pattern's past correctness guarantees its future correctness under different conditions.

### Summary

Read individually, this repository's fail-open error handling, deterministic idempotent IDs, one-shot init containers, decoupled bridge processes, and disciplined schema verification each looked like a sensible local decision. Read together, across twelve chapters, they resolve into something more valuable: a coherent, if imperfect, engineering philosophy — favor continuity of signal over strict correctness where the stakes allow it, make reprocessing safe by construction rather than by exception handling, keep independent systems genuinely independent through dedicated bridges, and never trust an inherited schema without checking it against ground truth. The two places where that philosophy visibly fractures — the sync/async data-access split, and the prototype-vs-production duality between the Python and Java stream processors — are just as instructive as the places where it holds, because they show exactly what organic, real-world software development costs even when most individual decisions along the way were reasonable ones. The final chapter consolidates every pitfall, inconsistency, and repository-wide caveat this handbook has surfaced into a single reference, alongside pointers for continuing to study each of the underlying technologies covered here in greater depth.

---

*Next: [Chapter 14 — Appendix: Common Pitfalls, Repository Inconsistencies & Further Reading](14-appendix.md)*

# Chapter 14 — Appendix: Common Pitfalls, Repository Inconsistencies & Further Reading

*Previous: [Chapter 13 — Design Patterns & Architecture Decisions](13-design-patterns-and-architecture-decisions.md) · Back to: [Index](00-index.md)*

---

This closing chapter is a reference, not a narrative — a consolidated place to look things up after you've read (or while you're still reading) the thirteen chapters that came before it. It gathers three things: every genuine repository inconsistency this handbook identified along the way, in one place, with its source chapter; a checklist of the pitfalls most worth carrying forward into your own event-driven systems; and pointers for going deeper into each underlying technology, organized by the chapter where it was introduced.

### A.1 Consolidated list of repository inconsistencies and gaps

None of the following are treated as defects in this handbook — every one of them was explained, in its originating chapter, as a legible consequence of real, reasonable engineering decisions made under real constraints. This table exists purely so a reader extending or operating this platform hands-on has a single place to check before being surprised by one of them.

| # | Gap | Where it's explained | Practical consequence |
|---|---|---|---|
| 1 | Two independent FastAPI applications exist (`api/`, `backend/`); only `api/` is deployed | Ch. 3, §3.3.1 | Don't edit `backend/` expecting it to affect the running system |
| 2 | Two independent SQLAlchemy `Base` registries and engine configurations (sync and async) inside `api/` | Ch. 3, §3.3.3 | New routers need to deliberately choose, and correctly wire, one data-access pattern |
| 3 | Schema truth is split across `domain.py`, an Alembic migration, and a stale `schema.sql` snapshot | Ch. 4, §4.1 | Always check the live ORM models, not the checked-in `pg_dump`, for current schema |
| 4 | Several Kafka topics are provisioned by `kafka-init` but never produced to or consumed from by any live component (`fraud_alerts`, `critical_alerts`, `risk_scores`, `risk_scores_v2`, `enriched_transactions`, `model_features`) | Ch. 6, §6.2 | Trace actual producer/consumer code, not the topic-creation list, to find what's live |
| 5 | `rules_engine.py` and `risk_scoring_engine.py` are not wired into `docker-compose.yml`; `risk_scoring_engine.py`'s topic names (`transactions`, `login_events`) are stale relative to the real `banking.*`-prefixed topics | Ch. 7, §7.1–7.2 | Neither script participates in the live pipeline as currently written |
| 6 | The Flink engine runs at `env.setParallelism(1)` | Ch. 8, §8.9 / Production Perspective | Throughput is bounded by a single task slot; not currently a bottleneck at simulator-scale traffic |
| 7 | `OpenSearchLookupFunction`'s result (`seenBeyondStateWindow`) is computed but not yet used in scoring | Ch. 8, §8.5 | The async lookup runs, and its plumbing exists, but doesn't currently change any alerting decision |
| 8 | The `/api/demo/scenarios/run` on-demand endpoint's dynamic import path doesn't match the real `ScenarioManager` module location, and falls back to a mock-log no-op | Ch. 10, §10.3 | Use the simulator's own internal scheduled loop, not this endpoint, to reliably generate a specific attack scenario |
| 9 | The `simulator` service is present in `infra/docker-compose.yml` (Swarm) but absent from the root `docker-compose.yml` (single-host) | Ch. 10, §10.4 / Ch. 12, §12.6 | On the single-host path, the simulator must be run manually, outside Compose |
| 10 | Grafana's dashboard-provisioning path is configured, but no dashboard JSON files are actually committed to the repository | Ch. 11, §11.4 | A fresh Grafana instance shows no pre-built panels despite being fully wired to Prometheus |
| 11 | Prometheus scrapes the API, Flink JobManager, and Kafka Exporter, but not PostgreSQL, OpenSearch, or Kafka Connect | Ch. 11, §11.1 | Several operationally important signals (replication lag, OpenSearch cluster health, connector-level lag) have no dashboard-visible metric today |
| 12 | Debezium connector credentials are plaintext in a checked-in JSON file | Ch. 5, §5.2 / Production Perspective | Fine for a homelab; would need externalized secrets management before any production use |
| 13 | Self-signed OpenSearch TLS certificates, with certificate verification explicitly disabled in both the Flink engine and the Python sink | Ch. 8, §8.5 / Ch. 9, Production Perspective | Both consumers document this as appropriate only within the isolated Docker network, not beyond it |
| 14 | `docker-compose.yml` originally published the fraud-engine watchdog on host port `9600` without checking that OpenSearch's performance-analyzer port already owned it | Ch. 15, Part 3 | OpenSearch, starting earlier in the dependency graph, always won the race; the watchdog's container came up but its port binding silently failed until remapped to `9601:9600` |
| 15 | `secondary_risk_engine.py`'s Avro-schema path resolution walks up two parent directories from `__file__`, which is correct only when the script runs from its natural two-levels-deep repository location, not from the one-level-deep flat path the Dockerfile actually copies it to | Ch. 15, Part 3 | Fixed with an explicit `API_REQUEST_SCHEMA_PATH` environment variable rather than making the walk-up itself container-aware |
| 16 | `grafana/provisioning/dashboards/dashboards.yml` configures a file-based dashboard provider pointed at `/var/lib/grafana/dashboards`, but no volume was ever mounted at that path in the original `docker-compose.yml` | Ch. 15, Part 2 | No dashboard has ever actually auto-loaded in this stack; a fresh Grafana instance shows a correctly-provisioned datasource and an empty dashboard list, with no error surfaced anywhere |
| 17 | Editing `prometheus/prometheus.yml` (a bind-mounted file external to `docker-compose.yml`) and running `docker compose up -d --build` does not restart the `prometheus` container or cause it to reread the file | Ch. 15, Part 3 | Compose only recreates a container when the service definition inside `docker-compose.yml` itself changes; a bind-mounted config file read once at process startup needs an explicit container restart after every edit |
| 18 | cAdvisor's per-container labeling (`name`, `image`, `container_label_*`) does not populate under this platform's Docker Desktop/WSL2 host, even with `privileged: true` and `/dev/kmsg` mounted | Ch. 15, Part 2 / Part 3 | Only raw, unstable `id="/docker/<hash>"` labels are available; the per-container dashboard panels were removed in favor of host-level `node-exporter` metrics on this deployment target |

### A.2 A cross-cutting pitfalls checklist

This list draws together the "Common Pitfalls" sections from every chapter into a single, scannable set of habits — organized as questions worth asking yourself whenever you're extending this platform, or building something with a similar shape from scratch.

**When adding a new event source:**
- Does it need to be added to Debezium's `table.include.list` (Ch. 5, §5.2), or is it already reachable via the application-telemetry path?
- Have you verified the field names you're relying on against the *live* schema (Ch. 4, §4.1; Ch. 8, §8.3; Ch. 9, §9.4) rather than an assumption or a stale snapshot?
- If it's a CDC-sourced event, have you handled `op == "d"` (delete) events explicitly, where `after` is `null` (Ch. 5, §5.3)?
- Are you trusting the CDC envelope's own `ts_ms` for timestamps, rather than a raw column value whose precision depends on `time.precision.mode` (Ch. 5, §5.3; Ch. 9, §9.3)?

**When adding a new Kafka consumer:**
- Have you given it its own `group.id`, distinct from any consumer that needs to process the same topic independently (Ch. 6, §6.3)?
- If it subscribes to multiple topics of meaningfully different volume, have you considered the poll-loop starvation risk documented in Chapter 9, §9.1 — and would a dedicated per-topic thread or consumer be safer?
- Have you decided, deliberately, whether it needs Avro deserialization (and Schema Registry integration) or plain JSON, per-topic, rather than assuming uniform serialization across the whole cluster (Ch. 6, §6.4; Ch. 9, §9.2)?
- Is your offset-commit strategy safe for at-least-once redelivery — meaning, is your processing step idempotent, or explicitly tolerant of being run twice on the same message (Ch. 9, §9.5; Ch. 13, §13.2)?

**When adding new stateful stream-processing logic:**
- Have you chosen a state structure appropriate to its access pattern — `MapState` for per-key collections accessed by individual entry, rather than a single serialized blob (Ch. 8, §8.7)?
- Is a TTL configured on any state that should eventually expire, and have you verified the cleanup-strategy method actually exists on your framework version (Ch. 8, §8.7)?
- If you're joining a high-volume stream against reference data, have you considered whether broadcast state's lack of cross-stream ordering guarantees affects your correctness requirements (Ch. 8, §8.4)?
- Are you submitting your job to the real cluster via the CLI, rather than accidentally invoking `main()` directly and falling back to an embedded local mini-cluster (Ch. 8, §8.9)?

**When deploying or redeploying:**
- Does every `depends_on` entry use the health-check condition it actually needs — `service_healthy`, not `service_started`, wherever downstream correctness depends on genuine readiness, not just process existence (Ch. 12, §12.1)?
- Does a redeploy correctly re-establish every stateful, external registration (a CDC connector, in this platform's case) that doesn't automatically survive a fresh deployment (Ch. 12, §12.6)?
- Are you certain no host-native process is competing for a port your containerized services expect to own exclusively (Ch. 12, §12.3)?

### A.3 Further reading, organized by chapter

**Chapter 4 — PostgreSQL & the Data Model.** The PostgreSQL documentation's own chapter on Write-Ahead Logging and on logical replication are the primary sources worth reading directly, rather than any secondary summary — they're unusually clear for official database documentation. For the money/`Decimal` precision discussion in §4.5, IEEE 754's own specification is dense but the practical consequences are well covered in most "why not to use floating point for money" language-agnostic explainers.

**Chapter 5 — Debezium & Kafka Connect.** Debezium's own documentation on the PostgreSQL connector, and specifically its page on the change-event envelope structure, is the direct primary source for everything covered in §5.3. Martin Kleppmann's *Designing Data-Intensive Applications* — not specific to this stack at all, but the single most widely recommended book for understanding *why* CDC, log-based architectures, and the outbox pattern exist — is worth reading in full for anyone who wants the deeper theoretical grounding behind this entire chapter.

**Chapter 6 — Kafka & Schema Registry.** The Apache Kafka documentation's own sections on topics/partitions and on consumer groups are the right primary source for §6.2–6.3. Confluent's documentation on Schema Registry and the Confluent wire format (the magic-byte-plus-schema-ID convention manually parsed in `alert_aggregator.py`, §6.4) is the definitive reference for anyone building a second consumer against this exact wire format.

**Chapter 8 — Apache Flink.** The official Flink documentation's sections on state and fault tolerance, on the DataStream API's event time and watermarks, and specifically on the Broadcast State pattern are all directly relevant to the material in this chapter and go considerably deeper than this handbook's scope allowed. Flink's own documentation on `AsyncDataStream` is the right next stop after §8.5. For the broader Flink-vs-Kafka-Streams-vs-Spark-Structured-Streaming comparison raised in this chapter's Production Perspective, all three projects' own architecture documentation, read side by side, is more informative than any third-party comparison article.

**Chapter 9 — OpenSearch.** OpenSearch's own documentation on index mappings and field types (directly relevant to §9.4's corrected mappings) and on the `_bulk`/`_doc` indexing APIs and refresh behavior (directly relevant to §9.5) are the right primary sources. For the broader CQRS pattern named in this chapter's "Why it Exists" section, Greg Young's original writing on CQRS is the primary source most subsequent explanations trace back to.

**Chapter 11 — Observability.** The Prometheus documentation's own page on metric types (directly relevant to §11.2's Counter-vs-Histogram discussion) and Google's *Site Reliability Engineering* book (freely available online), specifically its chapter on monitoring distributed systems, is where the RED and USE methods referenced in this chapter's Production Perspective are most thoroughly explained.

**Chapter 12 — Deployment.** Docker's own Compose specification documentation, specifically its section on `depends_on` and the three condition types covered in §12.1, is the direct primary source. For the Swarm-vs-Kubernetes discussion in this chapter's Production Perspective, Kubernetes's own documentation on StatefulSets is the clearest entry point into why stateful, ordered-deployment services like Kafka and PostgreSQL are handled differently from stateless ones in a mature orchestration platform.

---

*Next: [Chapter 15 — Addendum: Engine Redundancy & Cluster Resource Observability](15-redundancy-and-resource-observability.md)*

# Chapter 15 — Addendum: Engine Redundancy & Cluster Resource Observability

*Previous: [Chapter 14 — Appendix](14-appendix.md)*

---

Unlike Chapters 1 through 14, which document the repository as originally found, this chapter documents two deliberate extensions made afterward: eliminating the Flink risk engine as a single point of failure, and adding cluster-wide resource-usage observability that the platform's original monitoring stack (Chapter 11) never covered. The architecture diagram accompanying this handbook reflects both additions directly — the `SECONDARY`, `WATCHDOG`, `CADV`, and `NEXP` nodes it shows have no counterpart in Chapter 2's diagrams, because those diagrams describe the platform before this work was done. Everything in this chapter is presented with the same rigor as the rest of this handbook: what was built, why, and — candidly — what it cost and what broke along the way.

## 15.1 Part 1 — Eliminating the single point of failure

### The problem, stated precisely

Chapter 8 established that exactly one thing in this platform could turn raw telemetry into a fraud alert: the Java Flink job, `fraud-engine`. Chapter 1's repository map already noted that the repository *contained* two other candidate implementations (`analytics/apache-flink-scripts/`, `analytics/kafka-stream-scripts/rules_engine.py` and `risk_scoring_engine.py`) but that neither was wired into `docker-compose.yml` (Chapter 7). That meant a Flink job crash, a bad deploy, or a checkpoint-recovery stall left the entire platform blind to new fraud for as long as the outage lasted — `RestartStrategies.fixedDelayRestart(3, 10_000L)` (Chapter 8, §8.10) gives the job three chances to self-heal, and if all three fail, it simply stays down until a human intervenes.

### The design decision: active-active, not active-passive

There were two honest ways to solve this. **Active-passive**: keep a secondary engine dormant, detect that Flink is down, then start the secondary. **Active-active**: run both continuously, all the time, and let a downstream aggregation layer treat their outputs as interchangeable.

**Architectural inference.** Active-passive was rejected for a reason worth stating plainly: *detecting* "is Flink actually down" reliably, quickly, and without false positives is itself a hard distributed-systems problem — exactly the kind of thing this platform's own watchdog (below) has to approximate rather than solve perfectly. A passive secondary that only starts consuming once a failure is confirmed also starts with cold state and a Kafka backlog to catch up on, meaning there's a real detection-plus-catch-up gap between the primary failing and the secondary becoming useful. Active-active avoids the whole problem: both engines are always warm, always caught up, and the question "which one is authoritative right now" never has to be answered by any piece of code, because neither one is more authoritative than the other — they're two independent opinions, and the aggregation layer downstream (Chapter 2, §2.5) already knows how to combine independent opinions about the same identity into one incident.

### What was built

**Repository fact.** `analytics/kafka-stream-scripts/secondary_risk_engine.py` is a new, single, consolidated script — superseding the role the never-wired-in `rules_engine.py`/`risk_scoring_engine.py` pair would have played, which remain in the repository unmodified as historical reference, the same leave-it-in-place convention Chapter 1 already noted this codebase follows. It:

- Consumes `api_requests` (Avro), `banking.login_events`, `banking.transactions`, and `banking.accounts` directly — the same raw inputs Flink's `RiskScoringJob` reads, not a downstream topic Flink itself produces, which is what makes it a genuinely independent second opinion rather than a thing that inherits Flink's own blind spots.
- Rebuilds a simplified `account_id → customer_id` map from `banking.accounts` CDC events in a plain dict, with the identical `"acct:<id>"` fallback identity Flink's `AccountEnrichmentFunction` uses when resolution hasn't caught up yet (Chapter 8, §8.4).
- Re-implements the same indicator taxonomy documented in Chapter 8, §8.8 — `NEW_DEVICE`, `NEW_COUNTRY`, `IMPOSSIBLE_TRAVEL`, `MULTIPLE_FAILED_LOGINS`, `LARGE_TRANSFER_AMOUNT`, `VELOCITY_VIOLATION`, `SUSPICIOUS_API_PATTERN` — against the identical default thresholds from `risk-scoring.properties`, specifically so the two engines' alerts are comparable, not just structurally similar.
- Runs one `Consumer`/one `threading.Thread` per topic — deliberately mirroring the per-topic-thread fix Chapter 9 documents in `opensearch_sink.py`, so this new engine doesn't reintroduce the exact consumer-starvation bug that chapter spends a full section explaining.
- Emits alerts to `fraud_alerts`/`critical_alerts` — the two topics `kafka-init` has provisioned since the stack's first commit but that Chapter 6, §6.2 flagged as provisioned yet unproduced-to and unconsumed-from anywhere in the live pipeline. Repurposing them here means zero new topics were needed.
- Exposes Prometheus metrics on `:9500` (`secondary_engine_events_processed_total`, `secondary_engine_alerts_emitted_total`, `secondary_engine_up`, `secondary_engine_identities_tracked`).

What was deliberately **not** attempted: matching Flink's exactly-once, checkpointed, RocksDB-backed state (Chapter 8, §8.7, §8.10). This engine's state is a plain in-memory dict, lost on restart. That's a real, named trade-off, not an oversight — see Production Perspective below.

### Wiring the aggregation layer to actually use both engines

A secondary engine emitting alerts nobody reads is not redundancy. Three real bugs in the existing aggregation path (Chapter 2, §2.5) had to be fixed to make the two engines' outputs genuinely fungible:

1. **`alert_aggregator.py` was only ever subscribed to `fraud_alerts_v2`.** `critical_alerts_v2` — Flink's *higher-severity* output topic — was never consumed at all. Every `CRITICAL` alert Flink ever produced was silently discarded before it could become an incident. Fixed by subscribing to all four alert topics (`fraud_alerts_v2`, `critical_alerts_v2`, `fraud_alerts`, `critical_alerts`) in one consumer.
2. **The aggregator was reading the wrong field names.** `FraudAlertAvroSerializationSchema.java` (Chapter 8, §8.10) puts the score on the wire as `cumulative_score` and the triggering indicator as `trigger_reason`; the aggregator was reading `risk_score` and `rule_name` — fields that simply don't exist on a real Flink alert. Every incident sourced from the primary engine was silently falling back to a hardcoded `50.0`/`"ANOMALY_DETECTED"`, meaning severity escalation (`>= 90` → `CRITICAL`) could never actually trigger from a real Flink alert's true score. Fixed by accepting both naming conventions.
3. **No engine provenance was tracked.** Fixed by tagging each alert with `engine_source` (explicit from the secondary engine, inferred from topic name for Flink's), and adding `engine_sources`/`corroborated` columns to the `Incident` model (migration `003_add_incident_engine_redundancy_fields.py`, alongside the `002_create_incident_management_tables.py` migration described in Chapter 3, §3.3.5) so an incident that both engines independently flagged is now visibly distinguishable — `corroborated=True` — from one only one engine has seen. `GET /api/v1/incidents?corroborated=true` surfaces exactly those cases to an analyst as the strongest-signal subset of the queue.

### The watchdog

**Repository fact.** `analytics/engine-watchdog/watchdog.py` answers, continuously, "is at least one engine actually up right now" — polling Flink's JobManager REST API (`GET /jobs`, checking for a `RUNNING` job) and the secondary engine's own `/metrics` endpoint every 15 seconds, and exposing `fraud_engine_primary_up`, `fraud_engine_secondary_up`, and a combined `fraud_engine_redundancy_status` gauge (`0`=both down, `1`=secondary-only, `2`=primary-only, `3`=both up — normal). It performs no failover itself, because — as established above — there's nothing to fail *over* to; both engines already run continuously. Its only job is making the redundancy state observable rather than invisible, which is what the new Grafana dashboard (Part 2, below) graphs directly.

### Production Perspective

- **State durability is the honest cost of this design.** The secondary engine's dict-based state is wiped on every restart, unlike Flink's checkpointed RocksDB state (Chapter 8, §8.7). In active-active operation this is a much smaller risk than it would be for a sole engine — a restart only means the *secondary's* opinion resets, not the platform's only opinion — but a rigorous production deployment might still want the secondary to periodically snapshot its state (even a simple periodic JSON dump to a volume) to shrink its own post-restart blind window further.
- **Two engines double the false-positive surface, not just the coverage.** A production rollout of this pattern would want the SOC workflow (Chapter 2, §2.5) to visibly prioritize `corroborated=True` incidents — which this addendum's schema and API filter now support — precisely because agreement between two independently-implemented detectors is a much stronger signal than either alone, and triage capacity should be spent there first.
- **The secondary engine is intentionally simpler, not intentionally weaker.** It will not catch everything Flink's broadcast-join/async-I/O/watermark-aware pipeline catches (Chapter 8, §8.5's `OpenSearchLookupFunction` context, for instance, has no analogue here). Its job is to keep *some* detection running when Flink can't, not to be Flink's equal in every dimension — conflating "redundant" with "equivalent" is a common and costly mistake when designing failover/backstop systems generally.

## 15.2 Part 2 — Cluster-wide resource usage

### The gap

Chapter 11 covered the platform's existing observability thoroughly: `banking_api_requests_total`/`_duration_seconds` from the API, Flink's own Prometheus reporter, and Kafka consumer-lag via `kafka-exporter`. Every one of those is an *application-level* metric — request counts, job status, lag. Nothing in the original stack answered a much more basic operational question: **how much CPU, memory, disk, and network is each container, and the host as a whole, actually using right now?** Without that, an operator has no way to see a container approaching its `deploy.resources.limits` cap before it gets OOM-killed, no way to tell which service is actually driving load during a slow period, and — directly relevant to Part 1 — no way to distinguish "the secondary engine's container was OOM-killed" from "the secondary engine's process crashed for an application-level reason," since only the former shows up in resource metrics rather than application logs.

### What was added

- **cAdvisor** (`gcr.io/cadvisor/cadvisor:v0.47.2`) — per-container CPU, memory, network, and disk-I/O metrics for every `bank_*` container in the compose stack, scraped by the existing Prometheus instance on a new `cadvisor` job.
- **node-exporter** (`prom/node-exporter:v1.7.0`) — host-level CPU, memory, disk, and network metrics, covering resource usage outside any individual container's cgroup as well.
- A new Prometheus scrape config for both (`prometheus/prometheus.yml`), extending the scrape targets Chapter 11, §11.1 already documents (the API, Flink JobManager, and Kafka Exporter — still notably not PostgreSQL, OpenSearch, or Kafka Connect, a gap Chapter 11 already flagged and this addendum does not close).
- Two new Grafana dashboards: `grafana/dashboards/cluster-resource-usage.json` (host + per-container panels) and `grafana/dashboards/fraud-engine-redundancy.json` (the Part 1 watchdog metrics, visualized) — the first dashboard JSON files actually committed to this repository, closing the gap Chapter 11, §11.4 raised about dashboard-provisioning being configured but never populated.

### A pre-existing bug this surfaced and fixed

Wiring up the new dashboards required mounting `./grafana/dashboards` into the Grafana container at `/var/lib/grafana/dashboards` — and doing that revealed that **no volume was ever mounted at that path in the original `docker-compose.yml`**, despite `grafana/provisioning/dashboards/dashboards.yml` explicitly configuring a file-based dashboard provider pointed exactly there. In practice, this means **no dashboard had ever actually auto-loaded in this stack** — Grafana would start, find the datasource provisioned correctly, and have an empty dashboard list, silently, with no error. This is now fixed as a side effect of adding the two new dashboards, but it's worth calling out on its own: it's a good example of how an observability *gap* can itself go unobserved — nothing about a missing dashboard trips an alert, because the absence of a graph isn't the kind of thing Prometheus or Grafana monitor about themselves.

### Production Perspective

- **cAdvisor's full feature set (OOM events, per-process detail) needs `privileged: true` and a `/dev/kmsg` mount**, both deliberately omitted here because the existing stack's own volume paths indicate a Windows/Docker Desktop host, where that device isn't available the same way. On a native Linux deployment target, both should be re-added for full coverage; CPU/memory/network metrics work correctly either way.
- **No resource-based alerting rules exist yet.** Chapter 11 already flagged that `prometheus.yml` has no `rule_files`/Alertmanager integration; that gap now extends to these new metrics too — a natural next step is alerting on sustained high memory relative to `deploy.resources.limits`, and specifically on `fraud_engine_redundancy_status` dropping below `3` for more than a few minutes, which is the single most actionable alert this whole addendum makes possible.

## 15.3 Part 3 — What actually broke during rollout, and why

Everything above describes the design as built. Deploying it onto a real host surfaced five concrete problems — none hypothetical, all diagnosed against an actual running stack — worth recording here precisely because they're the kind of thing a design document never predicts and only a real rollout finds. Each one is a small, transferable lesson in its own right, and each is also carried into the cross-referenced pitfalls table in Chapter 14, Appendix A.1.

**1. `docker-compose.yml` reused a port OpenSearch already owned.** `fraud-engine-watchdog` was originally given host port `9600` without cross-checking the rest of the file — OpenSearch's performance-analyzer port was already published there (`9600:9600`, pre-existing, nothing to do with this addendum). Since OpenSearch starts earlier in the dependency graph (Chapter 12, §12.1), it won every race for that port, and the watchdog's container creation succeeded while its network binding failed outright. Fixed by moving the watchdog to `9601:9600` — host-side only; the container-internal port, the `WATCHDOG_METRICS_PORT` env var, and Prometheus's scrape target all correctly stayed at `9600`, since none of those cross the host boundary.

**2. A container-relative path assumption broke across the Dockerfile boundary.** `secondary_risk_engine.py`'s Avro schema lookup walked up two parent directories from `__file__` to find the repo root — correct when the script runs from its natural location (`analytics/kafka-stream-scripts/secondary_risk_engine.py`, two levels deep), but wrong once the Dockerfile copies it flat to `/app/secondary_risk_engine.py`, one level deep. The walk-up silently resolved to `/` instead of the repo root, and the schema was never found. Fixed with an explicit `API_REQUEST_SCHEMA_PATH` environment variable pointing at the exact path the Dockerfile actually copies the schema to (`/app/config/schemas/api_request.avsc`), bypassing the walk-up entirely rather than trying to make the walk-up itself container-aware.

**3. Python's stdout buffering made two healthy containers look silently dead.** With `fraud-engine-secondary` and `fraud-engine-watchdog` both freshly deployed, `docker compose logs` showed nothing at all for either — not an error, just silence — which reads exactly like a crash-on-startup with no traceback. The actual cause: CPython block-buffers stdout when it isn't attached to a TTY, which is always true inside a container. `print()` calls queue in an in-process buffer that only flushes once full or on exit; with only a handful of short status lines per minute, that buffer can simply never fill. Both processes were running correctly the entire time. Fixed with `ENV PYTHONUNBUFFERED=1` in both Dockerfiles — a one-line fix, but the diagnostic path to it (confirming liveness via `curl .../metrics` and `docker compose ps` *before* trusting an empty log stream) is the more durable lesson: an empty log is not proof of a dead process for any unbuffered-by-default language runtime running inside a container.

**4. Editing a bind-mounted config file doesn't get picked up without a manual restart.** After adding four new scrape jobs to `prometheus/prometheus.yml`, `docker compose up -d --build` did **not** restart the `prometheus` container, because Compose only recreates a container when the *service definition inside `docker-compose.yml` itself* changes — image, environment, volumes, ports. `prometheus.yml` lives outside that file, as a bind-mounted external file Compose has no visibility into; Prometheus itself (run here without `--web.enable-lifecycle`) only reads that file once, at process startup. The result: new scrape targets could sit correctly on disk indefinitely with zero effect, confirmed directly via `http://localhost:9090/config` showing the stale three-job config well after the edit. This generalizes beyond Prometheus and beyond this platform: **any bind-mounted config file that a long-running process reads once at boot needs an explicit restart of that specific container after every edit** — `docker compose up -d --build` is necessary but not sufficient when the change lives outside the compose file's own service definition.

**5. cAdvisor's per-container labeling doesn't work reliably under Docker Desktop's WSL2 backend.** Even with `privileged: true`, the `/dev/kmsg` device, and `/var/run/docker.sock` mounted explicitly, cAdvisor's own logs never showed a single line about Docker container registration — no error, no success, nothing — and every genuine container's cgroup metrics carried only a raw `id="/docker/<hash>"` label, with no `name`, `image`, or `container_label_*` attached at all. This was confirmed directly (`curl .../metrics | grep container_cpu_usage_seconds_total`) rather than assumed. Rather than keep escalating cAdvisor flags against a platform limitation, the four per-container panels were removed from `cluster-resource-usage.json` outright — they could only ever have shown unreadable, recreate-unstable raw hashes — leaving the host-level `node-exporter` panels (fully functional, confirmed) as the dashboard's actual resource-usage signal for this single-host deployment. A native Linux host would very likely not hit this at all; it's specifically a WSL2/Docker-Desktop characteristic, documented directly on the now-simplified dashboard's own text panel so a future reader doesn't have to rediscover it.

### Summary

This chapter closed two real gaps left open by the rest of this handbook: a single point of failure in fraud detection (Chapter 8's Flink engine, now backed by an independent, active-active secondary and a watchdog that makes the redundancy state observable rather than assumed), and a blind spot in observability itself (Chapter 11's metrics stack, now extended with cluster-wide resource usage via cAdvisor and node-exporter, and — as a direct side effect — a dashboard-provisioning bug fixed after sitting silently broken since the stack's first commit). Part 3's five rollout failures are worth carrying forward as a set, independent of this specific addendum: a port collision invisible until two services race for it, a path assumption that only breaks once code crosses a packaging boundary, a language runtime's default buffering behavior masquerading as a crash, a bind-mounted config file's staleness being invisible to the orchestrator that mounted it, and a monitoring tool's core feature silently failing under one specific virtualization backend. None of these are exotic; all five are the kind of thing worth checking for, specifically, the next time a similarly-shaped system goes from a design document to a running deployment.

---

## Closing note

This handbook set out to do two things at once: document `banking-fraud-platform` completely and honestly, and use it as a genuine teaching text for event-driven architecture, streaming systems, and production engineering discipline. The repository earned that second use — not because every design decision in it is what a bank's engineering team would ship, but because nearly every decision, including the ones this handbook flagged as gaps or tensions rather than triumphs, is legible, traceable to a real tradeoff, and worth understanding on its own terms. That's a genuinely rare property in a codebase of this size, and it's what made fourteen chapters of "here's what the code does, and here's why" possible without ever needing to invent architecture that wasn't actually there.

If you take one habit from this handbook into your own work, let it be the one this appendix's checklist keeps returning to: **verify against the running system, not against what you assume it to be** — the discipline visible in `RiskEventNormalizer.java`'s schema comments, in `opensearch_sink.py`'s corrected index mappings, and in this handbook's own chapter-by-chapter cross-referencing of the codebase against itself. It's the single most transferable lesson this repository has to teach, and it will serve you well on systems this handbook never saw.

*Back to: [Index](00-index.md)*
