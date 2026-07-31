import os
import json
import asyncio
import struct
import io
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fastavro
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from dotenv import load_dotenv

# Load env variables
ROOT_DIR = Path(__file__).resolve().parents[3]
load_dotenv(ROOT_DIR / ".env")

# Re-use async DB URL logic
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres_admin:SecureBankPassword2026!@localhost:5433/banking_db"
)
# Ensure it uses asyncpg
if "psycopg2" in DB_URL:
    DB_URL = DB_URL.replace("postgresql+psycopg2", "postgresql+asyncpg")
elif "localhost:5433" not in DB_URL and "bank_postgres" in DB_URL:
    # If running locally, fall back to localhost port
    try:
        import socket
        socket.gethostbyname("bank_postgres")
    except socket.gaierror:
        DB_URL = DB_URL.replace("bank_postgres:5432", "localhost:5433").replace("bank_postgres", "localhost")


engine = create_async_engine(DB_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Import models
from app.models.incidents import Incident, IncidentAlert, IncidentStatus, IncidentSeverity

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")

# REDUNDANCY: previously this consumed ONLY "fraud_alerts_v2" -- which meant
# two things were true and neither was intentional: (1) CRITICAL-severity
# Flink alerts on critical_alerts_v2 never became incidents at all, and
# (2) the platform had no way to form an incident from anything other than
# the single Flink engine. Both are fixed by subscribing to all four alert
# topics: the "_v2" pair is the primary (Flink) engine's output, the
# unsuffixed pair is the secondary (Python) engine's output -- see
# analytics/kafka-stream-scripts/secondary_risk_engine.py. Aggregation below
# is engine-agnostic: it keys on identity_key regardless of which topic (and
# therefore which engine) an alert came from, which is what lets an incident
# be corroborated by either engine, or continue forming even if one engine
# is completely down.
ALERT_TOPICS = ["fraud_alerts_v2", "critical_alerts_v2", "fraud_alerts", "critical_alerts"]
PRIMARY_TOPICS = {"fraud_alerts_v2", "critical_alerts_v2"}
WINDOW_MINUTES = 30

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:18081")
SCHEMA_CACHE = {}

def decode_message(msg_value: bytes) -> dict:
    """Smart decoder that handles both Plain JSON and Confluent Avro binary formats."""
    # Check if the message starts with the Confluent Magic Byte (0x00)
    if msg_value[0] == 0:
        # Unpack the 1-byte magic number and 4-byte schema ID
        magic, schema_id = struct.unpack('>bI', msg_value[:5])
        
        # Fetch schema from registry if we haven't seen it yet
        if schema_id not in SCHEMA_CACHE:
            print(f"🔍 Fetching unknown Schema ID {schema_id} from Registry...")
            url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read().decode())
                schema_str = data['schema']
                SCHEMA_CACHE[schema_id] = fastavro.parse_schema(json.loads(schema_str))
        
        # Decode the Avro binary payload (skipping the first 5 bytes)
        bytes_reader = io.BytesIO(msg_value[5:])
        return fastavro.schemaless_reader(bytes_reader, SCHEMA_CACHE[schema_id])
    else:
        # Fallback to standard JSON decoding
        return json.loads(msg_value.decode("utf-8"))


async def process_alert(alert_data: dict, db: AsyncSession, source_topic: str):
    """Core logic to deduplicate alerts into Incidents"""
    identity_key = alert_data.get("customer_id") or alert_data.get("account_id") or "UNKNOWN"
    alert_id = alert_data.get("alert_id", f"alt_{int(datetime.now().timestamp())}")

    # BUGFIX: this previously read "risk_score" / "rule_name", but the Flink
    # engine's actual wire fields (FraudAlertAvroSerializationSchema.java) are
    # "cumulative_score" / "trigger_reason" / "contributing_indicators" --
    # meaning every real Flink alert was silently falling through to the
    # 50.0 / "ANOMALY_DETECTED" defaults below, and every incident from the
    # primary engine was landing at MEDIUM severity regardless of its real
    # score. Both field names are now accepted so payloads from either engine
    # (which share this exact field naming by design -- see
    # secondary_risk_engine.py) resolve correctly.
    risk_score = float(alert_data.get("cumulative_score", alert_data.get("risk_score", 50.0)))
    indicator = (
        alert_data.get("trigger_reason")
        or alert_data.get("rule_name")
        or (alert_data.get("contributing_indicators") or [None])[-1]
        or "ANOMALY_DETECTED"
    )

    # REDUNDANCY: tag which engine produced this alert. Explicit engine_source
    # field (secondary_risk_engine.py sets this) wins; otherwise infer from
    # which topic it arrived on.
    engine_source = alert_data.get("engine_source") or (
        "flink-primary" if source_topic in PRIMARY_TOPICS else "python-secondary"
    )
    alert_data = {**alert_data, "engine_source": engine_source, "_source_topic": source_topic}
    
    # 1. Look for an OPEN/INVESTIGATING incident for this identity in the last 30 minutes
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=WINDOW_MINUTES)
    
    stmt = select(Incident).where(
        Incident.identity_key == identity_key,
        Incident.status.in_([IncidentStatus.OPEN, IncidentStatus.INVESTIGATING]),
        Incident.updated_at >= cutoff_time
    )
    result = await db.execute(stmt)
    incident = result.scalar_one_or_none()

    if incident:
        # 2a. Update existing incident
        prior_sources = set(incident.engine_sources or [])
        newly_corroborated = engine_source not in prior_sources and len(prior_sources) >= 1
        incident.engine_sources = sorted(prior_sources | {engine_source})
        incident.corroborated = len(incident.engine_sources) > 1

        tag = " (now corroborated by a second engine)" if newly_corroborated else ""
        print(f"🔄 Bundling alert {alert_id} [{engine_source}] into existing Incident {incident.id}{tag}")

        incident.alert_count += 1
        incident.max_risk_score = max(incident.max_risk_score, risk_score)
        incident.last_alert_at = datetime.now(timezone.utc)
        
        # Escalate severity if max_risk_score goes very high
        if incident.max_risk_score >= 90:
            incident.severity = IncidentSeverity.CRITICAL
        elif incident.max_risk_score >= 70:
            incident.severity = IncidentSeverity.HIGH
            
    else:
        # 2b. Create brand new incident
        print(f"🚨 Creating NEW Incident for identity: {identity_key} (first seen by {engine_source})")
        severity = IncidentSeverity.MEDIUM
        if risk_score >= 90: severity = IncidentSeverity.CRITICAL
        elif risk_score >= 70: severity = IncidentSeverity.HIGH
        
        incident = Incident(
            title=f"Fraud Alert: {indicator}",
            identity_key=identity_key,
            status=IncidentStatus.OPEN,
            severity=severity,
            alert_count=1,
            engine_sources=[engine_source],
            corroborated=False,
            max_risk_score=risk_score,
            first_alert_at=datetime.now(timezone.utc),
            last_alert_at=datetime.now(timezone.utc)
        )
        db.add(incident)
        await db.flush() # flush to get incident.id for the alert

    # 3. Create the linked alert record
    db_alert = IncidentAlert(
        incident_id=incident.id,
        alert_id=alert_id,
        indicator=indicator,
        risk_score=risk_score,
        source_topic=source_topic,
        raw_payload=alert_data,
        received_at=datetime.now(timezone.utc)
    )
    db.add(db_alert)
    await db.commit()


async def consume():
    print(f"🎧 Starting Alert Aggregator Worker... listening to {ALERT_TOPICS} on {KAFKA_BROKER}")
    consumer = AIOKafkaConsumer(
        *ALERT_TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        group_id="fraud_incident_aggregator",
        auto_offset_reset="earliest"
    )
    
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                # Use our new smart decoder! (handles both engines' output --
                # Avro from Flink, plain JSON from the secondary engine)
                alert_data = decode_message(msg.value)
                
                async with AsyncSessionLocal() as db:
                    await process_alert(alert_data, db, source_topic=msg.topic)
                    
            except Exception as e:
                print(f"❌ Error processing message from {msg.topic}: {e}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())