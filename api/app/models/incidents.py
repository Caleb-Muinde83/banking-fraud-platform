import enum
import uuid
from datetime import datetime
from sqlalchemy import (
    Column, String, Integer, Float, DateTime, Text, 
    Enum, ForeignKey, func
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

# Imports Base from your existing database configuration
from app.database import Base


class IncidentStatus(str, enum.Enum):
    OPEN = "OPEN"
    INVESTIGATING = "INVESTIGATING"
    FALSE_POSITIVE = "FALSE_POSITIVE"
    CONFIRMED_FRAUD = "CONFIRMED_FRAUD"
    RESOLVED_OTHER = "RESOLVED_OTHER"


class IncidentSeverity(str, enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Incident(Base):
    __tablename__ = "incidents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title = Column(String(255), nullable=False)
    identity_key = Column(String(255), index=True, nullable=False)
    status = Column(Enum(IncidentStatus), default=IncidentStatus.OPEN, index=True, nullable=False)
    severity = Column(Enum(IncidentSeverity), default=IncidentSeverity.MEDIUM, index=True, nullable=False)
    alert_count = Column(Integer, default=1, nullable=False)
    max_risk_score = Column(Float, default=0.0, nullable=False)
    assigned_to = Column(String(100), nullable=True)
    
    first_alert_at = Column(DateTime(timezone=True), nullable=False)
    last_alert_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    alerts = relationship("IncidentAlert", back_populates="incident", cascade="all, delete-orphan")
    audit_logs = relationship("IncidentAuditLog", back_populates="incident", cascade="all, delete-orphan")


class IncidentAlert(Base):
    __tablename__ = "incident_alerts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    incident_id = Column(UUID(as_uuid=True), ForeignKey("incidents.id", ondelete="CASCADE"), nullable=False)
    alert_id = Column(String(255), index=True, nullable=False)
    indicator = Column(String(100), nullable=False)
    risk_score = Column(Float, nullable=False)
    source_topic = Column(String(100), nullable=False)
    raw_payload = Column(JSONB, nullable=False)
    received_at = Column(DateTime(timezone=True), nullable=False)

    incident = relationship("Incident", back_populates="alerts")


class IncidentAuditLog(Base):
    __tablename__ = "incident_audit_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    incident_id = Column(UUID(as_uuid=True), ForeignKey("incidents.id", ondelete="CASCADE"), nullable=False)
    previous_status = Column(String(50), nullable=True)
    new_status = Column(String(50), nullable=False)
    changed_by = Column(String(100), nullable=False)
    notes = Column(Text, nullable=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    incident = relationship("Incident", back_populates="audit_logs")