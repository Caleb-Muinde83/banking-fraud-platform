from datetime import datetime
from typing import List, Optional, Any, Dict
from uuid import UUID
from pydantic import BaseModel, Field
from app.models import IncidentStatus, IncidentSeverity


class IncidentAlertResponse(BaseModel):
    id: UUID
    alert_id: str
    indicator: str
    risk_score: float
    source_topic: str
    raw_payload: Dict[str, Any]
    received_at: datetime

    class Config:
        from_attributes = True


class IncidentAuditLogResponse(BaseModel):
    id: UUID
    previous_status: Optional[str] = None
    new_status: str
    changed_by: str
    notes: Optional[str] = None
    timestamp: datetime

    class Config:
        from_attributes = True


class IncidentResponse(BaseModel):
    id: UUID
    title: str
    identity_key: str
    status: IncidentStatus
    severity: IncidentSeverity
    alert_count: int
    max_risk_score: float
    assigned_to: Optional[str] = None
    first_alert_at: datetime
    last_alert_at: datetime
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class IncidentDetailResponse(IncidentResponse):
    alerts: List[IncidentAlertResponse] = []
    audit_logs: List[IncidentAuditLogResponse] = []


class UpdateStatusRequest(BaseModel):
    new_status: IncidentStatus
    changed_by: str = Field(..., description="Analyst username or ID making the change")
    notes: Optional[str] = Field(None, description="Investigation notes or justification")


class AssignIncidentRequest(BaseModel):
    assigned_to: str = Field(..., description="Analyst username or ID assigned to case")
    changed_by: str = Field(..., description="Analyst username performing assignment")