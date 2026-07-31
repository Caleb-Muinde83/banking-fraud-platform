from typing import List, Optional
from uuid import UUID
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from sqlalchemy.orm import selectinload

from app.core.database import get_db
from app.models.incidents import Incident, IncidentStatus, IncidentSeverity, IncidentAuditLog
from app.schemas.incident import (
    IncidentResponse, 
    IncidentDetailResponse, 
    UpdateStatusRequest, 
    AssignIncidentRequest
)

router = APIRouter(prefix="/incidents", tags=["Case Management"])


@router.get("", response_model=List[IncidentResponse])
async def list_incidents(
    status_filter: Optional[IncidentStatus] = Query(None, alias="status"),
    severity_filter: Optional[IncidentSeverity] = Query(None, alias="severity"),
    identity_key: Optional[str] = Query(None),
    corroborated: Optional[bool] = Query(
        None, description="Filter to incidents confirmed by both the primary and secondary risk engines"
    ),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(Incident).options(
        selectinload(Incident.alerts),
        selectinload(Incident.audit_logs)
    )
    
    if status_filter:
        stmt = stmt.where(Incident.status == status_filter)
    if severity_filter:
        stmt = stmt.where(Incident.severity == severity_filter)
    if identity_key:
        stmt = stmt.where(Incident.identity_key == identity_key)
    if corroborated is not None:
        stmt = stmt.where(Incident.corroborated == corroborated)

    stmt = stmt.order_by(desc(Incident.updated_at)).offset(offset).limit(limit)
    
    result = await db.execute(stmt)
    return result.scalars().all()


@router.get("/{incident_id}", response_model=IncidentDetailResponse)
async def get_incident_detail(incident_id: UUID, db: AsyncSession = Depends(get_db)):
    stmt = (
        select(Incident)
        .options(
            selectinload(Incident.alerts),
            selectinload(Incident.audit_logs)
        )
        .where(Incident.id == incident_id)
    )
    result = await db.execute(stmt)
    incident = result.scalar_one_or_none()
    
    if not incident:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Incident {incident_id} not found"
        )
    return incident


@router.patch("/{incident_id}/status", response_model=IncidentDetailResponse)
async def update_incident_status(
    incident_id: UUID, 
    req: UpdateStatusRequest, 
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(Incident)
        .options(
            selectinload(Incident.alerts),
            selectinload(Incident.audit_logs)
        )
        .where(Incident.id == incident_id)
    )
    result = await db.execute(stmt)
    incident = result.scalar_one_or_none()

    if not incident:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Incident {incident_id} not found"
        )

    previous_status = incident.status.value
    
    incident.status = req.new_status
    incident.updated_at = datetime.now(timezone.utc)

    audit_entry = IncidentAuditLog(
        incident_id=incident.id,
        previous_status=previous_status,
        new_status=req.new_status.value,
        changed_by=req.changed_by,
        notes=req.notes
    )
    db.add(audit_entry)
    await db.commit()
    
    # Re-fetch with loaded options to ensure clean serialization after commit
    result = await db.execute(stmt)
    incident = result.scalar_one()

    print(f"🏷️ Ground Truth Label Updated: Incident {incident.id} transition: {previous_status} -> {req.new_status.value}")
    return incident


@router.post("/{incident_id}/assign", response_model=IncidentResponse)
async def assign_incident(
    incident_id: UUID, 
    req: AssignIncidentRequest, 
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(Incident)
        .options(
            selectinload(Incident.alerts),
            selectinload(Incident.audit_logs)
        )
        .where(Incident.id == incident_id)
    )
    result = await db.execute(stmt)
    incident = result.scalar_one_or_none()

    if not incident:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Incident {incident_id} not found"
        )

    incident.assigned_to = req.assigned_to
    
    if incident.status == IncidentStatus.OPEN:
        previous_status = incident.status.value
        incident.status = IncidentStatus.INVESTIGATING
        incident.updated_at = datetime.now(timezone.utc)
        
        audit_entry = IncidentAuditLog(
            incident_id=incident.id,
            previous_status=previous_status,
            new_status=IncidentStatus.INVESTIGATING.value,
            changed_by=req.changed_by,
            notes=f"Case automatically advanced to INVESTIGATING upon assignment to {req.assigned_to}"
        )
        db.add(audit_entry)

    await db.commit()
    
    # Re-fetch updated incident with preloaded relationships
    result = await db.execute(stmt)
    incident = result.scalar_one()
    return incident