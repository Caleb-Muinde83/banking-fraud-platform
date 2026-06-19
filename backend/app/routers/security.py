from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List

from app.database import get_db 
from app import models
from app.schemas import SessionResponse, DeviceResponse  # Explicit schema imports

router = APIRouter(prefix="/api", tags=["Security Monitoring Extensions"])

@router.get("/sessions", response_model=List[SessionResponse])
def get_sessions(
    customer_id: Optional[str] = Query(None, description="Filter active sessions by Customer ID"),
    db: Session = Depends(get_db)
):
    """
    GET /api/sessions
    Retrieves active or logged session profiles for system audits and session-hijack threat hunting.
    """
    query = db.query(models.SessionModel)
    if customer_id:
        query = query.filter(models.SessionModel.customer_id == customer_id)
    return query.all()


@router.get("/devices", response_model=List[DeviceResponse])
def get_devices(
    customer_id: Optional[str] = Query(None, description="Filter registered devices by Customer ID"),
    db: Session = Depends(get_db)
):
    """
    GET /api/devices
    Retrieves registered user hardware fingerprints and device risk scores.
    """
    query = db.query(models.Device)
    if customer_id:
        query = query.filter(models.Device.customer_id == customer_id)
    return query.all()