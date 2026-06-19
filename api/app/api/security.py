from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime

# Adjusting imports to match your existing 'api' folder structure
from app.core.database import get_db 
from app.models import domain as models 

router = APIRouter(prefix="/api", tags=["Security & Operational Extensions"])

# --- Pydantic Schemas ---
class BeneficiaryResponse(BaseModel):
    beneficiary_id: str
    customer_id: str
    account_number: str
    bank_name: str
    created_at: datetime
    class Config: from_attributes = True

class DeviceResponse(BaseModel):
    device_id: str
    customer_id: str
    device_type: str
    first_seen: datetime
    risk_score: float
    class Config: from_attributes = True

class SessionResponse(BaseModel):
    session_id: str
    customer_id: str
    device_id: str
    ip_address: str
    created_at: datetime
    expires_at: datetime
    class Config: from_attributes = True

# --- Endpoints ---
@router.get("/beneficiaries", response_model=List[BeneficiaryResponse])
def get_beneficiaries(customer_id: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(models.Beneficiary)
    if customer_id: query = query.filter(models.Beneficiary.customer_id == customer_id)
    return query.all()

@router.get("/sessions", response_model=List[SessionResponse])
def get_sessions(customer_id: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(models.SessionModel)
    if customer_id: query = query.filter(models.SessionModel.customer_id == customer_id)
    return query.all()

@router.get("/devices", response_model=List[DeviceResponse])
def get_devices(customer_id: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(models.Device)
    if customer_id: query = query.filter(models.Device.customer_id == customer_id)
    return query.all()