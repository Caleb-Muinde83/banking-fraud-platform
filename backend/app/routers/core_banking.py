from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List

from app.database import get_db 
from app import models
from app.schemas import BeneficiaryResponse  # Explicit schema import

router = APIRouter(prefix="/api", tags=["Core Banking Extensions"])

@router.get("/beneficiaries", response_model=List[BeneficiaryResponse])
def get_beneficiaries(
    customer_id: Optional[str] = Query(None, description="Filter beneficiaries by Customer ID"),
    db: Session = Depends(get_db)
):
    """
    GET /api/beneficiaries
    Retrieves all beneficiaries, optionally filtered by a specific customer ID.
    """
    query = db.query(models.Beneficiary)
    if customer_id:
        query = query.filter(models.Beneficiary.customer_id == customer_id)
    return query.all()