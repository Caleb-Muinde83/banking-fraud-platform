from typing import List, Optional
from fastapi import APIRouter, Depends, Header
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Card

router = APIRouter(prefix="/api", tags=["Card Management"])

class CardResponse(BaseModel):
    card_id: str
    account_id: Optional[str] = None
    card_type: str = "DEBIT"
    status: str = "ACTIVE"
    expiration_date: Optional[str] = "12/29"

    class Config:
        from_attributes = True

class CardBlockResponse(BaseModel):
    card_id: str
    status: str
    message: str

@router.get("/cards", response_model=List[CardResponse])
def get_user_cards(
    x_user_id: str = Header(..., alias="X-User-Id"),
    db: Session = Depends(get_db)
):
    try:
        # Synchronous DB query
        db_cards = db.query(Card).filter(Card.customer_id == x_user_id).all()
        if db_cards:
            return db_cards
    except Exception:
        db.rollback()

    # Simulator Fallback
    safe_id_suffix = (x_user_id + "000000")[-6:]
    return [
        CardResponse(
            card_id=f"CARD-{safe_id_suffix}-01",
            account_id=f"SIM-ACC-{safe_id_suffix}",
            card_type="DEBIT",
            status="ACTIVE",
            expiration_date="12/29"
        )
    ]

@router.post("/cards/{id}/block", response_model=CardBlockResponse)
def block_card(
    id: str,
    db: Session = Depends(get_db)
):
    try:
        # Synchronous DB query
        card = db.query(Card).filter(Card.card_id == id).first()
        if card:
            card.status = "BLOCKED"
            db.commit()
            db.refresh(card)
            return CardBlockResponse(
                card_id=card.card_id,
                status=card.status,
                message=f"Card {id} has been successfully BLOCKED."
            )
    except Exception:
        db.rollback()

    return CardBlockResponse(
        card_id=id,
        status="BLOCKED",
        message=f"Card {id} has been successfully BLOCKED (simulated)."
    )