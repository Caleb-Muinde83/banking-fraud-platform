import uuid
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import List

router = APIRouter(prefix="/api", tags=["Card Management"])

# Local schema for future POS expansion
class CardResponse(BaseModel):
    card_id: str
    account_id: str
    card_type: str  # e.g., DEBIT, CREDIT
    status: str     # e.g., ACTIVE, BLOCKED
    expiration_date: str

@router.get("/cards", response_model=List[CardResponse])
def get_user_cards(x_user_id: str = Header(..., alias="X-User-Id")):
    """
    Simulates retrieving a list of active cards for the user.
    Ready for future POS simulation expansions.
    """
    # Simulator Fallback: Return a mock active debit card tied to their JIT account
    safe_id_suffix = (x_user_id + "000000")[-6:]
    return [
        {
            "card_id": f"CARD-{safe_id_suffix}-01",
            "account_id": f"SIM-ACC-{safe_id_suffix}",
            "card_type": "DEBIT",
            "status": "ACTIVE",
            "expiration_date": "12/29"
        }
    ]

@router.post("/cards/{id}/block")
def block_card(id: str):
    """
    Simulates the blocking of a compromised debit or credit card.
    Acts as a sink for the fraud engine's automated mitigation actions.
    """
    # In the future, you would query a Card database model here and set status="BLOCKED"
    
    return {
        "status": "SUCCESS",
        "card_id": id,
        "message": "Card permanently blocked. Replacement dispatched to user address."
    }