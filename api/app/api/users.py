from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session

# Import your database session and models
from app.core.database import get_db
from app.models.domain import Customer

router = APIRouter(prefix="/api", tags=["Users & MFA"])

# --- Local schemas for incoming simulator payloads ---
class MFARequest(BaseModel):
    username: str
    device_id: Optional[str] = None
    action: Optional[str] = None

class MFAVerifyRequest(BaseModel):
    username: str
    code: str

class UpdateProfileRequest(BaseModel):
    username: str
    email: Optional[str] = None
    phone_number: Optional[str] = None
    address: Optional[str] = None

class UserProfileResponse(BaseModel):
    customer_id: str
    first_name: Optional[str] = "Simulated"
    last_name: Optional[str] = "User"
    email: Optional[str] = None
    phone_number: Optional[str] = None
    address: Optional[str] = None
    country: Optional[str] = "US"

# --- MFA Endpoints ---
@router.post("/mfa/request")
def request_mfa(payload: MFARequest):
    return {
        "message": "MFA challenge issued successfully.", 
        "status": "pending",
        "challenge_id": "mfa_mock_12345"
    }

@router.post("/mfa/verify")
def verify_mfa(payload: MFAVerifyRequest):
    """
    Validates an OTP sent to the user.
    For simulation purposes, the valid code is hardcoded to '123456'.
    """
    # Simulator Logic: Accept a static code for testing
    if payload.code == "123456":
        return {"status": "SUCCESS", "message": "MFA verified successfully. Action unblocked."}
    
    raise HTTPException(status_code=401, detail="Invalid or expired MFA code.")

# --- User Profile Endpoints ---
@router.get("/user/profile", response_model=UserProfileResponse)
def get_profile(
    x_user_id: str = Header(..., alias="X-User-Id"), 
    db: Session = Depends(get_db)
):
    """
    Retrieves the current user's profile information.
    Uses the X-User-Id header to identify the authenticated customer.
    """
    customer = db.query(Customer).filter(Customer.customer_id == x_user_id).first()
    
    # Simulator Fallback: If the user hasn't been JIT created yet, return mock data
    if not customer:
        return {
            "customer_id": x_user_id,
            "email": f"{x_user_id.lower()}@simulator.local"
        }
        
    return customer

@router.post("/user/update-profile")
def update_profile(
    payload: UpdateProfileRequest, 
    db: Session = Depends(get_db)
):
    """
    Updates the user's profile details. A real fraud engine will track these changes.
    """
    customer = db.query(Customer).filter(Customer.customer_id == payload.username).first()
    
    if customer:
        if payload.email: customer.email = payload.email
        if payload.phone_number: customer.phone_number = payload.phone_number
        db.commit()
        
    return {"message": "User profile updated successfully."}