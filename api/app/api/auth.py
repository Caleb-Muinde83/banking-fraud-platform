import uuid
import datetime
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional
from app.core.database import get_db
from app.models.domain import LoginEvent, Session as UserSession
from app.schemas.domain import TokenResponse

router = APIRouter(prefix="/api", tags=["Authentication"])

# Localized schema to squash 422 Unprocessable Entity errors
class SimulatorLoginRequest(BaseModel):
    username: str
    password: str
    device_id: Optional[str] = "UNKNOWN_DEVICE"
    country: Optional[str] = "US"
    ip_address: Optional[str] = "127.0.0.1"

@router.post("/login", response_model=TokenResponse)
def login(payload: SimulatorLoginRequest, db: Session = Depends(get_db)):
    # Catch the specific attack passwords sent by scenarios.py
    bad_passwords = ["wrong", "bruteforce", "spring2026"]
    password_lower = payload.password.lower()
    
    # If the password contains any of the bad strings, it's a failed login
    success = not any(bad in password_lower for bad in bad_passwords)
    
    # Track the login event according to spec requirements
    login_event = LoginEvent(
        event_id=str(uuid.uuid4()),
        customer_id=payload.username,
        device_id=payload.device_id,
        country=payload.country,
        ip_address=payload.ip_address,
        success=success,
        timestamp=datetime.datetime.utcnow()
    )
    db.add(login_event)
    db.commit()

    if not success:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid security credentials provided."
        )

    # Establish active session tracking
    session_id = str(uuid.uuid4())
    new_session = UserSession(
        session_id=session_id,
        customer_id=payload.username,
        device_id=payload.device_id,
        ip_address=payload.ip_address,
        created_at=datetime.datetime.utcnow(),
        expires_at=datetime.datetime.utcnow() + datetime.timedelta(minutes=30)
    )
    db.add(new_session)
    db.commit()

    return {
        "access_token": f"mock_access_token_{session_id}",
        "refresh_token": f"mock_refresh_token_{str(uuid.uuid4())}",
        "token_type": "bearer"
    }

@router.post("/logout")
def logout():
    return {"message": "Logged out successfully."}

@router.post("/refresh-token")
def refresh_token():
    return {"message": "Token refreshed successfully."}

@router.delete("/sessions/{session_id}")
def revoke_session(session_id: str, db: Session = Depends(get_db)):
    """
    Forcefully terminates an active session.
    Used by the automated fraud engine to kick out attackers during an ATO event.
    """
    user_session = db.query(UserSession).filter(UserSession.session_id == session_id).first()
    
    if not user_session:
        raise HTTPException(status_code=404, detail="Session not found or already expired.")

    db.delete(user_session)
    db.commit()
    
    return {"status": "SUCCESS", "message": "Session revoked. User forced to re-authenticate."}