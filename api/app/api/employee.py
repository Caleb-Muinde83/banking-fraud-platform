import uuid
import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.domain import EmployeeAction, Account
from app.schemas.domain import EmployeeViewAccountRequest, EmployeeUpdateAccountRequest

router = APIRouter(prefix="/api/employee", tags=["Employee Internal Operations"])

@router.post("/view-account")
def employee_view_account(payload: EmployeeViewAccountRequest, db: Session = Depends(get_db)):
    # Log internal access pattern to audit logs for threat monitoring
    audit_action = EmployeeAction(
        action_id=str(uuid.uuid4()),
        employee_id="emp_system_dev",
        customer_id=payload.customer_id,
        action_type="VIEW_ACCOUNT",
        timestamp=datetime.datetime.utcnow()
    )
    db.add(audit_action)
    db.commit()
    return {"status": "SUCCESS", "message": f"Account data accessed for {payload.customer_id}"}

@router.post("/update-account")
def employee_update_account(payload: EmployeeUpdateAccountRequest, db: Session = Depends(get_db)):
    account = db.query(Account).filter(Account.account_id == payload.account_id).first()
    
    # Gracefully skip instead of throwing a 404 error if the JIT account doesn't exist yet
    if not account:
        return {"status": "SKIPPED", "message": f"Account {payload.account_id} not yet initialized. Maintenance skipped."}
    
    account.status = payload.status
    
    audit_action = EmployeeAction(
        action_id=str(uuid.uuid4()),
        employee_id="emp_system_dev",
        customer_id=account.customer_id,
        action_type=f"SET_STATUS_{payload.status}",
        timestamp=datetime.datetime.utcnow()
    )
    db.add(audit_action)
    db.commit()
    return {"status": "SUCCESS", "message": f"Account status modified to {payload.status}"}