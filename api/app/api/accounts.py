import uuid
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy import or_
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.domain import Account, Beneficiary, Transaction
from app.schemas.domain import AccountBalanceResponse, BeneficiaryCreate, BeneficiaryResponse, TransactionResponse, BeneficiaryUpdate
from typing import List

router = APIRouter(prefix="/api", tags=["Accounts & Beneficiaries"])

@router.get("/accounts", response_model=List[AccountBalanceResponse])
def list_user_accounts(
    x_user_id: str = Header(..., alias="X-User-Id"),
    db: Session = Depends(get_db)
):
    """
    Retrieves all accounts owned by the authenticated user.
    """
    accounts = db.query(Account).filter(Account.customer_id == x_user_id).all()
    
    # Simulator Fallback: If no accounts are found, return a JIT mock account
    # This prevents the simulator dashboard from crashing on first load.
    if not accounts:
        # Fallback padding to ensure slicing doesn't fail on very short IDs
        safe_id_suffix = (x_user_id + "000000")[-6:]
        return [{
            "account_id": f"SIM-ACC-{safe_id_suffix}",
            "balance": Decimal("5432.10"),
            "currency": "USD",
            "status": "ACTIVE"
        }]
    
    return accounts

@router.get("/accounts/{id}", response_model=AccountBalanceResponse)
def get_account(id: str, db: Session = Depends(get_db)):
    account = db.query(Account).filter(Account.account_id == id).first()
    
    # Simulator Fallback
    if not account:
        return {
            "account_id": id,
            "balance": Decimal("5432.10"),
            "currency": "USD",
            "status": "ACTIVE"
        }
    return account

@router.get("/accounts/{id}/balance", response_model=AccountBalanceResponse)
def get_balance(id: str, db: Session = Depends(get_db)):
    account = db.query(Account).filter(Account.account_id == id).first()
    
    # Simulator Fallback
    if not account:
        return {
            "account_id": id,
            "balance": Decimal("5432.10"),
            "currency": "USD",
            "status": "ACTIVE"
        }
    return account

@router.get("/accounts/{id}/transactions", response_model=List[TransactionResponse])
def get_account_transactions(id: str, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    account = db.query(Account).filter(Account.account_id == id).first()

    # Simulator Fallback: If the account hasn't been JIT created yet, return an empty ledger
    if not account:
        return []

    # Fetch transactions where this account is either the sender or receiver
    transactions = (
        db.query(Transaction)
        .filter(
            or_(
                Transaction.from_account == id,
                Transaction.to_account == id
            )
        )
        .order_by(Transaction.timestamp.desc()) # Newest first
        .offset(skip)
        .limit(limit)
        .all()
    )

    return transactions

@router.post("/beneficiaries", response_model=BeneficiaryResponse)
def add_beneficiary(
    payload: BeneficiaryCreate, 
    x_user_id: str = Header(..., alias="X-User-Id"), 
    db: Session = Depends(get_db)
):
    beneficiary = Beneficiary(
        beneficiary_id=str(uuid.uuid4()),
        customer_id=x_user_id,  # Now dynamically uses the header passed by the ScenarioManager
        account_number=payload.account_number,
        bank_name=payload.bank_name
    )
    db.add(beneficiary)
    db.commit()
    db.refresh(beneficiary)
    return beneficiary

@router.put("/beneficiaries/{beneficiary_id}", response_model=BeneficiaryResponse)
def update_beneficiary(
    beneficiary_id: str,
    payload: BeneficiaryUpdate,
    x_user_id: str = Header(..., alias="X-User-Id"),
    db: Session = Depends(get_db)
):
    """
    Updates an existing beneficiary. Monitored for ATO fraud.
    """
    beneficiary = db.query(Beneficiary).filter(
        Beneficiary.beneficiary_id == beneficiary_id,
        Beneficiary.customer_id == x_user_id
    ).first()

    if not beneficiary:
        raise HTTPException(status_code=404, detail="Beneficiary not found or access denied.")

    if payload.account_number:
        beneficiary.account_number = payload.account_number
    if payload.bank_name:
        beneficiary.bank_name = payload.bank_name

    db.commit()
    db.refresh(beneficiary)
    return beneficiary

@router.delete("/beneficiaries/{beneficiary_id}")
def delete_beneficiary(
    beneficiary_id: str,
    x_user_id: str = Header(..., alias="X-User-Id"),
    db: Session = Depends(get_db)
):
    """
    Removes a beneficiary from the user's account.
    """
    beneficiary = db.query(Beneficiary).filter(
        Beneficiary.beneficiary_id == beneficiary_id,
        Beneficiary.customer_id == x_user_id
    ).first()

    if not beneficiary:
        raise HTTPException(status_code=404, detail="Beneficiary not found or access denied.")

    db.delete(beneficiary)
    db.commit()
    return {"status": "SUCCESS", "message": "Beneficiary deleted successfully."}