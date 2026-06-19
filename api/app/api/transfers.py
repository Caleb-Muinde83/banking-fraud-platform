import uuid
import datetime
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db

# Make sure to import your Customer model here!
from app.models.domain import Account, Transaction, Customer 
from app.schemas.domain import TransferRequest, TransactionResponse

router = APIRouter(prefix="/api", tags=["Transfers"])

def _ensure_account_exists(db: Session, account_id: str, currency: str) -> Account:
    """
    Just-In-Time (JIT) creation for simulated records.
    Resolves cascading Foreign Key constraints (Customer -> Account -> Transaction).
    """
    acc = db.query(Account).filter(Account.account_id == account_id).first()
    if not acc:
        customer_id = f"SIM-CUST-{account_id[-6:]}"
        
        # 1. JIT Create the Customer first to satisfy Account's FK constraint
        cust = db.query(Customer).filter(Customer.customer_id == customer_id).first()
        if not cust:
            cust = Customer(
                customer_id=customer_id,
                first_name="Simulated",
                last_name="User",
                email=f"{customer_id.lower()}@simulator.local",
                country="US", # Satisfies the NOT NULL constraint on the country column
            )
            db.add(cust)
            db.flush() # Push to DB so the Account insert works

        # 2. JIT Create the Account to satisfy the Transaction's FK constraint
        acc = Account(
            account_id=account_id,
            customer_id=customer_id,
            balance=Decimal("50000.00"),
            currency=currency,
            status="ACTIVE"
        )
        db.add(acc)
        db.flush() # Push to DB so the Transaction commit works
        
    return acc

@router.post("/transfers", response_model=TransactionResponse)
def execute_transfer(payload: TransferRequest, db: Session = Depends(get_db)):
    # 1. Look up ledgers, JIT creating the whole customer/account tree if needed
    source_acc = _ensure_account_exists(db, payload.from_account, payload.currency)
    target_acc = _ensure_account_exists(db, payload.to_account, payload.currency)

    # 2. Auto-replenish simulation funds if they are about to go broke
    if source_acc.balance < payload.amount:
        source_acc.balance += Decimal("100000.00")  # Magic simulator money!

    # 3. Deduct balance from origin account and credit target
    source_acc.balance -= payload.amount
    target_acc.balance += payload.amount

    # 4. Commit transaction audit row
    txn = Transaction(
        transaction_id=str(uuid.uuid4()),
        from_account=payload.from_account,
        to_account=payload.to_account,
        amount=payload.amount,
        currency=payload.currency,
        transaction_type=payload.transaction_type,
        timestamp=datetime.datetime.utcnow()
    )
    
    db.add(txn)
    db.commit()
    db.refresh(txn)
    return txn

@router.get("/transfers/{transaction_id}", response_model=TransactionResponse)
def get_transaction(transaction_id: str, db: Session = Depends(get_db)):
    """
    Retrieve the exact details of a specific transaction.
    Crucial for fraud analysts investigating a flagged transaction ID.
    """
    transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found.")
        
    return transaction