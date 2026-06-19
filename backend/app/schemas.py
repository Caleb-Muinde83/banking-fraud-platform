from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from decimal import Decimal
from datetime import datetime

# --- AUTH SCHEMAS ---
class LoginRequest(BaseModel):
    username: str  # maps to customer email or username identifier
    password: str
    device_id: Optional[str] = None
    device_type: Optional[str] = None
    ip_address: Optional[str] = None
    country: Optional[str] = None

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

# --- CUSTOMER & ACCOUNT SCHEMAS ---
class AccountBalanceResponse(BaseModel):
    account_id: str
    balance: Decimal
    currency: str
    status: str

    class Config:
        from_attributes = True

# --- TRANSFER SCHEMAS ---
class TransferRequest(BaseModel):
    from_account: str
    to_account: str
    amount: Decimal = Field(..., gt=0, max_digits=15, decimal_places=2)
    currency: str = "USD"
    transaction_type: str = "TRANSFER"

class TransactionResponse(BaseModel):
    transaction_id: str
    from_account: str
    to_account: str
    amount: Decimal
    currency: str
    transaction_type: str
    timestamp: datetime

    class Config:
        from_attributes = True

# --- BENEFICIARY SCHEMAS ---
class BeneficiaryCreate(BaseModel):
    account_number: str
    bank_name: str

class BeneficiaryResponse(BaseModel):
    beneficiary_id: str
    customer_id: str
    account_number: str
    bank_name: str
    created_at: datetime

    class Config:
        from_attributes = True

# --- EMPLOYEE OPERATIONS SCHEMAS ---
class EmployeeViewAccountRequest(BaseModel):
    customer_id: str

class EmployeeUpdateAccountRequest(BaseModel):
    account_id: str
    status: str  # e.g., SUSPENDED, FROZEN, ACTIVE