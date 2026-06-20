from sqlalchemy import Column, String, Float, Boolean, DateTime, ForeignKey, Numeric
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

# ==========================================
# 1. CUSTOMER & ACCOUNT ENTITIES
# ==========================================

class Customer(Base):
    __tablename__ = "customers"

    customer_id = Column(String, primary_key=True, index=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    country = Column(String, nullable=False)
    risk_level = Column(String, default="LOW")  # LOW, MEDIUM, HIGH
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    accounts = relationship("Account", back_populates="customer")
    beneficiaries = relationship("Beneficiary", back_populates="customer")
    devices = relationship("Device", back_populates="customer")
    sessions = relationship("SessionModel", back_populates="customer")


class Account(Base):
    __tablename__ = "accounts"

    account_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    balance = Column(Numeric(precision=15, scale=2), default=0.00)
    currency = Column(String, default="USD", nullable=False)
    status = Column(String, default="ACTIVE")  # ACTIVE, FROZEN, SUSPENDED
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    customer = relationship("Customer", back_populates="accounts")


# ==========================================
# 2. OPERATIONAL & TRANSACTION ENTITIES
# ==========================================

class Beneficiary(Base):
    __tablename__ = "beneficiaries"

    beneficiary_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    account_number = Column(String, nullable=False)
    bank_name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    customer = relationship("Customer", back_populates="beneficiaries")


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id = Column(String, primary_key=True, index=True)
    from_account = Column(String, nullable=False)
    to_account = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String, default="USD", nullable=False)
    transaction_type = Column(String, nullable=False) 
    # ADDED: Helps map exact transactions to hijacked environments
    device_id = Column(String, nullable=True)
    browser_fingerprint = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)


# ==========================================
# 3. SECURITY, SESSION, & DEVICE LOGGING
# ==========================================

class LoginEvent(Base):
    __tablename__ = "login_events"

    event_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, nullable=False, index=True)
    device_id = Column(String, nullable=False)
    country = Column(String, nullable=False)
    ip_address = Column(String, nullable=False)
    # ADDED: Track proxy infrastructure used during credential stuffing/spraying
    proxy_ip = Column(String, nullable=True) 
    # ADDED: Essential for tracking device hijack tracking
    browser_fingerprint = Column(String, nullable=True) 
    success = Column(Boolean, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)


class Device(Base):
    __tablename__ = "devices"

    device_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    device_type = Column(String, nullable=False)  # MOBILE, DESKTOP, TABLET
    first_seen = Column(DateTime, default=datetime.utcnow)
    risk_score = Column(Float, default=0.0)

    # Relationships
    customer = relationship("Customer", back_populates="devices")


class SessionModel(Base):
    __tablename__ = "sessions"

    session_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    device_id = Column(String, nullable=False)
    ip_address = Column(String, nullable=False)
    # ADDED: Track rapid middleware browser fingerprint transitions
    browser_fingerprint = Column(String, nullable=True) 
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)

    customer = relationship("Customer", back_populates="sessions")


# ==========================================
# 4. INTERNAL THREAT & AUDITING ENTITIES
# ==========================================

class Employee(Base):
    __tablename__ = "employees"

    employee_id = Column(String, primary_key=True, index=True)
    department = Column(String, nullable=False)
    role = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class EmployeeAction(Base):
    __tablename__ = "employee_actions"

    action_id = Column(String, primary_key=True, index=True)
    employee_id = Column(String, nullable=False, index=True)
    customer_id = Column(String, nullable=False)
    action_type = Column(String, nullable=False)  # VIEW_ACCOUNT, MODIFY_ACCOUNT
    timestamp = Column(DateTime, default=datetime.utcnow)

class Alert(Base):
    __tablename__ = "alerts"
    alert_id = Column(String, primary_key=True, index=True)
    alert_type = Column(String)
    severity = Column(String)
    customer_id = Column(String, index=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class Case(Base):
    __tablename__ = "cases"
    case_id = Column(String, primary_key=True, index=True)
    alert_id = Column(String)
    status = Column(String, default="OPEN")
    assignee = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)

class AuditEvent(Base):
    __tablename__ = "audit_events"
    audit_id = Column(String, primary_key=True, index=True)
    actor_type = Column(String)
    actor_id = Column(String)
    action = Column(String)
    resource = Column(String)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)