import datetime
from sqlalchemy import Column, String, Integer, Numeric, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base

class Customer(Base):
    __tablename__ = "customers"
    customer_id = Column(String, primary_key=True, index=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    country = Column(String, nullable=False)
    risk_level = Column(String, default="LOW")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class Account(Base):
    __tablename__ = "accounts"
    account_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    balance = Column(Numeric(15, 2), default=0.00)
    currency = Column(String, default="USD")
    status = Column(String, default="ACTIVE")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class Beneficiary(Base):
    __tablename__ = "beneficiaries"
    beneficiary_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    account_number = Column(String, nullable=False)
    bank_name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class Transaction(Base):
    __tablename__ = "transactions"
    transaction_id = Column(String, primary_key=True, index=True)
    from_account = Column(String, ForeignKey("accounts.account_id"), nullable=False)
    to_account = Column(String, nullable=False)
    amount = Column(Numeric(15, 2), nullable=False)
    currency = Column(String, default="USD")
    transaction_type = Column(String, nullable=False)  # e.g., TRANSFER, DEPOSIT, WITHDRAWAL
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class LoginEvent(Base):
    __tablename__ = "login_events"
    event_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    device_id = Column(String, nullable=False)
    country = Column(String, nullable=False)
    ip_address = Column(String, nullable=False)
    success = Column(Boolean, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class Device(Base):
    __tablename__ = "devices"
    device_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    device_type = Column(String, nullable=False)
    first_seen = Column(DateTime, default=datetime.datetime.utcnow)
    risk_score = Column(Integer, default=0)

class Session(Base):
    __tablename__ = "sessions"
    session_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=False)
    device_id = Column(String, nullable=False)
    ip_address = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)

class Employee(Base):
    __tablename__ = "employees"
    employee_id = Column(String, primary_key=True, index=True)
    department = Column(String, nullable=False)
    role = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class EmployeeAction(Base):
    __tablename__ = "employee_actions"
    action_id = Column(String, primary_key=True, index=True)
    employee_id = Column(String, ForeignKey("employees.employee_id"), nullable=False)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=True)
    action_type = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class Alert(Base):
    __tablename__ = "alerts"
    alert_id = Column(String, primary_key=True, index=True)
    alert_type = Column(String, nullable=False)
    severity = Column(String, nullable=False)
    customer_id = Column(String, ForeignKey("customers.customer_id"), nullable=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class Case(Base):
    __tablename__ = "cases"
    case_id = Column(String, primary_key=True, index=True)
    alert_id = Column(String, ForeignKey("alerts.alert_id"), nullable=False)
    status = Column(String, default="OPEN")
    assignee = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)

class AuditEvent(Base):
    __tablename__ = "audit_events"
    audit_id = Column(String, primary_key=True, index=True)
    actor_type = Column(String, nullable=False)
    actor_id = Column(String, nullable=False)
    action = Column(String, nullable=False)
    resource = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)