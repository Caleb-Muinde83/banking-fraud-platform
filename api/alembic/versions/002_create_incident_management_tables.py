"""create incident management tables

Revision ID: 002_incidents
Revises: None
Create Date: 2026-07-22
"""
from alembic import op
import sqlalchemy as sqa
from sqlalchemy.dialects import postgresql

# Revision identifiers, used by Alembic.
revision = '002_incidents'
down_revision = None

def upgrade() -> None:
    # Define PostgreSQL Enums
    incident_status = postgresql.ENUM(
        'OPEN', 'INVESTIGATING', 'FALSE_POSITIVE', 'CONFIRMED_FRAUD', 'RESOLVED_OTHER', 
        name='incidentstatus'
    )
    incident_severity = postgresql.ENUM(
        'LOW', 'MEDIUM', 'HIGH', 'CRITICAL', 
        name='incidentseverity'
    )
    
    # NOTE: We do NOT call .create() manually here. 
    # SQLAlchemy automatically creates the types when evaluating op.create_table.

    # Create master incidents table
    op.create_table(
        'incidents',
        sqa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sqa.Column('title', sqa.String(length=255), nullable=False),
        sqa.Column('identity_key', sqa.String(length=255), nullable=False),
        sqa.Column('status', incident_status, nullable=False, server_default='OPEN'),
        sqa.Column('severity', incident_severity, nullable=False, server_default='MEDIUM'),
        sqa.Column('alert_count', sqa.Integer(), nullable=False, server_default='1'),
        sqa.Column('max_risk_score', sqa.Float(), nullable=False, server_default='0.0'),
        sqa.Column('assigned_to', sqa.String(length=100), nullable=True),
        sqa.Column('first_alert_at', sqa.DateTime(timezone=True), nullable=False),
        sqa.Column('last_alert_at', sqa.DateTime(timezone=True), nullable=False),
        sqa.Column('created_at', sqa.DateTime(timezone=True), server_default=sqa.text('now()'), nullable=False),
        sqa.Column('updated_at', sqa.DateTime(timezone=True), server_default=sqa.text('now()'), nullable=False)
    )
    
    # Indexes for incidents performance optimization
    op.create_index(op.f('ix_incidents_identity_key'), 'incidents', ['identity_key'], unique=False)
    op.create_index(op.f('ix_incidents_status'), 'incidents', ['status'], unique=False)
    op.create_index(op.f('ix_incidents_severity'), 'incidents', ['severity'], unique=False)

    # Create secondary alerts table
    op.create_table(
        'incident_alerts',
        sqa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sqa.Column('incident_id', postgresql.UUID(as_uuid=True), sqa.ForeignKey('incidents.id', ondelete='CASCADE'), nullable=False),
        sqa.Column('alert_id', sqa.String(length=255), nullable=False),
        sqa.Column('indicator', sqa.String(length=100), nullable=False),
        sqa.Column('risk_score', sqa.Float(), nullable=False),
        sqa.Column('source_topic', sqa.String(length=100), nullable=False),
        sqa.Column('raw_payload', postgresql.JSONB(astext_type=sqa.Text()), nullable=False),
        sqa.Column('received_at', sqa.DateTime(timezone=True), nullable=False)
    )
    op.create_index(op.f('ix_incident_alerts_alert_id'), 'incident_alerts', ['alert_id'], unique=False)

    # Create history audit logs table
    op.create_table(
        'incident_audit_logs',
        sqa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sqa.Column('incident_id', postgresql.UUID(as_uuid=True), sqa.ForeignKey('incidents.id', ondelete='CASCADE'), nullable=False),
        sqa.Column('previous_status', sqa.String(length=50), nullable=True),
        sqa.Column('new_status', sqa.String(length=50), nullable=False),
        sqa.Column('changed_by', sqa.String(length=100), nullable=False),
        sqa.Column('notes', sqa.Text(), nullable=True),
        sqa.Column('timestamp', sqa.DateTime(timezone=True), server_default=sqa.text('now()'), nullable=False)
    )

def downgrade() -> None:
    # Drop tables in exact reverse dependency order
    op.drop_table('incident_audit_logs')
    op.drop_table('incident_alerts')
    op.drop_table('incidents')
    
    # Safely drop PostgreSQL custom types using explicit names
    incident_status = postgresql.ENUM(name='incidentstatus')
    incident_severity = postgresql.ENUM(name='incidentseverity')
    incident_status.drop(op.get_bind(), checkfirst=True)
    incident_severity.drop(op.get_bind(), checkfirst=True)