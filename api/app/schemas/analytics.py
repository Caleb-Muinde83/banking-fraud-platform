from typing import Optional, Dict
from pydantic import BaseModel, Field


class MetricsResponse(BaseModel):
    total_incidents: int = Field(..., description="Total number of incidents created")
    open_count: int = Field(..., description="Count of OPEN incidents")
    investigating_count: int = Field(..., description="Count of INVESTIGATING incidents")
    confirmed_fraud_count: int = Field(..., description="Ground truth: CONFIRMED_FRAUD")
    false_positive_count: int = Field(..., description="Ground truth: FALSE_POSITIVE")
    
    # Model evaluation metrics
    precision: float = Field(..., description="Precision = Confirmed Fraud / (Confirmed Fraud + False Positives)")
    false_positive_rate: float = Field(..., description="FPR = False Positives / Total Resolved Incidents")
    
    # Operational metrics
    avg_resolution_time_seconds: Optional[float] = Field(
        None, description="Average time taken from creation to resolution (seconds)"
    )
    
    # Aggregation breakdowns
    severity_counts: Dict[str, int] = Field(..., description="Incident breakdown by severity level")

    class Config:
        from_attributes = True