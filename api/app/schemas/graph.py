from enum import Enum
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class NodeType(str, Enum):
    CUSTOMER = "CUSTOMER"
    ACCOUNT = "ACCOUNT"
    DEVICE = "DEVICE"
    IP = "IP"


class EdgeType(str, Enum):
    OWNS = "OWNS"
    TRANSFERRED_TO = "TRANSFERRED_TO"
    USED_DEVICE = "USED_DEVICE"
    LOGGED_IN_FROM = "LOGGED_IN_FROM"


class NodeSchema(BaseModel):
    id: str = Field(
        ..., 
        description="Unique entity identifier (e.g., CUST-101, ACCT-502, DEV-f8a1, IP-192.168.1.5)", 
        example="CUST-101"
    )
    label: NodeType = Field(..., description="Entity type classification")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Entity attributes & metadata")


class EdgeSchema(BaseModel):
    source: str = Field(..., description="Source node ID")
    target: str = Field(..., description="Target node ID")
    relationship: EdgeType = Field(..., description="Type of relationship linking source and target")
    properties: Dict[str, Any] = Field(
        default_factory=dict, 
        description="Edge attributes (e.g., amount, timestamp, frequency)"
    )


class GraphResponse(BaseModel):
    nodes: List[NodeSchema] = Field(default_factory=list, description="Nodes present in the subgraph")
    edges: List[EdgeSchema] = Field(default_factory=list, description="Edges linking the nodes")


class RingType(str, Enum):
    CIRCULAR_TRANSFER_RING = "CIRCULAR_TRANSFER_RING"
    SHARED_INFRASTRUCTURE_CLUSTER = "SHARED_INFRASTRUCTURE_CLUSTER"


class FraudRingAlert(BaseModel):
    ring_id: str = Field(..., description="Generated UUID/identifier for the detected ring")
    ring_type: RingType = Field(..., description="Classification of the graph anomaly")
    member_nodes: List[str] = Field(..., description="List of node IDs forming the ring or cluster")
    hop_count: int = Field(..., description="Path length or number of steps in the cycle")
    total_volume: Optional[float] = Field(
        None, 
        description="Aggregated currency volume moving through circular transfer paths"
    )
    shared_entity_id: Optional[str] = Field(
        None, 
        description="ID of shared Device or IP node triggering a cluster alert"
    )
    severity: str = Field(..., description="Assessed risk level: MEDIUM, HIGH, or CRITICAL")
    description: str = Field(..., description="Detailed explanation of why this graph pattern was flagged")


class RingDetectionResponse(BaseModel):
    detected_rings: List[FraudRingAlert] = Field(
        default_factory=list, 
        description="Collection of identified circular paths and shared infrastructure clusters"
    )
    scanned_nodes_count: int = Field(..., description="Total nodes evaluated in the graph scope")
    scanned_edges_count: int = Field(..., description="Total edges evaluated in the graph scope")
    execution_time_ms: float = Field(..., description="Total scan time in milliseconds")

    class Config:
        from_attributes = True