import asyncio
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.graph import GraphResponse, RingDetectionResponse
from app.services.graph_builder import GraphBuilderService
from app.services.ring_detector import RingDetectorService

router = APIRouter(prefix="/api/graph", tags=["Entity Graph & Ring Detection"])


@router.get("/entity/{entity_id}", response_model=GraphResponse)
async def get_entity_subgraph(
    entity_id: str,
    depth: int = Query(2, ge=1, le=4, description="Graph traversal depth (1 to 4 hops)"),
    lookback_hours: int = Query(24, ge=1, le=168, description="Time window in hours for transactions and logins"),
    db: AsyncSession = Depends(get_db)
):
    """
    Fetch an ego-centric subgraph centered around a target entity ID 
    (e.g., CUST-101, ACCT-502, DEV-f8a1, IP-192.168.1.5).
    
    Database querying runs async via asyncpg, and graph traversal is offloaded
    to a background CPU thread to preserve API responsiveness.
    """
    # 1. Query database asynchronously and construct graph
    G = await GraphBuilderService.build_graph(db, lookback_hours=lookback_hours)

    if not G.has_node(entity_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Entity '{entity_id}' not found in relationship graph for the last {lookback_hours} hours."
        )

    # 2. Offload CPU-bound NetworkX traversal to thread pool
    subgraph_schema = await asyncio.to_thread(
        GraphBuilderService.extract_subgraph_schema,
        G,
        center_entity_id=entity_id,
        depth=depth
    )

    return subgraph_schema


@router.get("/rings/detect", response_model=RingDetectionResponse)
async def detect_fraud_rings(
    lookback_hours: int = Query(24, ge=1, le=168, description="Analysis time window in hours"),
    max_hop_length: int = Query(6, ge=3, le=10, description="Strict upper limit on cycle length to bound search complexity"),
    min_shared_customers: int = Query(3, ge=2, le=20, description="Minimum customer degree to trigger shared infrastructure alert"),
    db: AsyncSession = Depends(get_db)
):
    """
    Scan entity relationship structures for deliberate fraud rings:
    1. Bounded circular transfer paths (3 <= cycle_length <= max_hop_length).
    2. Shared infrastructure anomalies (Devices/IPs shared across unlinked customers).
    
    Executes depth-limited cycle searching in a thread pool via asyncio.to_thread().
    """
    # 1. Query database asynchronously and construct graph
    G = await GraphBuilderService.build_graph(db, lookback_hours=lookback_hours)

    # 2. Offload CPU-bound detection algorithms to thread pool
    detection_results = await asyncio.to_thread(
        RingDetectorService.analyze_graph,
        G,
        max_hop_length=max_hop_length,
        min_shared_customers=min_shared_customers
    )

    return detection_results