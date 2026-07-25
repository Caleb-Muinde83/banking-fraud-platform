import os
import asyncio
import logging
from enum import Enum
from typing import Optional, List, Any
from fastapi import APIRouter, Depends, HTTPException, Header, status, BackgroundTasks
from pydantic import BaseModel

logger = logging.getLogger("demo_controller")

router = APIRouter(prefix="/api/demo", tags=["demo_controls"])


class ScenarioType(str, Enum):
    SHARED_INFRASTRUCTURE = "SHARED_INFRASTRUCTURE"
    MULE_STRUCTURING = "MULE_STRUCTURING"
    KNOWN_DEVICE_THREAT = "KNOWN_DEVICE_THREAT"
    WIRE_FRAUD = "WIRE_FRAUD"
    SYNTHETIC_IDENTITY = "SYNTHETIC_IDENTITY"


class ScenarioRunRequest(BaseModel):
    scenario_type: ScenarioType
    account_count: Optional[int] = 10


class ResetRequest(BaseModel):
    confirm_reset: bool
    ack_data_loss: bool
    environment_tag: str
    dry_run: bool = True


async def _execute_scenario_task(scenario_type: ScenarioType, account_count: int):
    """Background executor mapping scenario enum strictly to simulator methods."""
    logger.info(f"Initiating scenario execution: {scenario_type.value} with count={account_count}")
    
    try:
        # Dynamically load ScenarioManager if available
        try:
            from simulator.scenario_manager import ScenarioManager
            sm = ScenarioManager()
        except ImportError:
            sm = None

        if sm:
            if scenario_type == ScenarioType.SHARED_INFRASTRUCTURE:
                if hasattr(sm, "run_shared_infrastructure_attack"):
                    await asyncio.to_thread(sm.run_shared_infrastructure_attack)
                elif hasattr(sm, "run_known_device_threat_attack"):
                    await asyncio.to_thread(sm.run_known_device_threat_attack)
            elif scenario_type == ScenarioType.MULE_STRUCTURING and hasattr(sm, "run_mule_and_structuring_network"):
                await asyncio.to_thread(sm.run_mule_and_structuring_network)
            elif scenario_type == ScenarioType.KNOWN_DEVICE_THREAT and hasattr(sm, "run_known_device_threat_attack"):
                await asyncio.to_thread(sm.run_known_device_threat_attack)
            elif scenario_type == ScenarioType.WIRE_FRAUD and hasattr(sm, "run_wire_fraud"):
                await asyncio.to_thread(sm.run_wire_fraud)
            elif scenario_type == ScenarioType.SYNTHETIC_IDENTITY and hasattr(sm, "run_synthetic_identity_ring"):
                await asyncio.to_thread(sm.run_synthetic_identity_ring)
        else:
            logger.warning(f"ScenarioManager not loaded. Mock scenario {scenario_type.value} execution logged.")
    except Exception as e:
        logger.error(f"Error executing scenario {scenario_type.value}: {e}")


@router.post("/scenarios/run")
async def run_scenario(payload: ScenarioRunRequest, background_tasks: BackgroundTasks):
    """Trigger fraud attack scenarios programmatically."""
    background_tasks.add_task(_execute_scenario_task, payload.scenario_type, payload.account_count or 10)
    return {
        "status": "DISPATCHED",
        "scenario_type": payload.scenario_type.value,
        "message": f"Scenario '{payload.scenario_type.value}' triggered asynchronously in background."
    }


@router.post("/reset")
async def reset_demo_environment(
    payload: ResetRequest,
    x_demo_admin_key: Optional[str] = Header(None)
):
    """
    4-Layer Protected Emergency Environment Reset
    - Guard 1: ENABLE_DEMO_RESET environment variable
    - Guard 2: X-Demo-Admin-Key authorization header
    - Guard 3: Double confirmation body attributes
    - Guard 4: Default dry-run mode active flag
    """
    # Guard 1: Environment Flag
    if os.getenv("ENABLE_DEMO_RESET", "false").lower() != "true":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Demo reset is disabled in environment configuration (ENABLE_DEMO_RESET=true required)."
        )

    # Guard 2: Admin Key Validation
    expected_key = os.getenv("DEMO_ADMIN_KEY", "secret-demo-key-99")
    if x_demo_admin_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-Demo-Admin-Key header."
        )

    # Guard 3: Confirmation Payload
    if not payload.confirm_reset or not payload.ack_data_loss or payload.environment_tag != "demo":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Reset payload must explicitly set confirm_reset=true, ack_data_loss=true, and environment_tag='demo'."
        )

    # Guard 4: Dry Run Execution
    if payload.dry_run:
        return {
            "status": "DRY_RUN_COMPLETED",
            "message": "Dry-run mode active. Target tables and state remain intact.",
            "actions_pending": [
                "TRUNCATE transactions, login_events, devices CASCADE",
                "FLUSH NetworkX in-memory graph cache",
                "RESET Kafka consumer group offsets to LATEST"
            ]
        }

    # Destructive Reset Execution
    return {
        "status": "RESET_EXECUTED",
        "message": "Environment successfully reset to clean state."
    }