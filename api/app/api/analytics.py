from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.analytics import MetricsResponse
from app.services.analytics_service import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["Analytics & Model Evaluation"])


@router.get("/metrics", response_model=MetricsResponse)
async def get_analytics_metrics(db: AsyncSession = Depends(get_db)):
    """
    Fetch system-wide model performance metrics, ground truth labels, 
    and analyst feedback analytics.
    """
    return await AnalyticsService.compute_metrics(db)