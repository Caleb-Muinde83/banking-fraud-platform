from typing import Dict
from sqlalchemy import select, func, extract
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.incidents import Incident, IncidentStatus, IncidentSeverity
from app.schemas.analytics import MetricsResponse


class AnalyticsService:
    """Service layer encapsulating analytics computations and database aggregations."""

    @staticmethod
    async def compute_metrics(db: AsyncSession) -> MetricsResponse:
        # 1. Fetch incident counts grouped by status
        status_stmt = select(Incident.status, func.count(Incident.id)).group_by(Incident.status)
        status_res = await db.execute(status_stmt)
        status_counts: Dict[IncidentStatus, int] = {row[0]: row[1] for row in status_res.all()}

        open_cnt = status_counts.get(IncidentStatus.OPEN, 0)
        investigating_cnt = status_counts.get(IncidentStatus.INVESTIGATING, 0)
        confirmed_cnt = status_counts.get(IncidentStatus.CONFIRMED_FRAUD, 0)
        fp_cnt = status_counts.get(IncidentStatus.FALSE_POSITIVE, 0)

        total_incidents = sum(status_counts.values())
        total_resolved = confirmed_cnt + fp_cnt

        # 2. Compute Precision & False Positive Rate (safely handle division by zero)
        precision = (confirmed_cnt / total_resolved) if total_resolved > 0 else 0.0
        fpr = (fp_cnt / total_resolved) if total_resolved > 0 else 0.0

        # 3. Fetch severity distribution
        sev_stmt = select(Incident.severity, func.count(Incident.id)).group_by(Incident.severity)
        sev_res = await db.execute(sev_stmt)
        severity_counts = {
            (row[0].value if hasattr(row[0], "value") else str(row[0])): row[1]
            for row in sev_res.all()
        }

        # 4. Calculate Average Resolution Time (seconds) for terminal cases (CONFIRMED_FRAUD / FALSE_POSITIVE)
        res_time_stmt = select(
            func.avg(extract("epoch", Incident.updated_at - Incident.created_at))
        ).where(
            Incident.status.in_([IncidentStatus.CONFIRMED_FRAUD, IncidentStatus.FALSE_POSITIVE])
        )
        res_time_res = await db.execute(res_time_stmt)
        avg_res_seconds = res_time_res.scalar()

        return MetricsResponse(
            total_incidents=total_incidents,
            open_count=open_cnt,
            investigating_count=investigating_cnt,
            confirmed_fraud_count=confirmed_cnt,
            false_positive_count=fp_cnt,
            precision=round(precision, 4),
            false_positive_rate=round(fpr, 4),
            avg_resolution_time_seconds=round(avg_res_seconds, 2) if avg_res_seconds is not None else None,
            severity_counts=severity_counts
        )