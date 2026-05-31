import logging
from fastapi import APIRouter, HTTPException, Path, Query
from starlette.concurrency import run_in_threadpool

from core.db.async_client import db_manager
from core.calculations.bond_math import QuantLibHelper
from core.models.schemas import InstantaneousForwardResponse, validate_date_format

logger = logging.getLogger(__name__)

router = APIRouter()

_MIN_OIS_POINTS = 4


def _calc_sync(date: str, ois_data: list, max_years: float, num_points: int) -> list:
    helper = QuantLibHelper(date)
    helper.build_ois_curve(ois_data)
    return helper.calculate_instantaneous_forward_curve(max_years=max_years, num_points=num_points)


@router.get("/api/instantaneous-forward/{date}", response_model=InstantaneousForwardResponse)
async def get_instantaneous_forward(
    date: str = Path(..., description="対象日付 (YYYY-MM-DD)"),
    max_years: float = Query(10.0, ge=1.0, le=40.0, description="グリッド上限年数"),
    num_points: int = Query(240, ge=50, le=600, description="サンプリング点数"),
):
    """OIS カーブから瞬間フォワードレートとゼロレートを返す。"""
    if not validate_date_format(date):
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    result = await db_manager.get_irs_data(trade_date=date, product_type="OIS", limit=100)
    if not result["success"] or not result["data"]:
        return InstantaneousForwardResponse(
            date=date, data=[], error=f"No OIS data for {date}"
        )

    ois_data = [r for r in result["data"] if r.get("rate") is not None]
    if len(ois_data) < _MIN_OIS_POINTS:
        return InstantaneousForwardResponse(
            date=date, data=[], error=f"Insufficient OIS data for {date} ({len(ois_data)} points)"
        )

    try:
        points = await run_in_threadpool(_calc_sync, date, ois_data, max_years, num_points)
        return InstantaneousForwardResponse(date=date, data=points)
    except Exception as e:
        logger.error(f"instantaneous_forward error for {date}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
