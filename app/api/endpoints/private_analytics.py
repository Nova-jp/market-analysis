
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, Dict, Any
from app.services.private_analysis_service import PrivateAnalysisService
from app.api.deps import get_current_username
import logging

router = APIRouter(dependencies=[Depends(get_current_username)])
logger = logging.getLogger(__name__)
service = PrivateAnalysisService()

@router.get("/forward-curve")
async def get_forward_curve(date: str = Query(..., description="Target date (YYYY-MM-DD)")):
    """Get 1Y Forward and Spot curve data for a specific date."""
    try:
        result = await service.get_forward_curve_data(date)
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    except Exception as e:
        logger.error(f"Error in get_forward_curve: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pca")
async def get_private_pca(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    components: int = Query(3, ge=1, le=5)
):
    """Perform PCA on swap forward grid for the given range."""
    try:
        result = await service.run_pca_analysis(start_date, end_date, components)
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    except Exception as e:
        logger.error(f"Error in get_private_pca: {e}")
        raise HTTPException(status_code=500, detail=str(e))
