"""
Cloud Scheduler用データ収集API
毎日18:00にCloud Schedulerから呼び出される
"""
import logging
from fastapi import APIRouter, HTTPException, Header
from datetime import datetime
from typing import Optional

from app.services.scheduler_service import SchedulerService

router = APIRouter()
logger = logging.getLogger(__name__)

# スケジューラーサービスのインスタンス
scheduler_service = SchedulerService()


@router.post("/api/scheduler/daily-collection")
async def daily_data_collection(
    x_cloudscheduler: Optional[str] = Header(None, alias="X-CloudScheduler"),
    user_agent: Optional[str] = Header(None, alias="User-Agent")
):
    """
    毎日のデータ収集を実行するエンドポイント
    Cloud Schedulerからのリクエストのみ受け付ける（セキュリティ）
    """
    # Cloud Schedulerからのリクエスト検証
    if not x_cloudscheduler and user_agent != "Google-Cloud-Scheduler":
        logger.warning(f"Unauthorized access attempt - User-Agent: {user_agent}")
        raise HTTPException(
            status_code=403,
            detail="This endpoint is only accessible by Cloud Scheduler"
        )

    logger.info("=" * 60)
    logger.info("Daily data collection triggered by Cloud Scheduler")
    logger.info(f"X-CloudScheduler: {x_cloudscheduler}")
    logger.info("=" * 60)

    try:
        # データ収集実行
        result = scheduler_service.collect_data()

        if result["status"] == "success":
            logger.info("Data collection completed successfully")
            return result
        else:
            logger.error(f"Data collection failed: {result.get('message')}")
            raise HTTPException(status_code=500, detail=result.get("message"))

    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.get("/api/scheduler/status")
async def scheduler_status():
    """
    スケジューラーエンドポイントの状態確認
    ヘルスチェック用
    """
    try:
        status = scheduler_service.health_check()
        return status

    except Exception as e:
        logger.error(f"Status check error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
