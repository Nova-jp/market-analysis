"""
Cloud Scheduler用データ収集API

スケジュール:
- 毎日18:00: JSDAデータ収集 (daily-collection)
- 毎日21:00: 金利スワップデータ収集 (irs-daily-collection) ★NEW
- 毎月1日 6:00: 入札カレンダー取得 (calendar-refresh)
- 毎日12:36: 入札結果収集 (auction-collection) ※入札がある日のみ実行

セキュリティ:
- X-CloudScheduler ヘッダーまたは User-Agent で検証
- Cloud Run では --no-allow-unauthenticated を推奨
- 本番環境では OIDC 認証の追加を検討
"""
import logging
import os
from fastapi import APIRouter, HTTPException, Header, Request
from datetime import datetime, date
from typing import Optional

from app.services.scheduler_service import SchedulerService
from app.services.irs_scheduler_service import IRSSchedulerService
from app.services.asw_scheduler_service import ASWSchedulerService
from app.services.pca_service import PCAService

router = APIRouter()
logger = logging.getLogger(__name__)

# カレンダーコレクターはグローバルインスタンスでキャッシュを共有
_calendar_collector = None


def get_calendar_collector():
    """カレンダーコレクターのシングルトン取得"""
    global _calendar_collector
    if _calendar_collector is None:
        from data.collectors.mof import AuctionCalendarCollector
        _calendar_collector = AuctionCalendarCollector()
    return _calendar_collector


# スケジューラーサービスのインスタンス
scheduler_service = SchedulerService()
irs_scheduler_service = IRSSchedulerService()
asw_scheduler_service = ASWSchedulerService()


def verify_cloud_scheduler_request(
    request: Request,
    x_cloudscheduler: Optional[str],
    user_agent: Optional[str]
) -> bool:
    """
    Cloud Schedulerからのリクエストを検証

    検証方法（優先順位順）:
    1. X-CloudScheduler ヘッダーの存在（Cloud Scheduler が自動付与）
    2. User-Agent が "Google-Cloud-Scheduler"
    3. 環境変数 ALLOW_SCHEDULER_DEBUG が "true" の場合はスキップ（開発用）

    Note:
        本番環境ではCloud RunのIAM認証（--no-allow-unauthenticated）と
        OIDCトークン検証を組み合わせることを推奨。
        https://cloud.google.com/scheduler/docs/http-target-auth

    Args:
        request: FastAPI Request オブジェクト
        x_cloudscheduler: X-CloudScheduler ヘッダー値
        user_agent: User-Agent ヘッダー値

    Returns:
        bool: 検証成功時True
    """
    # 開発/デバッグモード（本番では無効にすること）
    if os.getenv("ALLOW_SCHEDULER_DEBUG", "").lower() == "true":
        logger.warning("Scheduler debug mode enabled - authentication bypassed")
        return True

    # X-CloudScheduler ヘッダーによる検証（最も信頼性が高い）
    if x_cloudscheduler:
        logger.info(f"Authenticated via X-CloudScheduler header: {x_cloudscheduler[:20]}...")
        return True

    # User-Agent による検証（フォールバック）
    if user_agent == "Google-Cloud-Scheduler":
        logger.info("Authenticated via User-Agent: Google-Cloud-Scheduler")
        return True

    # 認証失敗時のログ
    client_ip = request.client.host if request.client else "unknown"
    logger.warning(
        f"Unauthorized scheduler access attempt - "
        f"IP: {client_ip}, User-Agent: {user_agent}, "
        f"X-CloudScheduler: {x_cloudscheduler}"
    )
    return False


@router.post("/api/scheduler/daily-collection")
async def daily_data_collection(
    request: Request,
    x_cloudscheduler: Optional[str] = Header(None, alias="X-CloudScheduler"),
    user_agent: Optional[str] = Header(None, alias="User-Agent")
):
    """
    毎日のデータ収集を実行するエンドポイント
    Cloud Schedulerからのリクエストのみ受け付ける
    """
    if not verify_cloud_scheduler_request(request, x_cloudscheduler, user_agent):
        raise HTTPException(
            status_code=403,
            detail="This endpoint is only accessible by Cloud Scheduler"
        )

    logger.info("=" * 60)
    logger.info("Daily data collection triggered by Cloud Scheduler")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    try:
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
    ヘルスチェック用（認証不要）
    """
    try:
        status = scheduler_service.health_check()
        return status

    except Exception as e:
        logger.error(f"Status check error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/scheduler/calendar-refresh")
async def calendar_refresh(
    request: Request,
    x_cloudscheduler: Optional[str] = Header(None, alias="X-CloudScheduler"),
    user_agent: Optional[str] = Header(None, alias="User-Agent")
):
    """
    入札カレンダー更新エンドポイント
    Cloud Schedulerから毎月1日 6:00に呼び出される

    当月のカレンダーをキャッシュに読み込む
    """
    if not verify_cloud_scheduler_request(request, x_cloudscheduler, user_agent):
        raise HTTPException(
            status_code=403,
            detail="This endpoint is only accessible by Cloud Scheduler"
        )

    logger.info("=" * 60)
    logger.info("Calendar refresh triggered by Cloud Scheduler")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    try:
        today = date.today()
        calendar_collector = get_calendar_collector()

        # キャッシュをクリアして最新を取得
        calendar_collector.clear_cache()
        calendar = calendar_collector.fetch_calendar(today.year, today.month, use_cache=False)

        if calendar:
            auction_count = len(calendar)
            logger.info(f"Calendar loaded: {today.year}/{today.month} - {auction_count} auctions")

            return {
                "status": "success",
                "message": f"Calendar refreshed for {today.year}/{today.month}",
                "data": {
                    "year": today.year,
                    "month": today.month,
                    "auction_count": auction_count,
                    "auctions": [
                        {
                            "date": str(a["auction_date"]),
                            "type": a["auction_type"],
                            "bond_type": a["bond_type"]
                        }
                        for a in calendar
                    ]
                }
            }
        else:
            return {
                "status": "warning",
                "message": f"No calendar data available for {today.year}/{today.month}"
            }

    except Exception as e:
        error_msg = f"Calendar refresh error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/api/scheduler/auction-collection")
async def auction_data_collection(
    request: Request,
    x_cloudscheduler: Optional[str] = Header(None, alias="X-CloudScheduler"),
    user_agent: Optional[str] = Header(None, alias="User-Agent")
):
    """
    入札結果データ収集エンドポイント
    Cloud Schedulerから毎日12:36に呼び出される

    カレンダーを確認し、本日入札がある場合のみ収集を実行
    """
    if not verify_cloud_scheduler_request(request, x_cloudscheduler, user_agent):
        raise HTTPException(
            status_code=403,
            detail="This endpoint is only accessible by Cloud Scheduler"
        )

    logger.info("=" * 60)
    logger.info("Auction data collection triggered by Cloud Scheduler")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    try:
        today = date.today()

        # まずカレンダーで今日入札があるか確認（キャッシュ利用）
        calendar_collector = get_calendar_collector()
        today_auctions = calendar_collector.get_auctions_for_date(today)

        if not today_auctions:
            logger.info(f"No auctions scheduled for today ({today})")
            return {
                "status": "success",
                "message": f"No auctions scheduled for {today}",
                "data": {
                    "date": str(today),
                    "auctions_scheduled": 0,
                    "auctions_collected": 0
                }
            }

        logger.info(f"Today's auctions: {len(today_auctions)}")
        for a in today_auctions:
            logger.info(f"  - {a['auction_type']}: {a['description']}")

        # 入札結果収集
        from data.collectors.mof import DailyAuctionCollector
        collector = DailyAuctionCollector()
        result = collector.collect_for_date(today)

        return {
            "status": "success",
            "message": "Auction data collection completed",
            "data": result
        }

    except Exception as e:
        error_msg = f"Auction collection error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/api/scheduler/irs-daily-collection")
async def irs_daily_data_collection(
    request: Request,
    x_cloudscheduler: Optional[str] = Header(None, alias="X-CloudScheduler"),
    user_agent: Optional[str] = Header(None, alias="User-Agent")
):
    """
    金利スワップ(IRS)データ収集エンドポイント
    Cloud Schedulerから毎日21:00に呼び出される

    JPXから金利スワップ清算値段を取得してSupabaseに保存
    """
    if not verify_cloud_scheduler_request(request, x_cloudscheduler, user_agent):
        raise HTTPException(
            status_code=403,
            detail="This endpoint is only accessible by Cloud Scheduler"
        )

    logger.info("=" * 60)
    logger.info("IRS daily data collection triggered by Cloud Scheduler")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    try:
        result = irs_scheduler_service.collect_data()

        if result["status"] == "success":
            logger.info("IRS data collection completed successfully")
            return result
        else:
            logger.error(f"IRS data collection failed: {result.get('message')}")
            raise HTTPException(status_code=500, detail=result.get("message"))

    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Unexpected error in IRS collection: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/api/scheduler/asw-daily-calculation")
async def asw_daily_calculation(
    request: Request,
    x_cloudscheduler: Optional[str] = Header(None, alias="X-CloudScheduler"),
    user_agent: Optional[str] = Header(None, alias="User-Agent")
):
    """
    ASW計算実行エンドポイント
    Cloud Schedulerから毎日21:30に呼び出される
    """
    if not verify_cloud_scheduler_request(request, x_cloudscheduler, user_agent):
        raise HTTPException(
            status_code=403,
            detail="This endpoint is only accessible by Cloud Scheduler"
        )

    logger.info("=" * 60)
    logger.info("ASW daily calculation triggered by Cloud Scheduler")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    try:
        # ASW計算の前にPCAキャッシュをクリア
        try:
            pca_service = PCAService()
            pca_service.clear_cache()
            logger.info("PCA cache cleared as part of daily update")
        except Exception as cache_err:
            logger.error(f"Failed to clear PCA cache: {cache_err}")

        result = await asw_scheduler_service.calculate_daily_asw()

        if result["status"] in ["success", "skipped"]:
            logger.info(f"ASW calculation task finished: {result.get('message')}")
            return result
        else:
            logger.error(f"ASW calculation failed: {result.get('message')}")
            raise HTTPException(status_code=500, detail=result.get("message"))

    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Unexpected error in ASW calculation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/api/scheduler/clear-pca-cache")
async def clear_pca_cache(
    request: Request,
    x_cloudscheduler: Optional[str] = Header(None, alias="X-CloudScheduler"),
    user_agent: Optional[str] = Header(None, alias="User-Agent")
):
    """
    PCAキャッシュを手動またはスケジュールでクリアするエンドポイント
    """
    if not verify_cloud_scheduler_request(request, x_cloudscheduler, user_agent):
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    try:
        pca_service = PCAService()
        pca_service.clear_cache()
        return {"status": "success", "message": "PCA cache cleared"}
    except Exception as e:
        logger.error(f"Error clearing PCA cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))

