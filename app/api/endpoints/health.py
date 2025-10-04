"""
ヘルスチェック・システム情報API
アプリケーションとデータベースの状態確認
"""
from fastapi import APIRouter
from datetime import datetime
from app.core.database import db_manager
from app.core.config import settings
from app.core.models import HealthCheckResponse, SystemInfoResponse

router = APIRouter()


@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """システムヘルスチェック"""
    db_status = await db_manager.health_check()

    return HealthCheckResponse(
        status="healthy",
        app=settings.app_name,
        version=settings.app_version,
        timestamp=datetime.now().isoformat(),
        database_config=db_status
    )


@router.get("/api/info", response_model=SystemInfoResponse)
async def system_info():
    """システム情報とAPI一覧"""
    return SystemInfoResponse(
        app_name="Market Analytics - 国債金利分析システム",
        version=settings.app_version,
        description="イールドカーブ比較・分析のための包括的ツール",
        apis={
            "search_dates": "/api/search-dates?q={partial_date}",
            "quick_dates": "/api/quick-dates",
            "yield_data": "/api/yield-data/{date}",
            "health": "/health",
            "info": "/api/info"
        },
        features=[
            "最大20日付のイールドカーブ同時比較",
            "年限範囲フィルタリング（例：20-30年）",
            "高速営業日検索・選択",
            "レスポンシブWebUI"
        ]
    )