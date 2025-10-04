"""
API依存性注入
共通の依存関数・バリデーション
"""
from fastapi import HTTPException
from app.core.database import db_manager
from app.core.config import settings


async def get_database():
    """データベースマネージャーを取得"""
    return db_manager


async def get_settings():
    """設定を取得"""
    return settings


def validate_environment():
    """環境設定の検証"""
    if not settings.supabase_url or not settings.supabase_key:
        raise HTTPException(
            status_code=500,
            detail="Database configuration is incomplete"
        )