"""
環境設定管理システム
ローカル開発と本番環境の設定を統一管理
"""
import os
from typing import Optional
from pydantic import validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """アプリケーション設定クラス"""

    # アプリケーション基本設定
    app_name: str = "Market Analytics"
    app_version: str = "2.0.0"
    debug: bool = False

    # サーバー設定
    host: str = "127.0.0.1"
    port: int = 8000

    # Supabase データベース設定
    supabase_url: Optional[str] = None
    supabase_key: Optional[str] = None

    # 環境判定
    environment: str = "local"  # local, production

    @validator('supabase_url')
    def validate_supabase_url(cls, v):
        if not v:
            raise ValueError("SUPABASE_URL is required")
        return v

    @validator('supabase_key')
    def validate_supabase_key(cls, v):
        if not v:
            raise ValueError("SUPABASE_KEY is required")
        return v

    @property
    def is_production(self) -> bool:
        """本番環境かどうかを判定"""
        return self.environment == "production"

    @property
    def is_local(self) -> bool:
        """ローカル環境かどうかを判定"""
        return self.environment == "local"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # 環境変数名の自動変換（小文字 + アンダースコア）
        case_sensitive = False
        # 定義されていない環境変数を許可
        extra = "ignore"


def get_settings() -> Settings:
    """設定インスタンスを取得（シングルトンパターン）"""
    return Settings()


# グローバル設定インスタンス
settings = get_settings()