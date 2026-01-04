"""
環境設定管理システム
ローカル開発と本番環境の設定を統一管理
"""
import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """アプリケーション設定クラス"""

    # アプリケーション基本設定
    app_name: str = "Market Analytics"
    app_version: str = "2.1.0"
    debug: bool = False

    # サーバー設定
    host: str = "127.0.0.1"
    port: int = 8000

    # データベース設定 (PostgreSQL)
    # .env の DB_HOST, DB_USER 等を自動的に読み込む
    db_host: str = Field(validation_alias='DB_HOST')
    db_port: int = Field(5432, validation_alias='DB_PORT')
    db_name: str = Field("neondb", validation_alias='DB_NAME')
    db_user: str = Field(validation_alias='DB_USER')
    db_password: str = Field(validation_alias='DB_PASSWORD')
    
    # 接続プール設定
    db_pool_size: int = 5
    db_max_overflow: int = 10
    
    # 環境判定
    environment: str = "local"  # local, production

    @property
    def database_url(self) -> str:
        """SQLAlchemy用同期接続URL (psycopg2)"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}?sslmode=require"

    @property
    def async_database_url(self) -> str:
        """SQLAlchemy用非同期接続URL (asyncpg)"""
        # postgresql+asyncpg://user:pass@host:port/dbname
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

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
        case_sensitive = False
        extra = "ignore"


def get_settings() -> Settings:
    """設定インスタンスを取得（シングルトンパターン）"""
    return Settings()


# グローバル設定インスタンス
settings = get_settings()
