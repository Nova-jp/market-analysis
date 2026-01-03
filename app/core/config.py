"""
環境設定管理システム
ローカル開発と本番環境の設定を統一管理
"""
import os
from typing import Optional, Dict, Any
from pydantic import validator, PostgresDsn
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

    # データベース設定 (Neon / PostgreSQL)
    # デフォルト値はローカル開発用または.envから読み込み
    db_host: str
    db_port: int = 5432
    db_name: str = "neondb"
    db_user: str
    db_password: str
    
    # 接続プール設定など
    db_pool_size: int = 5
    db_max_overflow: int = 10
    
    # Supabase設定（互換性のため維持するが、DB接続には使用しない推奨）
    supabase_url: Optional[str] = None
    supabase_key: Optional[str] = None

    # 環境判定
    environment: str = "local"  # local, production

    @validator('db_host', pre=True)
    def validate_host(cls, v, values):
        # 移行過渡期対応: CLOUD_SQL_HOST や NEON_HOST も許容
        if not v:
            return os.getenv('NEON_HOST') or os.getenv('CLOUD_SQL_HOST')
        return v

    @validator('db_user', pre=True)
    def validate_user(cls, v, values):
        if not v:
            return os.getenv('NEON_USER') or os.getenv('CLOUD_SQL_USER')
        return v

    @validator('db_password', pre=True)
    def validate_password(cls, v, values):
        if not v:
            return os.getenv('NEON_PASSWORD') or os.getenv('CLOUD_SQL_PASSWORD')
        return v

    @validator('db_name', pre=True)
    def validate_db_name(cls, v, values):
        if not v:
            return os.getenv('NEON_DATABASE') or os.getenv('CLOUD_SQL_DATABASE', 'market_analytics')
        return v

    @property
    def database_url(self) -> str:
        """SQLAlchemy用同期接続URL (psycopg2)"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

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
