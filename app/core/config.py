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

    # Cloud SQL データベース設定
    cloud_sql_host: str
    cloud_sql_port: int = 5432
    cloud_sql_database: str = "market_analytics"
    cloud_sql_user: str
    cloud_sql_password: str
    cloud_sql_connection_name: Optional[str] = None  # Cloud Run用

    # 環境判定
    environment: str = "local"  # local, production

    @validator('cloud_sql_host')
    def validate_host(cls, v):
        if not v:
            raise ValueError("CLOUD_SQL_HOST is required")
        return v

    @validator('cloud_sql_user')
    def validate_user(cls, v):
        if not v:
            raise ValueError("CLOUD_SQL_USER is required")
        return v

    @validator('cloud_sql_password')
    def validate_password(cls, v):
        if not v:
            raise ValueError("CLOUD_SQL_PASSWORD is required")
        return v

    @property
    def database_url(self) -> str:
        """データベース接続URL（asyncpg用）"""
        return f"postgresql://{self.cloud_sql_user}:{self.cloud_sql_password}@{self.cloud_sql_host}:{self.cloud_sql_port}/{self.cloud_sql_database}"

    @property
    def database_dsn(self) -> dict:
        """データベース接続パラメータ（psycopg2用）"""
        return {
            "host": self.cloud_sql_host,
            "port": self.cloud_sql_port,
            "database": self.cloud_sql_database,
            "user": self.cloud_sql_user,
            "password": self.cloud_sql_password
        }

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