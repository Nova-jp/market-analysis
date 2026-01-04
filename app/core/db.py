"""
Database Connection Module (SQLAlchemy Async)

SQLAlchemyを使用したデータベース接続管理。
FastAPIのDependency Injectionで使用する get_db 関数を提供。
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from typing import AsyncGenerator

from app.core.config import settings

# 非同期データベースエンジン
engine = create_async_engine(
    settings.async_database_url,
    echo=settings.debug,  # デバッグモード時はSQLログを出力
    future=True,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    connect_args={"ssl": "require"},
)

# 非同期セッションファクトリ
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI Dependency: データベースセッションを取得
    リクエストごとにセッションを作成し、終了時にクローズする
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
