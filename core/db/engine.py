"""
Database Connection Module (SQLAlchemy Async)
FastAPI の Dependency Injection で使用する get_db 関数を提供。
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from typing import AsyncGenerator

from core.config import settings

engine = create_async_engine(
    settings.async_database_url,
    echo=settings.debug,
    future=True,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_timeout=30,
    pool_recycle=1800,
    connect_args={
        "ssl": "require",
        "command_timeout": 30,
    },
)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI Dependency: リクエストごとにセッションを作成してクローズする"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
