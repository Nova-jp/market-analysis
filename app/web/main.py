"""
メインFastAPIアプリケーション
Next.jsフロントエンドと統合されたWebアプリケーション
"""
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse

from app.core.config import settings
from app.api.endpoints import health, dates, yield_data, scheduler, pca, market_amount, private_analytics
from app.api.deps import get_current_username
from fastapi import Depends


@asynccontextmanager
async def lifespan(app: FastAPI):
    """アプリケーションのライフサイクル管理"""
    print(f"🚀 {settings.app_name} starting...")
    yield
    print(f"👋 {settings.app_name} shutting down...")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan
)

# プロジェクトルート設定
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))

# APIルーターの登録
app.include_router(health.router, tags=["health"])
app.include_router(dates.router, tags=["dates"])
app.include_router(yield_data.router, tags=["yield_data"])
app.include_router(scheduler.router, tags=["scheduler"])
app.include_router(pca.router, prefix="/api/pca", tags=["pca"])
app.include_router(market_amount.router, tags=["market_amount"])
app.include_router(private_analytics.router, prefix="/api/private", tags=["private"])

# 静的ファイルの配信設定 (Next.jsビルド成果物)
# Dockerfileで /build/out が static/dist にコピーされている想定
dist_path = os.path.join(project_root, "static", "dist")

if os.path.exists(dist_path):
    # Next.js の静的アセット (_next 等) を配信
    app.mount("/_next", StaticFiles(directory=os.path.join(dist_path, "_next")), name="next-static")
    app.mount("/static", StaticFiles(directory=dist_path), name="static")

    # 各ルートに対するHTML配信
    @app.get("/", response_class=FileResponse)
    async def home():
        return os.path.join(dist_path, "index.html")

    @app.get("/yield-curve", response_class=FileResponse)
    async def yield_curve_page():
        # Next.js の出力形式に合わせて index.html または yield-curve.html を返す
        path = os.path.join(dist_path, "yield-curve.html")
        if not os.path.exists(path):
            path = os.path.join(dist_path, "yield-curve/index.html")
        return path

    @app.get("/pca", response_class=FileResponse)
    async def pca_page():
        path = os.path.join(dist_path, "pca.html")
        if not os.path.exists(path):
            path = os.path.join(dist_path, "pca/index.html")
        return path
        
    @app.get("/market-amount", response_class=FileResponse)
    async def market_amount_page():
        path = os.path.join(dist_path, "market-amount.html")
        if not os.path.exists(path):
            path = os.path.join(dist_path, "market-amount/index.html")
        return path

    # その他の静的ファイルへのフォールバック（faviconなど）
    @app.get("/{path:path}")
    async def static_proxy(path: str):
        file_path = os.path.join(dist_path, path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)
        # 見つからない場合は index.html を返す (SPA的な挙動)
        return os.path.join(dist_path, "index.html")
else:
    # 開発環境等でビルド済みファイルがない場合
    @app.get("/")
    async def root():
        return {"message": "Frontend build not found. Please run 'npm run build' in frontend directory."}
