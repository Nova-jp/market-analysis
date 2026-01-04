"""
メインFastAPIアプリケーション
統一されたWebアプリケーションエントリーポイント
"""
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

from app.core.config import settings
from app.api.endpoints import health, dates, yield_data, scheduler, pca, market_amount
from app.web.routes import router as web_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    アプリケーションのライフサイクル管理
    起動時と終了時の処理を安全に実行
    """
    # === 起動時処理 ===
    print(f"🚀 {settings.app_name} v{settings.app_version} starting...")
    print(f"📊 Environment: {settings.environment}")
    print(f"🔗 Database Host: {settings.db_host}")

    if settings.is_local:
        print(f"🌐 Local server: http://{settings.host}:{settings.port}")

    # yieldで制御を渡す（アプリケーション実行中）
    yield

    # === 終了時処理 ===
    print(f"👋 {settings.app_name} shutting down...")


# FastAPIアプリケーション初期化
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="国債金利分析システム - イールドカーブ比較・分析のための包括的ツール",
    lifespan=lifespan
)

# プロジェクトルート設定
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))

# テンプレート設定
templates = Jinja2Templates(directory=os.path.join(project_root, "templates"))

# 静的ファイル設定
try:
    app.mount("/static", StaticFiles(directory=os.path.join(project_root, "static")), name="static")
except Exception:
    # 静的ディレクトリが見つからない場合はスキップ
    pass

# APIルーターの登録
app.include_router(health.router, tags=["health"])
app.include_router(dates.router, tags=["dates"])
app.include_router(yield_data.router, tags=["yield_data"])
app.include_router(scheduler.router, tags=["scheduler"])
app.include_router(pca.router, prefix="/api/pca", tags=["pca"])
app.include_router(market_amount.router, tags=["market_amount"])

# Webページルーターの登録
app.include_router(web_router, tags=["web"])


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """ホーム画面 - 機能選択"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/yield-curve", response_class=HTMLResponse)
async def yield_curve_page(request: Request):
    """イールドカーブ比較画面"""
    return templates.TemplateResponse("yield_curve.html", {"request": request})


@app.get("/pca", response_class=HTMLResponse)
async def pca_analysis_page(request: Request):
    """PCA分析画面"""
    return templates.TemplateResponse("pca.html", {"request": request})


@app.get("/market-amount", response_class=HTMLResponse)
async def market_amount_page(request: Request):
    """市中残存額可視化画面"""
    return templates.TemplateResponse("market_amount.html", {"request": request})


@app.get("/debug", response_class=HTMLResponse)
async def debug_page(request: Request):
    """API Debug Test Page"""
    return templates.TemplateResponse("debug.html", {"request": request})