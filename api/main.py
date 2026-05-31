"""
メインFastAPIアプリケーション
Next.jsフロントエンドと統合されたWebアプリケーション
"""
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from core.config import settings
from api.routes import health, dates, yield_data, scheduler, pca, market_amount, private_analytics, export, imm_forward_matrix, instantaneous_forward


@asynccontextmanager
async def lifespan(app: FastAPI):
    from sqlalchemy import text
    from core.db.engine import AsyncSessionLocal
    async with AsyncSessionLocal() as session:
        await session.execute(text("SELECT 1"))
    print(f"{settings.app_name} starting...")
    yield
    print(f"{settings.app_name} shutting down...")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan
)

_cors_origins = ["https://market-analytics-646409283435.asia-northeast1.run.app"]
if not settings.is_production:
    _cors_origins += ["http://localhost:3000", "http://localhost:8000", "http://127.0.0.1:8000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# APIルーターの登録
app.include_router(health.router, tags=["health"])
app.include_router(dates.router, tags=["dates"])
app.include_router(yield_data.router, tags=["yield_data"])
app.include_router(scheduler.router, tags=["scheduler"])
app.include_router(pca.router, prefix="/api/pca", tags=["pca"])
app.include_router(market_amount.router, tags=["market_amount"])
app.include_router(private_analytics.router, prefix="/api/private", tags=["private"])
app.include_router(export.router, prefix="/api/export", tags=["export"])
app.include_router(imm_forward_matrix.router, prefix="/api/imm-forward-matrix", tags=["imm-forward-matrix"])
app.include_router(instantaneous_forward.router, tags=["instantaneous_forward"])

# 静的ファイルの配信設定 (Next.jsビルド成果物)
# Docker本番: static/dist、ローカル開発: web/out
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dist_path = os.path.join(project_root, "static", "dist")
if not os.path.exists(dist_path):
    dist_path = os.path.join(project_root, "web", "out")

if os.path.exists(dist_path):
    app.mount("/_next", StaticFiles(directory=os.path.join(dist_path, "_next")), name="next-static")
    app.mount("/static", StaticFiles(directory=dist_path), name="static")

    @app.get("/", response_class=FileResponse)
    async def home():
        return os.path.join(dist_path, "index.html")

    @app.get("/yield-curve", response_class=FileResponse)
    async def yield_curve_page():
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

    @app.get("/asw", response_class=FileResponse)
    async def asw_page():
        path = os.path.join(dist_path, "asw.html")
        if not os.path.exists(path):
            path = os.path.join(dist_path, "asw/index.html")
        return path

    @app.get("/market-amount", response_class=FileResponse)
    async def market_amount_page():
        path = os.path.join(dist_path, "market-amount.html")
        if not os.path.exists(path):
            path = os.path.join(dist_path, "market-amount/index.html")
        return path

    @app.get("/imm-forward-matrix", response_class=FileResponse)
    async def imm_forward_matrix_page():
        path = os.path.join(dist_path, "imm-forward-matrix.html")
        if not os.path.exists(path):
            path = os.path.join(dist_path, "imm-forward-matrix/index.html")
        return path

    @app.get("/instantaneous-forward", response_class=FileResponse)
    async def instantaneous_forward_page():
        path = os.path.join(dist_path, "instantaneous-forward.html")
        if not os.path.exists(path):
            path = os.path.join(dist_path, "instantaneous-forward/index.html")
        return path

    @app.get("/{path:path}")
    async def static_proxy(path: str):
        if path.startswith("api/"):
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="API endpoint not found")
        file_path = os.path.join(dist_path, path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(dist_path, "index.html"))
else:
    @app.get("/")
    async def root():
        return {"message": "Frontend build not found. Please run 'npm run build' in the web/ directory."}
