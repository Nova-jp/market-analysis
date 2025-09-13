#!/usr/bin/env python3
"""
Market Analytics API Main Application
将来のWebアプリ化のためのFastAPI メインアプリケーション
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import sys
from datetime import datetime

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# APIルーターのインポート
from app.api.yield_curve import router as yield_curve_router

# FastAPIアプリケーション作成
app = FastAPI(
    title="Market Analytics API",
    description="日本国債市場分析API - イールドカーブ分析、金利データ提供",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS設定（将来のWebアプリ対応）
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # React開発サーバー
        "http://localhost:5173",  # Vite開発サーバー
        "http://localhost:8080",  # Vue.js開発サーバー
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:8080",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# ルーター登録
app.include_router(yield_curve_router)

# ヘルスチェックエンドポイント
@app.get("/")
async def root():
    """APIルートエンドポイント"""
    return {
        "message": "Market Analytics API",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "yield_curve": "/api/yield-curve/",
            "docs": "/docs",
            "health": "/health"
        }
    }

@app.get("/health")
async def health_check():
    """ヘルスチェックエンドポイント"""
    try:
        # データベース接続チェック
        from data.utils.database_manager import DatabaseManager
        db_manager = DatabaseManager()
        
        # 簡単な接続テスト
        import requests
        response = requests.head(
            f'{db_manager.supabase_url}/rest/v1/clean_bond_data',
            headers=db_manager.headers
        )
        
        db_status = "connected" if response.status_code == 200 else "disconnected"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": db_status,
            "services": {
                "yield_curve_analyzer": "available",
                "api_endpoints": "available"
            }
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
        )

# エラーハンドリング
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not Found",
            "message": "指定されたリソースが見つかりません",
            "timestamp": datetime.now().isoformat()
        }
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error", 
            "message": "内部サーバーエラーが発生しました",
            "timestamp": datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    # 環境変数から設定を読み込み
    host = os.getenv("API_HOST", "127.0.0.1")
    port = int(os.getenv("API_PORT", 8000))
    
    print(f"🚀 Market Analytics API を起動中...")
    print(f"📍 URL: http://{host}:{port}")
    print(f"📚 API ドキュメント: http://{host}:{port}/docs")
    
    uvicorn.run(
        "app.api.main:app",
        host=host,
        port=port,
        reload=True,  # 開発モード
        log_level="info"
    )