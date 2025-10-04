"""
Web UI ルーティング
追加のWebページルートを定義
"""
from fastapi import APIRouter

router = APIRouter()

# 将来的な追加ページ用のルートをここに定義
# 例：
# @router.get("/analytics")
# async def analytics_page(request: Request):
#     return templates.TemplateResponse("analytics.html", {"request": request})