#!/usr/bin/env python3
"""
Market Analytics - 国債金利分析システム
- イールドカーブ比較・分析機能
- 効率的なデータベースアクセスAPI
- シンプルで拡張性の高い設計
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from datetime import datetime, timedelta
import os
import sys
import requests
from typing import Optional, List, Dict, Any

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data.utils.database_manager import DatabaseManager

# アプリ初期化
app = FastAPI(title="Market Analytics - Simple", version="2.0.0")

# テンプレートとstatic設定
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# データベースマネージャー初期化
db_manager = DatabaseManager()

# ========================================
# フロントエンドルート
# ========================================

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

# ========================================
# データ取得API - 汎用的なデータベースアクセス
# ========================================

async def execute_db_query(params: Dict[str, Any]) -> Dict[str, Any]:
    """データベースクエリの共通実行関数"""
    try:
        response = requests.get(
            f'{db_manager.supabase_url}/rest/v1/clean_bond_data',
            params=params,
            headers=db_manager.headers
        )

        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": f"Database error: {response.status_code}"}

    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/search-dates")
async def search_dates(q: str = "") -> Dict[str, List[str]]:
    """日付検索API - YYYY-MM-DD形式での日付検索"""
    if len(q) < 4:
        return {"dates": []}

    try:
        # 完全な日付形式の場合
        if len(q) == 10:
            datetime.strptime(q, '%Y-%m-%d')
            result = await execute_db_query({
                'select': 'trade_date',
                'trade_date': f'eq.{q}',
                'limit': 1
            })

            if result["success"]:
                return {"dates": [item['trade_date'] for item in result["data"]]}

        # 部分入力の場合
        else:
            result = await execute_db_query({
                'select': 'trade_date',
                'trade_date': f'gte.{q}',
                'order': 'trade_date.desc',
                'limit': 50
            })

            if result["success"]:
                # 入力文字列で始まる日付のみフィルタリング
                filtered_data = [item for item in result["data"] if item['trade_date'].startswith(q)]
                unique_dates = sorted(list(set([item['trade_date'] for item in filtered_data])), reverse=True)
                return {"dates": unique_dates[:15]}

        return {"dates": []}

    except ValueError:
        return {"dates": []}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/quick-dates")
async def get_quick_dates() -> Dict[str, str]:
    """
    クイック選択用の営業日を取得（最新、前日、5営業日前、1ヶ月前）
    シングルクエリ+ソート方式でパフォーマンス最適化
    """
    try:
        # 全ユニーク日付を一度取得（約30営業日分）
        result = await execute_db_query({
            'select': 'trade_date',
            'order': 'trade_date.desc',
            'limit': 10000  # 約30営業日分をカバー (300レコード/日 × 30日)
        })

        if not result["success"]:
            return JSONResponse(status_code=500, content={"error": result["error"]})

        if not result["data"]:
            return JSONResponse(status_code=404, content={
                "error": "No trading dates found in database"
            })

        # ユニークな日付リストを作成（降順ソート維持）
        unique_dates = list(dict.fromkeys([item['trade_date'] for item in result["data"]]))

        quick_dates = {}

        # インデックスベースで営業日を取得
        if len(unique_dates) > 0:
            quick_dates['latest'] = unique_dates[0]

        if len(unique_dates) > 1:
            quick_dates['previous'] = unique_dates[1]

        if len(unique_dates) > 5:
            quick_dates['five_days_ago'] = unique_dates[5]

        # 1ヶ月前の営業日を計算
        if unique_dates:
            try:
                latest_dt = datetime.strptime(unique_dates[0], '%Y-%m-%d')
                month_ago_target = latest_dt - timedelta(days=30)
                target_str = month_ago_target.strftime('%Y-%m-%d')

                # 1ヶ月前に最も近い過去の営業日を検索
                for date in unique_dates:
                    if date <= target_str:
                        quick_dates['month_ago'] = date
                        break
            except ValueError:
                pass

        return quick_dates

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/yield-data/{date}")
async def get_yield_data(date: str) -> Dict[str, Any]:
    """指定日のイールドカーブデータ取得と満期年数計算"""
    try:
        # 日付形式チェック
        datetime.strptime(date, '%Y-%m-%d')

        # データベースから取得
        result = await execute_db_query({
            'select': 'trade_date,due_date,ave_compound_yield,bond_name',
            'trade_date': f'eq.{date}',
            'ave_compound_yield': 'not.is.null',
            'due_date': 'not.is.null',
            'order': 'due_date.asc',
            'limit': 1000
        })

        if not result["success"]:
            return {"error": result["error"]}

        if not result["data"]:
            return {"error": f"No data found for {date}"}

        # 満期年数を計算してデータを処理
        processed_data = []
        for row in result["data"]:
            try:
                trade_dt = datetime.strptime(row['trade_date'], '%Y-%m-%d')
                due_dt = datetime.strptime(row['due_date'], '%Y-%m-%d')
                days_to_maturity = (due_dt - trade_dt).days
                years_to_maturity = days_to_maturity / 365.25

                if years_to_maturity > 0 and row['ave_compound_yield'] is not None:
                    processed_data.append({
                        "maturity": round(years_to_maturity, 3),
                        "yield": float(row['ave_compound_yield']),
                        "bond_name": row['bond_name']
                    })
            except (ValueError, TypeError):
                continue

        return {
            "date": date,
            "data": processed_data
        }

    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ========================================
# システム管理API
# ========================================

@app.get("/health")
async def health_check() -> Dict[str, str]:
    """システムヘルスチェック"""
    return {
        "status": "healthy",
        "app": "Market Analytics",
        "version": "2.0.0",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/info")
async def system_info() -> Dict[str, Any]:
    """システム情報とAPI一覧"""
    return {
        "app_name": "Market Analytics - 国債金利分析システム",
        "version": "2.0.0",
        "description": "イールドカーブ比較・分析のための包括的ツール",
        "apis": {
            "search_dates": "/api/search-dates?q={partial_date}",
            "quick_dates": "/api/quick-dates",
            "yield_data": "/api/yield-data/{date}",
            "health": "/health",
            "info": "/api/info"
        },
        "features": [
            "最大20日付のイールドカーブ同時比較",
            "年限範囲フィルタリング（例：20-30年）",
            "高速営業日検索・選択",
            "レスポンシブWebUI"
        ]
    }

# ========================================
# アプリケーション起動
# ========================================

if __name__ == "__main__":
    import uvicorn

    print("🚀 Market Analytics - 国債金利分析システム")
    print("=" * 50)
    print("📊 主要機能:")
    print("   ✓ イールドカーブ比較（最大20日付）")
    print("   ✓ 年限範囲フィルタリング")
    print("   ✓ 高速営業日検索")
    print("   ✓ PCA分析（開発中）")
    print()
    print("🌐 アクセスURL:")
    print("   ホーム: http://127.0.0.1:8001")
    print("   分析画面: http://127.0.0.1:8001/yield-curve")
    print("   API情報: http://127.0.0.1:8001/api/info")
    print("=" * 50)

    uvicorn.run(
        "simple_app:app",
        host="127.0.0.1",
        port=8001,
        reload=True
    )