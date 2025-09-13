#!/usr/bin/env python3
"""
Market Analytics Web Application
FastAPI + Jinja2 による本格的なWebアプリケーション
"""

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, date
import os
import sys
from typing import Optional

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.yield_curve_analyzer import YieldCurveAnalyzer
from analysis.pca_analyzer import YieldCurvePCAAnalyzer

# FastAPIアプリケーション作成
app = FastAPI(
    title="Market Analytics Web Application",
    description="日本国債市場分析Webアプリケーション",
    version="1.0.0"
)

# CORS設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 静的ファイル配信
static_dir = os.path.join(os.path.dirname(__file__), "static")
template_dir = os.path.join(os.path.dirname(__file__), "templates")

app.mount("/static", StaticFiles(directory=static_dir), name="static")

# テンプレート設定
templates = Jinja2Templates(directory=template_dir)

# グローバル分析器
analyzer = YieldCurveAnalyzer()
pca_analyzer = YieldCurvePCAAnalyzer(db_manager=analyzer.db_manager)

async def get_available_dates():
    """利用可能な日付一覧を取得（clean_bond_dataテーブルから実際のデータ存在日のみ）"""
    try:
        import requests
        
        # 実際にデータが存在する日付のみを取得
        response = requests.get(
            f'{analyzer.db_manager.supabase_url}/rest/v1/clean_bond_data',
            params={
                'select': 'trade_date',
                'order': 'trade_date.desc',
                'limit': 50000  # 十分大きな値で全データを取得
            },
            headers=analyzer.db_manager.headers
        )
        
        if response.status_code == 200:
            data = response.json()
            # 重複排除して降順でソート
            unique_dates = sorted(list(set([item['trade_date'] for item in data])), reverse=True)
            print(f"📅 データベースから{len(unique_dates)}日分の利用可能日付を取得")
            return unique_dates
        else:
            print(f"❌ データ取得エラー: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ 日付取得例外: {e}")
        return ['2025-09-09']

# =====================
# HTMLページルート
# =====================

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """メインダッシュボード"""
    available_dates = await get_available_dates()
    
    context = {
        "request": request,
        "title": "日本国債イールドカーブ分析",
        "available_dates": available_dates,
        "latest_date": available_dates[0] if available_dates else None
    }
    
    return templates.TemplateResponse("dashboard.html", context)

@app.get("/yield-curve", response_class=HTMLResponse)
async def yield_curve_page(request: Request):
    """イールドカーブ専用ページ"""
    available_dates = await get_available_dates()
    
    context = {
        "request": request,
        "title": "イールドカーブ分析",
        "available_dates": available_dates,
        "latest_date": available_dates[0] if available_dates else None
    }
    
    return templates.TemplateResponse("yield_curve.html", context)

@app.get("/yield-curve-enhanced", response_class=HTMLResponse)
async def yield_curve_enhanced_page(request: Request):
    """イールドカーブ強化版ページ（5パネル+PCA）"""
    available_dates = await get_available_dates()
    
    context = {
        "request": request,
        "title": "イールドカーブ分析（強化版）",
        "available_dates": available_dates,
        "latest_date": available_dates[0] if available_dates else None
    }
    
    return templates.TemplateResponse("yield_curve_enhanced.html", context)

@app.get("/time-series", response_class=HTMLResponse)
async def time_series_page(request: Request):
    """時系列分析ページ"""
    available_dates = await get_available_dates()
    
    context = {
        "request": request,
        "title": "時系列分析",
        "available_dates": available_dates,
        "latest_date": available_dates[0] if available_dates else None
    }
    
    return templates.TemplateResponse("time_series.html", context)

@app.get("/about", response_class=HTMLResponse)
async def about_page(request: Request):
    """About ページ"""
    context = {
        "request": request,
        "title": "システムについて"
    }
    
    return templates.TemplateResponse("about.html", context)

# =====================
# API エンドポイント（JSON）
# =====================

@app.get("/api/dates")
async def api_get_dates():
    """利用可能な日付一覧API"""
    try:
        dates = await get_available_dates()
        return {
            "success": True,
            "dates": dates,
            "total": len(dates),
            "latest": dates[0] if dates else None
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)}
        )

@app.get("/api/yield-curve/{target_date}")
async def api_get_yield_curve(target_date: str):
    """指定日のイールドカーブデータAPI"""
    try:
        # 日付形式チェック
        datetime.strptime(target_date, '%Y-%m-%d')
        
        # 分析実行
        result = analyzer.analyze_yield_curve(target_date, show_plots=False)
        
        if "error" in result:
            return JSONResponse(
                status_code=404,
                content={"status": "error", "error": result["error"]}
            )
        
        # レスポンス用にデータ変換（scatter plot用）
        df = result['data']
        
        # データポイント作成（scatter plot用）
        # NaN, Inf値を除去してJSON安全にする
        import math
        data_points = []
        for _, row in df.iterrows():
            maturity = row['years_to_maturity']
            yield_rate = row['ave_compound_yield']
            coupon = row.get('coupon_rate', 0)
            
            # NaN, Inf値のチェック
            if (not math.isnan(maturity) and not math.isinf(maturity) and 
                not math.isnan(yield_rate) and not math.isinf(yield_rate) and
                not math.isnan(coupon) and not math.isinf(coupon)):
                
                data_points.append({
                    "maturity_years": float(maturity),
                    "yield_rate": float(yield_rate),
                    "bond_name": str(row['bond_name']),
                    "coupon_rate": float(coupon)
                })
        
        # 統計情報
        stats = result.copy()
        stats.pop('data', None)  # 重いデータフレームを除去
        
        # 期間別統計（NaN/Inf安全）
        def safe_mean(series):
            """NaN/Infを除外した平均値計算"""
            if len(series) == 0:
                return None
            clean_values = [x for x in series if not math.isnan(x) and not math.isinf(x)]
            return float(sum(clean_values) / len(clean_values)) if len(clean_values) > 0 else None
        
        short_term = df[df['years_to_maturity'] <= 2]
        medium_term = df[(df['years_to_maturity'] > 2) & (df['years_to_maturity'] <= 10)]
        long_term = df[df['years_to_maturity'] > 10]
        
        period_stats = {
            "short_term": {
                "count": len(short_term),
                "avg_yield": safe_mean(short_term['ave_compound_yield']),
                "label": "短期（2年以下）"
            },
            "medium_term": {
                "count": len(medium_term),
                "avg_yield": safe_mean(medium_term['ave_compound_yield']),
                "label": "中期（2-10年）"
            },
            "long_term": {
                "count": len(long_term),
                "avg_yield": safe_mean(long_term['ave_compound_yield']),
                "label": "長期（10年超）"
            }
        }
        
        return {
            "status": "success",
            "date": target_date,
            "data": data_points,
            "stats": stats,
            "period_stats": period_stats,
            "bond_count": len(df)
        }
        
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "error": "無効な日付形式です"}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )

@app.get("/api/compare")
async def api_compare_dates(dates: str, maturity_min: float = 0, maturity_max: float = 50):
    """複数日比較API"""
    try:
        date_list = [d.strip() for d in dates.split(',')]
        
        if len(date_list) > 5:  # 制限
            return JSONResponse(
                status_code=400,
                content={"success": False, "error": "比較できるのは5日までです"}
            )
        
        results = {}
        colors = [
            "rgba(54, 162, 235, 1)",   # 青
            "rgba(255, 99, 132, 1)",   # 赤  
            "rgba(75, 192, 192, 1)",   # 緑
            "rgba(255, 205, 86, 1)",   # 黄
            "rgba(153, 102, 255, 1)"   # 紫
        ]
        
        chart_data = {
            "labels": [],
            "datasets": []
        }
        
        for i, target_date in enumerate(date_list):
            try:
                datetime.strptime(target_date, '%Y-%m-%d')
                result = analyzer.analyze_yield_curve(target_date, show_plots=False)
                
                if "error" not in result:
                    df = result['data']
                    df_filtered = df[
                        (df['years_to_maturity'] >= maturity_min) & 
                        (df['years_to_maturity'] <= maturity_max)
                    ]
                    
                    # データセット作成
                    dataset = {
                        "label": target_date,
                        "data": [],
                        "borderColor": colors[i % len(colors)],
                        "backgroundColor": colors[i % len(colors)].replace("1)", "0.1)"),
                        "fill": False,
                        "tension": 0.4,
                        "borderWidth": 2
                    }
                    
                    df_sorted = df_filtered.sort_values('years_to_maturity')
                    for _, row in df_sorted.iterrows():
                        dataset["data"].append({
                            "x": row['years_to_maturity'],
                            "y": row['ave_compound_yield']
                        })
                        
                        # X軸ラベル更新
                        maturity_label = f"{row['years_to_maturity']:.1f}年"
                        if maturity_label not in chart_data["labels"]:
                            chart_data["labels"].append(maturity_label)
                    
                    chart_data["datasets"].append(dataset)
                    results[target_date] = {"success": True, "count": len(df_filtered)}
                else:
                    results[target_date] = {"success": False, "error": result["error"]}
                    
            except ValueError:
                results[target_date] = {"success": False, "error": "無効な日付形式"}
            except Exception as e:
                results[target_date] = {"success": False, "error": str(e)}
        
        # X軸ラベルをソート
        chart_data["labels"].sort(key=lambda x: float(x.replace("年", "")))
        
        return {
            "success": True,
            "comparison_results": results,
            "chart_data": chart_data,
            "filters": {
                "maturity_min": maturity_min,
                "maturity_max": maturity_max
            }
        }
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)}
        )

@app.post("/api/pca-analysis")
async def api_pca_analysis(request: dict):
    """主成分分析API"""
    try:
        days = request.get('days', 100)
        components = request.get('components', 3)
        spline_points = request.get('spline_points', 50)
        
        # パラメータ検証
        days = max(50, min(1000, days))
        components = max(1, min(10, components))
        spline_points = max(20, min(100, spline_points))
        
        # PCA分析実行
        result = pca_analyzer.analyze_yield_curve_pca(
            days=days,
            n_components=components,
            spline_points=spline_points,
            maturity_range=(0.1, 30.0)
        )
        
        if result['status'] == 'success':
            # フロントエンド用にデータを整形
            return {
                "status": "success",
                "sample_count": result['sample_count'],
                "maturity_grid": result['maturity_grid'],
                "principal_components": result['principal_components'],
                "explained_variance": result['explained_variance_ratio'],
                "reconstruction_error": result['reconstruction_error'],
                "mean_reconstruction_error": result['mean_reconstruction_error'],
                "analysis_params": result['analysis_params']
            }
        else:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "error": result['error']}
            )
            
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )

# ヘルスチェック
@app.get("/health")
async def health_check():
    """ヘルスチェック"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Market Analytics Web App"
    }

if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv("WEB_HOST", "127.0.0.1")
    port = int(os.getenv("WEB_PORT", 8000))
    
    print(f"🌐 Market Analytics Web App 起動中...")
    print(f"📍 URL: http://{host}:{port}")
    print(f"📈 ダッシュボード: http://{host}:{port}")
    print(f"📊 イールドカーブ: http://{host}:{port}/yield-curve")
    print(f"📈 時系列分析: http://{host}:{port}/time-series")
    
    uvicorn.run(
        "webapp.main:app",
        host=host,
        port=port,
        reload=True
    )