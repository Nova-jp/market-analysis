#!/usr/bin/env python3
"""
Yield Curve API Endpoints
イールドカーブ分析用REST API（将来のWebアプリ化準備）
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from analysis.yield_curve_analyzer import YieldCurveAnalyzer

router = APIRouter(prefix="/api/yield-curve", tags=["yield-curve"])

# Pydanticモデル定義
class BondData(BaseModel):
    """債券データモデル"""
    bond_name: str
    bond_code: str
    years_to_maturity: float
    ave_compound_yield: float
    coupon_rate: Optional[float]
    ave_price: Optional[float]
    issue_type: int

class YieldCurveStats(BaseModel):
    """イールドカーブ統計モデル"""
    date: str
    total_bonds: int
    yield_stats: Dict[str, float]
    maturity_stats: Dict[str, float]
    period_stats: Dict[str, Dict[str, Any]]

class YieldCurveResponse(BaseModel):
    """イールドカーブレスポンスモデル"""
    date: str
    bonds: List[BondData]
    stats: YieldCurveStats

class DateRange(BaseModel):
    """日付範囲モデル"""
    available_dates: List[str]
    total_count: int
    latest_date: str
    oldest_date: str

# グローバルアナライザーインスタンス
analyzer = YieldCurveAnalyzer()

@router.get("/dates", response_model=DateRange)
async def get_available_dates(limit: int = Query(100, ge=1, le=1000)):
    """
    利用可能な日付一覧を取得
    
    Args:
        limit (int): 取得する日付数の上限
        
    Returns:
        DateRange: 利用可能な日付情報
    """
    try:
        import requests
        
        response = requests.get(
            f'{analyzer.db_manager.supabase_url}/rest/v1/clean_bond_data',
            params={
                'select': 'trade_date',
                'order': 'trade_date.desc',
                'limit': min(limit * 100, 10000)  # 余裕を持ってデータを取得
            },
            headers=analyzer.db_manager.headers
        )
        
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail="データベース接続エラー")
        
        data = response.json()
        dates = sorted(list(set([item['trade_date'] for item in data])), reverse=True)
        dates = dates[:limit]  # 指定された上限に調整
        
        if not dates:
            raise HTTPException(status_code=404, detail="利用可能な日付が見つかりません")
        
        return DateRange(
            available_dates=dates,
            total_count=len(dates),
            latest_date=dates[0],
            oldest_date=dates[-1]
        )
        
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"データ取得エラー: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"内部エラー: {str(e)}")

@router.get("/{date}", response_model=YieldCurveResponse)
async def get_yield_curve(date: str):
    """
    指定日のイールドカーブデータを取得
    
    Args:
        date (str): 対象日付 (YYYY-MM-DD)
        
    Returns:
        YieldCurveResponse: イールドカーブデータ
    """
    try:
        # 日付形式の検証
        datetime.strptime(date, '%Y-%m-%d')
        
        # データ分析実行
        result = analyzer.analyze_yield_curve(date, show_plots=False)
        
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        
        # データ変換
        df = result['data']
        bonds = []
        
        for _, row in df.iterrows():
            bond = BondData(
                bond_name=row['bond_name'],
                bond_code=row['bond_code'],
                years_to_maturity=float(row['years_to_maturity']),
                ave_compound_yield=float(row['ave_compound_yield']),
                coupon_rate=float(row['coupon_rate']) if row['coupon_rate'] is not None else None,
                ave_price=float(row['ave_price']) if row['ave_price'] is not None else None,
                issue_type=int(row['issue_type'])
            )
            bonds.append(bond)
        
        # 期間別統計計算
        short_term = df[df['years_to_maturity'] <= 2]
        medium_term = df[(df['years_to_maturity'] > 2) & (df['years_to_maturity'] <= 10)]
        long_term = df[df['years_to_maturity'] > 10]
        
        period_stats = {
            "short_term": {
                "count": len(short_term),
                "avg_yield": float(short_term['ave_compound_yield'].mean()) if len(short_term) > 0 else None,
                "description": "2年以下"
            },
            "medium_term": {
                "count": len(medium_term),
                "avg_yield": float(medium_term['ave_compound_yield'].mean()) if len(medium_term) > 0 else None,
                "description": "2-10年"
            },
            "long_term": {
                "count": len(long_term),
                "avg_yield": float(long_term['ave_compound_yield'].mean()) if len(long_term) > 0 else None,
                "description": "10年超"
            }
        }
        
        # 統計情報作成
        stats = YieldCurveStats(
            date=date,
            total_bonds=len(df),
            yield_stats=result['yield_stats'],
            maturity_stats=result['maturity_stats'],
            period_stats=period_stats
        )
        
        return YieldCurveResponse(
            date=date,
            bonds=bonds,
            stats=stats
        )
        
    except ValueError:
        raise HTTPException(status_code=400, detail="無効な日付形式です。YYYY-MM-DD形式で入力してください。")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"内部エラー: {str(e)}")

@router.get("/compare/dates")
async def compare_yield_curves(
    dates: str = Query(..., description="比較する日付のリスト（カンマ区切り）"),
    maturity_filter_min: float = Query(0, description="最小満期年数"),
    maturity_filter_max: float = Query(50, description="最大満期年数")
):
    """
    複数日のイールドカーブを比較
    
    Args:
        dates (str): 比較する日付（例: "2025-01-01,2025-01-02"）
        maturity_filter_min (float): 最小満期年数フィルター
        maturity_filter_max (float): 最大満期年数フィルター
        
    Returns:
        Dict: 複数日の比較データ
    """
    try:
        date_list = [d.strip() for d in dates.split(',')]
        
        if len(date_list) > 10:  # 上限設定
            raise HTTPException(status_code=400, detail="比較できる日付は10個までです")
        
        results = {}
        
        for target_date in date_list:
            try:
                # 日付形式の検証
                datetime.strptime(target_date, '%Y-%m-%d')
                
                # データ分析実行
                result = analyzer.analyze_yield_curve(target_date, show_plots=False)
                
                if "error" not in result:
                    df = result['data']
                    
                    # 満期フィルター適用
                    df_filtered = df[
                        (df['years_to_maturity'] >= maturity_filter_min) & 
                        (df['years_to_maturity'] <= maturity_filter_max)
                    ]
                    
                    # データポイントのみを返す（軽量化）
                    curve_data = []
                    for _, row in df_filtered.iterrows():
                        curve_data.append({
                            "maturity": float(row['years_to_maturity']),
                            "yield": float(row['ave_compound_yield']),
                            "bond_name": row['bond_name']
                        })
                    
                    results[target_date] = {
                        "curve_data": curve_data,
                        "stats": {
                            "count": len(df_filtered),
                            "avg_yield": float(df_filtered['ave_compound_yield'].mean()) if len(df_filtered) > 0 else None,
                            "min_yield": float(df_filtered['ave_compound_yield'].min()) if len(df_filtered) > 0 else None,
                            "max_yield": float(df_filtered['ave_compound_yield'].max()) if len(df_filtered) > 0 else None,
                        }
                    }
                else:
                    results[target_date] = {"error": result["error"]}
                    
            except ValueError:
                results[target_date] = {"error": "無効な日付形式"}
            except Exception as e:
                results[target_date] = {"error": str(e)}
        
        return {
            "comparison_data": results,
            "filters": {
                "maturity_min": maturity_filter_min,
                "maturity_max": maturity_filter_max
            },
            "requested_dates": date_list
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"内部エラー: {str(e)}")

@router.get("/stats/{date}")
async def get_yield_curve_stats(date: str):
    """
    指定日のイールドカーブ統計情報のみ取得（軽量版）
    
    Args:
        date (str): 対象日付 (YYYY-MM-DD)
        
    Returns:
        YieldCurveStats: 統計情報
    """
    try:
        # 日付形式の検証
        datetime.strptime(date, '%Y-%m-%d')
        
        # データ分析実行
        result = analyzer.analyze_yield_curve(date, show_plots=False)
        
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        
        # 期間別統計計算
        df = result['data']
        short_term = df[df['years_to_maturity'] <= 2]
        medium_term = df[(df['years_to_maturity'] > 2) & (df['years_to_maturity'] <= 10)]
        long_term = df[df['years_to_maturity'] > 10]
        
        period_stats = {
            "short_term": {
                "count": len(short_term),
                "avg_yield": float(short_term['ave_compound_yield'].mean()) if len(short_term) > 0 else None,
                "description": "2年以下"
            },
            "medium_term": {
                "count": len(medium_term),
                "avg_yield": float(medium_term['ave_compound_yield'].mean()) if len(medium_term) > 0 else None,
                "description": "2-10年"
            },
            "long_term": {
                "count": len(long_term),
                "avg_yield": float(long_term['ave_compound_yield'].mean()) if len(long_term) > 0 else None,
                "description": "10年超"
            }
        }
        
        return YieldCurveStats(
            date=date,
            total_bonds=len(df),
            yield_stats=result['yield_stats'],
            maturity_stats=result['maturity_stats'],
            period_stats=period_stats
        )
        
    except ValueError:
        raise HTTPException(status_code=400, detail="無効な日付形式です。YYYY-MM-DD形式で入力してください。")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"内部エラー: {str(e)}")