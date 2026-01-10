"""
イールドカーブデータAPI
国債利回りデータの取得・加工
"""
from fastapi import APIRouter, HTTPException, Path
from datetime import datetime
from typing import List, Optional
import numpy as np
from scipy.interpolate import CubicSpline
from pydantic import BaseModel, Field
from app.core.database import db_manager
from app.core.models import YieldCurveResponse, BondYieldData, SwapYieldData, ASWCurveResponse, ASWData, validate_date_format

router = APIRouter()


class SwapCurveResponse(BaseModel):
    """スワップカーブレスポンスモデル"""
    date: str = Field(..., description="対象日付 (YYYY-MM-DD)")
    data: List[SwapYieldData] = Field(..., description="スワップイールドカーブデータ")
    error: Optional[str] = Field(None, description="エラーメッセージ")


def convert_tenor_to_years(tenor: str) -> Optional[float]:
    """期間文字列(1Y, 6M等)を年数(float)に変換"""
    if not tenor:
        return None
    
    tenor = tenor.upper().strip()
    try:
        if tenor.endswith('Y'):
            return float(tenor[:-1])
        elif tenor.endswith('M'):
            return float(tenor[:-1]) / 12.0
        elif tenor.endswith('W'):
            return float(tenor[:-1]) / 52.0
        elif tenor.endswith('D'):
            return float(tenor[:-1]) / 365.0
        # "3M(0x3)" などの特殊表記への簡易対応
        if 'M' in tenor: 
             # Extract number before M
             import re
             match = re.match(r'(\d+)M', tenor)
             if match:
                 return float(match.group(1)) / 12.0
        return None
    except ValueError:
        return None


@router.get("/api/yield-data/{date}", response_model=YieldCurveResponse)
async def get_yield_data(
    date: str = Path(..., description="取得日付 (YYYY-MM-DD形式)")
):
    """指定日のイールドカーブデータ取得 (国債のみ)"""
    try:
        # 日付形式チェック
        if not validate_date_format(date):
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )

        # データベースから取得
        result = await db_manager.get_bond_data({
            'select': 'trade_date,due_date,ave_compound_yield,bond_name,bond_code',
            'trade_date': f'eq.{date}',
            'ave_compound_yield': 'not.is.null',
            'due_date': 'not.is.null',
            'order': 'due_date.asc',
            'limit': 1000
        })

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])

        if not result["data"]:
            return YieldCurveResponse(
                date=date,
                data=[],
                error=f"No data found for {date}"
            )

        # 満期年数を計算してデータを処理
        processed_data: List[BondYieldData] = []
        for row in result["data"]:
            try:
                trade_dt = datetime.strptime(row['trade_date'], '%Y-%m-%d')
                due_dt = datetime.strptime(row['due_date'], '%Y-%m-%d')
                days_to_maturity = (due_dt - trade_dt).days
                years_to_maturity = days_to_maturity / 365.25

                if years_to_maturity > 0 and row['ave_compound_yield'] is not None:
                    bond_yield_data = BondYieldData(
                        maturity=round(years_to_maturity, 3),
                        yield_rate=float(row['ave_compound_yield']),
                        bond_name=row['bond_name'],
                        bond_code=row.get('bond_code')
                    )
                    processed_data.append(bond_yield_data)
            except (ValueError, TypeError):
                continue

        return YieldCurveResponse(
            date=date,
            data=processed_data
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/swap-data/{date}", response_model=SwapCurveResponse)
async def get_swap_data(
    date: str = Path(..., description="取得日付 (YYYY-MM-DD形式)")
):
    """指定日のスワップカーブデータ取得 (OISのみ)"""
    try:
        # 日付形式チェック
        if not validate_date_format(date):
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )

        # スワップデータ(OIS)を取得
        result = await db_manager.get_irs_data({
            'trade_date': f'eq.{date}',
            'product_type': 'eq.OIS',
            'limit': 100
        })

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])

        processed_data: List[SwapYieldData] = []
        if result["data"]:
            for row in result["data"]:
                try:
                    years = convert_tenor_to_years(row['tenor'])
                    if years is not None and row['rate'] is not None:
                        processed_data.append(SwapYieldData(
                            maturity=round(years, 3),
                            rate=float(row['rate']),
                            tenor=row['tenor']
                        ))
                except (ValueError, TypeError):
                    continue
            
            # 年限順にソート
            processed_data.sort(key=lambda x: x.maturity)

        return SwapCurveResponse(
            date=date,
            data=processed_data,
            error=None if processed_data else f"No data found for {date}"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/asw-data/{date}", response_model=ASWCurveResponse)
async def get_asw_data(
    date: str = Path(..., description="取得日付 (YYYY-MM-DD形式)")
):
    """指定日のASW (Asset Swap Spread) データ取得
    ASW_data テーブルから正確な計算結果を取得 (なければ簡易補間)
    """
    try:
        # 1. Try to get accurate data from ASW_data
        db_result = await db_manager.get_bond_spreads({'trade_date': f'eq.{date}'})
        
        if db_result["success"] and db_result["data"]:
            asw_results: List[ASWData] = []
            for row in db_result["data"]:
                try:
                    trade_dt = datetime.strptime(row['trade_date'], '%Y-%m-%d')
                    if row.get('maturity_date'):
                        due_dt = datetime.strptime(row['maturity_date'], '%Y-%m-%d')
                    else:
                        continue
                        
                    days = (due_dt - trade_dt).days
                    years = days / 365.25
                    
                    # Use Act/365 SA (Semi-Annual) as standard for display
                    asw_val = row.get('asw_act365_sa')
                    bond_yield = float(row['yield_compound'])
                    
                    if asw_val is None:
                        continue

                    # Derive swap_rate: swap_rate = yield - asw
                    swap_rate = bond_yield - float(asw_val)

                    asw_results.append(ASWData(
                        maturity=round(years, 3),
                        bond_code=row['bond_code'],
                        bond_name=row.get('bond_name', ''),
                        bond_yield=bond_yield,
                        swap_rate=round(swap_rate, 4),
                        asw=float(asw_val)
                    ))
                except Exception:
                    continue
            
            if asw_results:
                return ASWCurveResponse(date=date, data=asw_results)

        # Fallback to legacy on-the-fly calculation if DB has no data
        # 1. 国債データ取得
        bond_response = await get_yield_data(date)
        bond_data = bond_response.data

        # 2. スワップデータ取得
        swap_response = await get_swap_data(date)
        swap_data = swap_response.data

        if not bond_data or not swap_data:
            return ASWCurveResponse(
                date=date,
                data=[],
                error="Insufficient data for ASW calculation"
            )

        # 3. スプライン補間関数の作成
        # スワップデータを配列に変換 (x: 年限, y: レート)
        swap_maturities = np.array([s.maturity for s in swap_data])
        swap_rates = np.array([s.rate for s in swap_data])

        # データポイントが少なすぎる場合は計算不可 (最低3点など必要だが、CubicSplineは2点でも動作はする、ただし精度は低い)
        if len(swap_maturities) < 2:
             return ASWCurveResponse(
                date=date,
                data=[],
                error="Not enough swap data points for interpolation"
            )

        # 重複するxがある場合は平均を取るなどの処理が必要だが、
        # 通常OISデータはTenorごとにユニークはず。
        # ソート済みであることを前提
        
        try:
            # 自然スプライン補間 (bc_type='natural')
            spline = CubicSpline(swap_maturities, swap_rates, bc_type='natural')
        except Exception as e:
            return ASWCurveResponse(date=date, data=[], error=f"Interpolation failed: {str(e)}")

        # 4. 各国債についてASWを計算
        asw_results: List[ASWData] = []
        
        # 外挿の許容範囲 (例: スワップ最短〜最長の外側どれくらいまで許すか)
        # 今回は基本的に許容するが、極端な値にならないよう注意
        min_swap_mat = swap_maturities[0]
        max_swap_mat = swap_maturities[-1]

        for bond in bond_data:
            # 補間実行
            # CubicSplineはデフォルトで外挿可能
            interpolated_swap_rate = float(spline(bond.maturity))
            
            # ASW = 国債利回り - スワップレート
            # 結果は % 単位
            asw_val = bond.yield_rate - interpolated_swap_rate
            
            asw_results.append(ASWData(
                maturity=bond.maturity,
                bond_code=bond.bond_code,
                bond_name=bond.bond_name,
                bond_yield=bond.yield_rate,
                swap_rate=round(interpolated_swap_rate, 4),
                asw=round(asw_val, 4)
            ))

        return ASWCurveResponse(
            date=date,
            data=asw_results
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))