"""
イールドカーブデータAPI
国債利回りデータの取得・加工
"""
from fastapi import APIRouter, HTTPException, Path
from datetime import datetime
from typing import List
from app.core.database import db_manager
from app.core.models import YieldCurveResponse, BondYieldData, validate_date_format

router = APIRouter()


@router.get("/api/yield-data/{date}", response_model=YieldCurveResponse)
async def get_yield_data(
    date: str = Path(..., description="取得日付 (YYYY-MM-DD形式)")
):
    """指定日のイールドカーブデータ取得と満期年数計算"""
    try:
        # 日付形式チェック
        if not validate_date_format(date):
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )

        # データベースから取得
        result = await db_manager.get_bond_data({
            'select': 'trade_date,due_date,ave_compound_yield,bond_name',
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
                        bond_name=row['bond_name']
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