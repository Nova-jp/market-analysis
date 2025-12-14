"""
市中残存額データAPI
市中残存額の残存年限別集計データの取得・加工
"""
from fastapi import APIRouter, HTTPException, Path, Query
from datetime import datetime
from typing import List
from app.core.database import db_manager
from app.core.models import MarketAmountResponse, MarketAmountBucket, validate_date_format

router = APIRouter()


@router.get("/api/market-amount/{date}", response_model=MarketAmountResponse)
async def get_market_amount(
    date: str = Path(..., description="取得日付 (YYYY-MM-DD形式)"),
    bucket_size: float = Query(1.0, description="区切り幅（年）", ge=0.25, le=10)
):
    """指定日の市中残存額分布データ取得（可変区切り集計）"""
    try:
        # 1. 日付形式検証
        if not validate_date_format(date):
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )

        # 2. データベースクエリ
        result = await db_manager.get_bond_data({
            'select': 'trade_date,due_date,market_amount,bond_code',
            'trade_date': f'eq.{date}',
            'market_amount': 'not.is.null',
            'due_date': 'not.is.null',
            'limit': 10000
        })

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])

        if not result["data"]:
            return MarketAmountResponse(
                date=date,
                buckets=[],
                total_amount=0,
                bond_count=0,
                error=f"No data found for {date}"
            )

        # 3. バケット集計（可変区切り幅対応）
        max_years = 40
        max_buckets = int(max_years / bucket_size)
        buckets = [0.0] * max_buckets
        total_amount = 0
        bond_count = 0

        for row in result["data"]:
            try:
                # 残存年限計算（yield_data.pyと同じロジック）
                trade_dt = datetime.strptime(row['trade_date'], '%Y-%m-%d')
                due_dt = datetime.strptime(row['due_date'], '%Y-%m-%d')
                days_to_maturity = (due_dt - trade_dt).days
                years_to_maturity = days_to_maturity / 365.25

                # バケット判定（可変区切り幅）
                bucket_index = int(years_to_maturity / bucket_size)

                # 範囲内のみ集計
                if 0 <= bucket_index < max_buckets:
                    market_amt = float(row['market_amount'])
                    buckets[bucket_index] += market_amt
                    total_amount += market_amt
                    bond_count += 1
            except (ValueError, TypeError):
                continue

        # 4. レスポンス生成
        bucket_data = [
            MarketAmountBucket(year=i, amount=round(buckets[i], 2))
            for i in range(max_buckets)
        ]

        return MarketAmountResponse(
            date=date,
            buckets=bucket_data,
            total_amount=round(total_amount, 2),
            bond_count=bond_count
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
