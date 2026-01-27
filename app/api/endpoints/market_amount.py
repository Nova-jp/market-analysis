"""
市中残存額データAPI
市中残存額の残存年限別集計データの取得・加工
"""
from fastapi import APIRouter, HTTPException, Path, Query
from datetime import datetime
from typing import List, Optional
from app.core.database import db_manager
from app.core.models import (
    MarketAmountResponse, MarketAmountBucket, validate_date_format,
    BondTimeseriesResponse, BondTimeseriesPoint, BondTimeseriesStatistics,
    BondSearchResponse, BondSearchItem
)

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

        # 2. データベースクエリ (新テーブル bond_market_amount を参照)
        result = await db_manager.get_market_amount_data({
            'trade_date': f'eq.{date}',
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
                # 結合先の bond_data がない（銘柄詳細不明）場合はスキップ
                if not row.get('due_date'):
                    continue

                # 残存年限計算
                trade_dt = datetime.strptime(row['trade_date'], '%Y-%m-%d')
                due_dt = datetime.strptime(row['due_date'], '%Y-%m-%d')
                days_to_maturity = (due_dt - trade_dt).days
                years_to_maturity = days_to_maturity / 365.25

                # バケット判定
                bucket_index = int(years_to_maturity / bucket_size)

                # 範囲内のみ集計
                if 0 <= bucket_index < max_buckets:
                    market_amt = float(row['market_amount'])
                    buckets[bucket_index] += market_amt
                    total_amount += market_amt
                    bond_count += 1
            except (ValueError, TypeError, KeyError):
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


@router.get("/api/market-amount/bond/{bond_code}", response_model=BondTimeseriesResponse)
async def get_bond_market_amount_timeseries(
    bond_code: str = Path(..., description="銘柄コード (9桁)", min_length=9, max_length=9),
    start_date: Optional[str] = Query(None, description="開始日 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="終了日 (YYYY-MM-DD)")
):
    """
    銘柄ごとの市中残存額時系列データを取得

    Response:
    {
        "bond_code": "003720067",
        "bond_name": "第372回利付国債(10年)",
        "due_date": "2033-09-20",
        "timeseries": [
            {"trade_date": "2023-01-04", "market_amount": 123456},
            ...
        ],
        "statistics": {
            "latest_date": "2025-09-05",
            "latest_amount": 98765,
            "min_amount": 50000,
            "max_amount": 150000,
            "avg_amount": 100000,
            "data_points": 500
        }
    }
    """
    try:
        # パラメータ構築 (新テーブル bond_market_amount を参照)
        params = {
            'bond_code': f'eq.{bond_code}',
            'order': 'trade_date.asc',
            'limit': 10000
        }

        # 日付範囲フィルター
        if start_date and end_date:
            params['trade_date'] = f'gte.{start_date},lte.{end_date}'
        elif start_date:
            params['trade_date'] = f'gte.{start_date}'
        elif end_date:
            params['trade_date'] = f'lte.{end_date}'

        # データ取得 (修正後のメソッドを使用)
        result = await db_manager.get_market_amount_data(params)

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])

        if not result["data"]:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for bond_code: {bond_code}"
            )

        # 時系列データ抽出
        timeseries = [
            BondTimeseriesPoint(
                trade_date=row['trade_date'],
                market_amount=float(row['market_amount'])
            )
            for row in result["data"]
        ]

        # 統計情報計算
        amounts = [float(row['market_amount']) for row in result["data"]]
        statistics = BondTimeseriesStatistics(
            latest_date=result["data"][-1]['trade_date'],
            latest_amount=float(result["data"][-1]['market_amount']),
            min_amount=float(min(amounts)),
            max_amount=float(max(amounts)),
            avg_amount=round(sum(amounts) / len(amounts), 2),
            data_points=len(timeseries)
        )

        return BondTimeseriesResponse(
            bond_code=bond_code,
            bond_name=result["data"][0].get('bond_name', f"Bond {bond_code}"),
            due_date=result["data"][0].get('due_date', "N/A"),
            timeseries=timeseries,
            statistics=statistics
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/market-amount/bonds/search", response_model=BondSearchResponse)
async def search_bonds(
    query: Optional[str] = Query(None, description="検索キーワード (銘柄コードまたは名称)"),
    bond_type: Optional[str] = Query(None, description="債券種別フィルター (例: 10年債)"),
    limit: int = Query(100, ge=1, le=1000, description="取得件数")
):
    """
    銘柄一覧を取得（検索用）。全期間のユニークな銘柄を返します。
    """
    try:
        # 新しい専用メソッドを使用して、全期間からユニークな銘柄を取得
        result = await db_manager.get_unique_bonds(limit=limit, query=query)

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])

        bonds_list = [
            BondSearchItem(
                bond_code=row['bond_code'],
                bond_name=row.get('bond_name') or f"Bond {row['bond_code']}",
                due_date=row.get('due_date') or "N/A",
                latest_market_amount=float(row['latest_market_amount']),
                latest_trade_date=row['latest_trade_date']
            )
            for row in result["data"]
        ]

        return BondSearchResponse(
            bonds=bonds_list,
            count=len(bonds_list)
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
