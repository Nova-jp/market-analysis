"""
日付関連API
営業日検索・クイック選択機能
"""
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta
from typing import List
from app.core.database import db_manager
from app.core.models import QuickDatesResponse, DateSearchResponse, validate_date_format

router = APIRouter()


@router.get("/api/search-dates", response_model=DateSearchResponse)
async def search_dates(
    q: str = Query("", description="検索クエリ (YYYY-MM-DD)"),
    table: str = Query("bond_data", description="参照テーブル (bond_data or bond_market_amount)")
):
    """日付検索API - YYYY-MM-DD形式での日付検索"""
    if len(q) < 4:
        return DateSearchResponse(dates=[])

    try:
        # 完全な日付形式の場合
        if len(q) == 10:
            if not validate_date_format(q):
                return DateSearchResponse(dates=[])

            result = await db_manager.get_bond_data({
                'trade_date': f'eq.{q}',
                'limit': 1
            }, table_name=table)

            if result["success"]:
                return DateSearchResponse(
                    dates=[item['trade_date'] for item in result["data"]]
                )

        # 部分入力の場合
        else:
            result = await db_manager.get_bond_data({
                'trade_date': f'gte.{q}',
                'order': 'trade_date.desc',
                'limit': 100
            }, table_name=table)

            if result["success"]:
                # 入力文字列で始まる日付のみフィルタリング
                filtered_data = [
                    item for item in result["data"]
                    if item['trade_date'].startswith(q)
                ]
                unique_dates = sorted(
                    list(set([item['trade_date'] for item in filtered_data])),
                    reverse=True
                )
                return DateSearchResponse(dates=unique_dates[:15])

        return DateSearchResponse(dates=[])

    except ValueError:
        return DateSearchResponse(dates=[])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/quick-dates", response_model=QuickDatesResponse)
async def get_quick_dates(
    table: str = Query("bond_data", description="参照テーブル (bond_data or bond_market_amount)")
):
    """
    クイック選択用の営業日を取得
    """
    try:
        # テーブル名検証
        if table not in ["bond_data", "bond_market_amount", "ASW_data"]:
            table = "bond_data"

        # ユニークな日付を効率的に取得するために直接SQLを実行
        # bond_dataは1日あたり数百行あるため、単純なlimit=100では過去の日付に到達しない
        sql = f'SELECT DISTINCT trade_date FROM "{table}" ORDER BY trade_date DESC LIMIT 30'
        
        # db_managerのsessionを使って実行
        from sqlalchemy import text
        from app.core.database import AsyncSessionLocal
        
        async with AsyncSessionLocal() as session:
            result = await session.execute(text(sql))
            unique_dates = [str(row[0]) for row in result]

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

        return QuickDatesResponse(**quick_dates)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

