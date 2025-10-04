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
async def search_dates(q: str = Query("", description="検索クエリ (YYYY-MM-DD)")):
    """日付検索API - YYYY-MM-DD形式での日付検索"""
    if len(q) < 4:
        return DateSearchResponse(dates=[])

    try:
        # 完全な日付形式の場合
        if len(q) == 10:
            if not validate_date_format(q):
                return DateSearchResponse(dates=[])

            result = await db_manager.get_bond_data({
                'select': 'trade_date',
                'trade_date': f'eq.{q}',
                'limit': 1
            })

            if result["success"]:
                return DateSearchResponse(
                    dates=[item['trade_date'] for item in result["data"]]
                )

        # 部分入力の場合
        else:
            result = await db_manager.get_bond_data({
                'select': 'trade_date',
                'trade_date': f'gte.{q}',
                'order': 'trade_date.desc',
                'limit': 50
            })

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
async def get_quick_dates():
    """
    クイック選択用の営業日を取得（最新、前日、5営業日前、1ヶ月前）
    シングルクエリ+ソート方式でパフォーマンス最適化
    """
    try:
        # 全ユニーク日付を一度取得（約30営業日分）
        result = await db_manager.get_bond_data({
            'select': 'trade_date',
            'order': 'trade_date.desc',
            'limit': 10000  # 約30営業日分をカバー (300レコード/日 × 30日)
        })

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])

        if not result["data"]:
            raise HTTPException(
                status_code=404,
                detail="No trading dates found in database"
            )

        # ユニークな日付リストを作成（降順ソート維持）
        unique_dates = list(dict.fromkeys([
            item['trade_date'] for item in result["data"]
        ]))

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