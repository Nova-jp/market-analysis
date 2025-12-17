#!/usr/bin/env python3
"""
日付・営業日ユーティリティモジュール

日本の営業日判定機能を提供。
土日・祝日を考慮した営業日計算をサポート。

Usage:
    from data.utils.date_utils import is_business_day, get_next_business_day

    today = datetime.now().date()
    if is_business_day(today):
        print("Today is a business day")
"""

from datetime import date, timedelta, datetime
from typing import Union
import jpholiday


def is_business_day(date_obj: Union[date, str]) -> bool:
    """
    営業日かどうか判定

    土日祝日は営業日ではないと判定します。

    Args:
        date_obj: 判定する日付（datetime.date または 'YYYY-MM-DD' 文字列）

    Returns:
        bool: 営業日の場合True、土日祝日の場合False

    Examples:
        >>> from datetime import date
        >>> is_business_day(date(2024, 1, 1))  # 元日
        False
        >>> is_business_day(date(2024, 1, 9))  # 平日
        True
    """
    # 文字列の場合は日付に変換
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()

    # 土日祝日は営業日ではない
    if date_obj.weekday() >= 5:  # 土曜=5, 日曜=6
        return False
    if jpholiday.is_holiday(date_obj):
        return False
    return True


def get_next_business_day(date_obj: Union[date, str]) -> date:
    """
    次の営業日を取得

    指定日付の翌営業日を返します。
    土日祝日をスキップして次の営業日を見つけます。

    Args:
        date_obj: 基準日（datetime.date または 'YYYY-MM-DD' 文字列）

    Returns:
        date: 次の営業日

    Examples:
        >>> from datetime import date
        >>> get_next_business_day(date(2024, 1, 5))  # 金曜日
        date(2024, 1, 9)  # 次の月曜日
    """
    # 文字列の場合は日付に変換
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()

    next_day = date_obj + timedelta(days=1)
    while not is_business_day(next_day):
        next_day += timedelta(days=1)
    return next_day


def get_previous_business_day(date_obj: Union[date, str]) -> date:
    """
    前の営業日を取得

    指定日付の前営業日を返します。
    土日祝日をスキップして前の営業日を見つけます。

    Args:
        date_obj: 基準日（datetime.date または 'YYYY-MM-DD' 文字列）

    Returns:
        date: 前の営業日

    Examples:
        >>> from datetime import date
        >>> get_previous_business_day(date(2024, 1, 9))  # 月曜日
        date(2024, 1, 5)  # 前の金曜日
    """
    # 文字列の場合は日付に変換
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()

    prev_day = date_obj - timedelta(days=1)
    while not is_business_day(prev_day):
        prev_day -= timedelta(days=1)
    return prev_day


def count_business_days(start_date: Union[date, str], end_date: Union[date, str]) -> int:
    """
    期間内の営業日数をカウント

    開始日と終了日の間（両端含む）の営業日数を返します。

    Args:
        start_date: 開始日（datetime.date または 'YYYY-MM-DD' 文字列）
        end_date: 終了日（datetime.date または 'YYYY-MM-DD' 文字列）

    Returns:
        int: 営業日数

    Examples:
        >>> from datetime import date
        >>> count_business_days(date(2024, 1, 1), date(2024, 1, 5))
        3
    """
    # 文字列の場合は日付に変換
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    count = 0
    current = start_date
    while current <= end_date:
        if is_business_day(current):
            count += 1
        current += timedelta(days=1)
    return count
