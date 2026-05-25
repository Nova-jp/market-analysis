#!/usr/bin/env python3
"""
日付・営業日ユーティリティ
土日・祝日を考慮した営業日計算をサポート。
"""
from datetime import date, timedelta, datetime
from typing import Union
import jpholiday


def third_wednesday(year: int, month: int) -> date:
    """指定年月の第3水曜日を返す"""
    d = date(year, month, 1)
    days_to_wed = (2 - d.weekday()) % 7
    return d + timedelta(days=days_to_wed + 14)


def get_imm_strip_columns(
    from_date: date,
    years: int = 10,
    max_cols: int = 40,
) -> list[tuple[str, str]]:
    """from_date 翌日以降の IMM 日付を生成。戻り値: [(code, date_str), ...]

    IMM 月: 3(H), 6(M), 9(U), 12(Z)。years=10, max_cols=40 で 10 年分 ≈ 40 列。
    """
    MONTH_CODE = {3: "H", 6: "M", 9: "U", 12: "Z"}
    IMM_MONTHS = [3, 6, 9, 12]
    end_date = date(from_date.year + years, 12, 31)

    results = []
    for year in range(from_date.year, from_date.year + years + 2):
        for month in IMM_MONTHS:
            imm_date = third_wednesday(year, month)
            if imm_date <= from_date:
                continue
            if imm_date > end_date:
                return results[:max_cols]
            code = f"{MONTH_CODE[month]}{str(year)[-2:]}"
            results.append((code, str(imm_date)))

    return results[:max_cols]


def is_business_day(date_obj: Union[date, str]) -> bool:
    """営業日かどうか判定（土日祝日は False）"""
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
    if date_obj.weekday() >= 5:
        return False
    if jpholiday.is_holiday(date_obj):
        return False
    return True


def get_next_business_day(date_obj: Union[date, str]) -> date:
    """次の営業日を取得"""
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
    next_day = date_obj + timedelta(days=1)
    while not is_business_day(next_day):
        next_day += timedelta(days=1)
    return next_day


def get_previous_business_day(date_obj: Union[date, str]) -> date:
    """前の営業日を取得"""
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
    prev_day = date_obj - timedelta(days=1)
    while not is_business_day(prev_day):
        prev_day -= timedelta(days=1)
    return prev_day


def count_business_days(start_date: Union[date, str], end_date: Union[date, str]) -> int:
    """期間内の営業日数をカウント（両端含む）"""
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
