#!/usr/bin/env python3
"""
スワップカーブ補間比較分析: スプライン vs 線形 (1Y-2Y間)
過去100日の各日で、スプライン補間と線形補間の差の最大値を求める。
18Mテナーは除外。
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np
from scipy.interpolate import CubicSpline
from core.config import settings


def tenor_to_years(tenor: str) -> float | None:
    """テナー文字列を年数に変換。1D/1W等の短期は None を返す"""
    if tenor.endswith('D'):
        return None  # 日次テナーは除外
    elif tenor.endswith('W'):
        return None  # 週次テナーは除外
    elif tenor.endswith('M'):
        return int(tenor[:-1]) / 12.0
    elif tenor.endswith('Y'):
        return float(tenor[:-1])
    return None


def main():
    conn = psycopg2.connect(settings.database_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 過去100取引日を取得
            cur.execute("""
                SELECT DISTINCT trade_date
                FROM irs_data
                WHERE product_type = 'OIS'
                ORDER BY trade_date DESC
                LIMIT 100
            """)
            dates = [row['trade_date'] for row in cur.fetchall()]

            if not dates:
                print("データが見つかりません")
                return

            print(f"分析対象: {len(dates)}日 ({dates[-1]} ～ {dates[0]})")

            # 各日のスワップデータを取得（6M, 1Y, 2Y, 3Y のみ）
            cur.execute("""
                SELECT trade_date, tenor, rate
                FROM irs_data
                WHERE product_type = 'OIS'
                  AND trade_date = ANY(%s)
                  AND tenor IN ('6M', '1Y', '2Y', '3Y')
                ORDER BY trade_date DESC,
                         CASE
                           WHEN tenor LIKE '%%M' THEN CAST(REPLACE(tenor,'M','') AS INTEGER) * 30
                           WHEN tenor LIKE '%%Y' THEN CAST(REPLACE(tenor,'Y','') AS INTEGER) * 365
                         END ASC
            """, (dates,))
            rows = cur.fetchall()

    finally:
        conn.close()

    # 日付ごとにグループ化
    from collections import defaultdict
    by_date = defaultdict(list)
    for row in rows:
        by_date[row['trade_date']].append(row)

    # 評価点: 1Yと2Yの間を1000分割
    eval_points = np.linspace(1.0, 2.0, 1000)

    results = []
    skipped = 0

    for trade_date in sorted(by_date.keys()):
        tenors_data = by_date[trade_date]

        # テナーを年数に変換（1D/1W/2W/3W は除外）
        curve = []
        for r in tenors_data:
            years = tenor_to_years(r['tenor'])
            if years is not None:
                curve.append((years, float(r['rate'])))

        # 6M, 1Y, 2Y, 3Y が全て揃っているか確認（浮動小数点対策: round）
        maturities_set = {round(m, 6) for m, _ in curve}
        required = {round(0.5, 6), round(1.0, 6), round(2.0, 6), round(3.0, 6)}
        if not required.issubset(maturities_set):
            skipped += 1
            continue

        # ソート
        curve.sort(key=lambda x: x[0])
        x = np.array([m for m, _ in curve])
        y = np.array([r for _, r in curve])

        # 1Y と 2Y の前後テナーを含む全カーブでスプライン
        # スプラインには全テナーを使用
        try:
            spline = CubicSpline(x, y, bc_type='natural')
        except Exception as e:
            skipped += 1
            continue

        # 線形補間は 1Y-2Y 間のみ（2点）
        y_1y = y[x == 1.0][0]
        y_2y = y[x == 2.0][0]

        spline_vals = spline(eval_points)
        linear_vals = y_1y + (y_2y - y_1y) * (eval_points - 1.0) / (2.0 - 1.0)

        diff = np.abs(spline_vals - linear_vals)
        max_diff = diff.max()
        max_point = eval_points[diff.argmax()]

        results.append({
            'date': trade_date,
            'max_diff_bp': max_diff * 100,  # %→bp
            'max_diff_pct': max_diff,
            'at_years': max_point,
            'rate_1y': y_1y,
            'rate_2y': y_2y,
        })

    if not results:
        print("有効なデータがありませんでした")
        return

    print(f"\nスキップ: {skipped}日 (1Y/2Yデータ不足)")
    print(f"有効分析: {len(results)}日\n")

    # 結果サマリー
    diffs = [r['max_diff_bp'] for r in results]
    print("=" * 60)
    print("スプライン vs 線形補間 差異サマリー (1Y-2Y間, 使用テナー: 6M/1Y/2Y/3Y)")
    print("=" * 60)
    print(f"  最大差異:   {max(diffs):.4f} bp")
    print(f"  平均差異:   {np.mean(diffs):.4f} bp")
    print(f"  中央値:     {np.median(diffs):.4f} bp")
    print(f"  最小差異:   {min(diffs):.4f} bp")
    print()

    # 最大差異が大きかった上位10日
    top10 = sorted(results, key=lambda r: r['max_diff_bp'], reverse=True)[:10]
    print("差異が大きかった上位10日:")
    print(f"  {'日付':<12} {'最大差(bp)':>10} {'最大点(年)':>10} {'1Y率(%)':>8} {'2Y率(%)':>8}")
    print(f"  {'-'*12} {'-'*10} {'-'*10} {'-'*8} {'-'*8}")
    for r in top10:
        print(f"  {str(r['date']):<12} {r['max_diff_bp']:>10.4f} {r['at_years']:>10.4f} "
              f"{r['rate_1y']:>8.4f} {r['rate_2y']:>8.4f}")

    print()

    # 最近10日
    recent10 = sorted(results, key=lambda r: r['date'], reverse=True)[:10]
    print("最近10日の差異:")
    print(f"  {'日付':<12} {'最大差(bp)':>10} {'最大点(年)':>10} {'1Y率(%)':>8} {'2Y率(%)':>8}")
    print(f"  {'-'*12} {'-'*10} {'-'*10} {'-'*8} {'-'*8}")
    for r in recent10:
        print(f"  {str(r['date']):<12} {r['max_diff_bp']:>10.4f} {r['at_years']:>10.4f} "
              f"{r['rate_1y']:>8.4f} {r['rate_2y']:>8.4f}")


if __name__ == '__main__':
    main()
