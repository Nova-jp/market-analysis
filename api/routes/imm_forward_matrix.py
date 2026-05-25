"""
IMM フォワードマトリックス API
GET /api/imm-forward-matrix/history  — 全履歴（クライアント側 Z-score 計算用）

レスポンス形式:
  trade_dates: ["2025-07-24", ...]           — 200 営業日 (昇順)
  snapshots:   [{codes, imm_dates, rates}, ...]

  codes     — そのトレード日の 40 個の IMM コード（時系列順）
  imm_dates — codes に対応する実際のカレンダー日付
  rates     — 上三角行列を行優先フラット化した 780 要素
              インデックス k(i,j) = (n-1)*i - i*(i-1)//2 + (j-i-1)  (j > i)
"""
from collections import defaultdict
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from core.db.engine import AsyncSessionLocal

router = APIRouter()


@router.get("/history")
async def get_imm_forward_history(
    days: int = Query(200, ge=1, le=200, description="取得する最大営業日数"),
):
    """
    直近 N 営業日分の IMM フォワードマトリックス全履歴を返す。
    クライアント側でスナップショットをキャッシュし、日付選択・Z-score ウィンドウ変更を
    ネットワーク往復なしに行うための設計（Load Once, Compute Everywhere）。
    """
    try:
        async with AsyncSessionLocal() as session:
            rows = await session.execute(text("""
                WITH target_dates AS (
                    SELECT DISTINCT trade_date
                    FROM imm_forward_matrix
                    ORDER BY trade_date DESC
                    LIMIT :days
                )
                SELECT
                    m.trade_date::text  AS trade_date,
                    m.start_imm_code,
                    m.end_imm_code,
                    m.start_date::text  AS start_date,
                    m.end_date::text    AS end_date,
                    m.forward_rate
                FROM imm_forward_matrix m
                INNER JOIN target_dates d ON m.trade_date = d.trade_date
                ORDER BY m.trade_date ASC, m.start_date ASC, m.end_date ASC
            """), {"days": days})
            all_rows = rows.fetchall()

        if not all_rows:
            raise HTTPException(status_code=404, detail="No IMM forward matrix data found.")

        by_date: dict = defaultdict(list)
        for r in all_rows:
            by_date[r.trade_date].append(r)

        trade_dates = sorted(by_date.keys())
        snapshots = []

        for td in trade_dates:
            date_rows = by_date[td]

            # start_date / end_date でコードを時系列ソート
            code_to_caldate: dict[str, str] = {}
            for r in date_rows:
                code_to_caldate[r.start_imm_code] = r.start_date
                code_to_caldate[r.end_imm_code]   = r.end_date

            codes     = [c for c, _ in sorted(code_to_caldate.items(), key=lambda x: x[1])]
            imm_dates = [code_to_caldate[c] for c in codes]
            n         = len(codes)

            code_idx   = {c: i for i, c in enumerate(codes)}
            total_pairs = n * (n - 1) // 2
            rates: list = [None] * total_pairs

            for r in date_rows:
                i = code_idx.get(r.start_imm_code)
                j = code_idx.get(r.end_imm_code)
                if i is not None and j is not None and j > i:
                    k = (n - 1) * i - i * (i - 1) // 2 + (j - i - 1)
                    rates[k] = float(r.forward_rate) if r.forward_rate is not None else None

            snapshots.append({
                "codes":     codes,
                "imm_dates": imm_dates,
                "rates":     rates,
            })

        return {"trade_dates": trade_dates, "snapshots": snapshots}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
