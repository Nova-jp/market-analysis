#!/usr/bin/env python3
"""
IMM フォワードレート・マトリックス計算ジョブ

10年以下のIMM日付（40本）の全ペア（780組）について
OIS フォワードパースワップレートを計算し imm_forward_matrix テーブルに保存する。

使い方:
  単一日:    ./venv/bin/python pipeline/jobs/calc_imm_forward_matrix.py --date 2026-05-25
  過去一括:  ./venv/bin/python pipeline/jobs/calc_imm_forward_matrix.py --from 2026-01-06 --to 2026-05-25
"""
import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime, date

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.db.sync_client import DatabaseManager
from core.calculations.bond_math import QuantLibHelper
from core.utils.date_utils import get_imm_strip_columns

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("calc_imm_forward_matrix")

# OIS カーブ構築に必要な最低データ点数
_MIN_OIS_POINTS = 4


def _build_ois_input(ois_rows: list) -> list:
    """sync_client の OIS 行リストを QuantLibHelper.build_ois_curve() 入力形式に変換"""
    return [
        {"tenor": r["tenor"], "rate": float(r["rate"])}
        for r in ois_rows
        if r.get("rate") is not None
    ]


def calculate_for_date(trade_date: date, db: DatabaseManager, force: bool = False) -> dict:
    """1日分の IMM フォワードマトリックスを計算して DB に保存する。

    Returns:
        {"status": "success"|"skipped"|"error", "message": str, "rows": int}
    """
    date_str = trade_date.strftime("%Y-%m-%d")

    if not force:
        existing = db.execute_query(
            "SELECT COUNT(*) FROM imm_forward_matrix WHERE trade_date = %s",
            (trade_date,),
        )
        if existing and existing[0][0] > 0:
            return {"status": "skipped", "message": f"{date_str}: already calculated ({existing[0][0]} rows)", "rows": 0}

    ois_rows = db.get_ois_data(start_date=date_str, end_date=date_str)
    ois_input = _build_ois_input(ois_rows)
    if len(ois_input) < _MIN_OIS_POINTS:
        return {"status": "skipped", "message": f"{date_str}: insufficient OIS data ({len(ois_input)} points)", "rows": 0}

    try:
        ql_helper = QuantLibHelper(date_str)
        ql_helper.build_ois_curve(ois_input)
    except Exception as e:
        return {"status": "error", "message": f"{date_str}: OIS curve build failed: {e}", "rows": 0}

    imm_list = get_imm_strip_columns(trade_date, years=10, max_cols=40)
    if len(imm_list) < 2:
        return {"status": "skipped", "message": f"{date_str}: not enough IMM dates", "rows": 0}

    records = []
    for i, (code_s, date_s) in enumerate(imm_list):
        for j in range(i + 1, len(imm_list)):
            code_e, date_e = imm_list[j]
            tenor_months = (j - i) * 3
            try:
                rate = ql_helper.calculate_forward_between_dates(date_s, date_e)
            except Exception:
                rate = None
            records.append({
                "trade_date": trade_date,
                "start_imm_code": code_s,
                "end_imm_code": code_e,
                "start_date": date_s,
                "end_date": date_e,
                "forward_rate": rate,
                "tenor_months": tenor_months,
            })

    inserted = db.batch_insert_data(
        records,
        table_name="imm_forward_matrix",
        batch_size=200,
        conflict_target="(trade_date, start_imm_code, end_imm_code)",
        update_columns=["forward_rate"],
    )
    return {"status": "success", "message": f"{date_str}: {inserted} rows upserted", "rows": inserted}


def get_ois_available_dates(db: DatabaseManager, from_date: date, to_date: date) -> list[date]:
    """指定範囲で OIS データが存在する日付一覧を返す"""
    rows = db.execute_query(
        """
        SELECT DISTINCT trade_date
        FROM irs_data
        WHERE product_type = 'OIS'
          AND trade_date >= %s
          AND trade_date <= %s
        ORDER BY trade_date ASC
        """,
        (from_date, to_date),
    )
    return [row[0] if isinstance(row[0], date) else datetime.strptime(str(row[0]), "%Y-%m-%d").date() for row in rows]


def main():
    parser = argparse.ArgumentParser(description="IMM フォワードマトリックス計算")
    parser.add_argument("--date", type=str, help="単一日 (YYYY-MM-DD)")
    parser.add_argument("--from", dest="from_date", type=str, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", type=str, help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="既存データを上書き")
    args = parser.parse_args()

    db = DatabaseManager()

    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
        result = calculate_for_date(target, db, force=args.force)
        logger.info(f"[{result['status'].upper()}] {result['message']}")
        sys.exit(0 if result["status"] in ("success", "skipped") else 1)

    if args.from_date and args.to_date:
        from_date = datetime.strptime(args.from_date, "%Y-%m-%d").date()
        to_date = datetime.strptime(args.to_date, "%Y-%m-%d").date()
        dates = get_ois_available_dates(db, from_date, to_date)
        logger.info(f"対象日数: {len(dates)} ({from_date} 〜 {to_date})")
        success = skipped = errors = 0
        for d in dates:
            result = calculate_for_date(d, db, force=args.force)
            if result["status"] == "success":
                success += 1
            elif result["status"] == "skipped":
                skipped += 1
            else:
                errors += 1
            logger.info(f"[{result['status'].upper()}] {result['message']}")
        logger.info(f"完了: success={success}, skipped={skipped}, errors={errors}")
        sys.exit(0 if errors == 0 else 1)

    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()
