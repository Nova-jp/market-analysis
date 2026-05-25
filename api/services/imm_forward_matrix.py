"""
IMM フォワードマトリックス・スケジューラーサービス
日次の IMM フォワードレート計算処理のビジネスロジックを提供する。
"""
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional
from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)


class IMMForwardMatrixService:
    """日次 IMM フォワードマトリックス計算を実行するサービスクラス"""

    async def calculate_daily(self, target_date: Optional[date] = None) -> Dict[str, Any]:
        """
        指定日（デフォルトは今日）の IMM フォワードマトリックスを計算して DB に保存する。
        pipeline/jobs/calc_imm_forward_matrix.py の calculate_for_date() をスレッドプールで実行。
        """
        if target_date is None:
            target_date = datetime.now().date()

        logger.info(f"Starting IMM forward matrix calculation for {target_date}")

        def _run():
            from core.db.sync_client import DatabaseManager
            from pipeline.jobs.calc_imm_forward_matrix import calculate_for_date
            db = DatabaseManager()
            return calculate_for_date(target_date, db, force=False)

        try:
            result = await run_in_threadpool(_run)
            log_level = logging.INFO if result["status"] in ("success", "skipped") else logging.ERROR
            logger.log(log_level, f"IMM forward matrix [{result['status']}]: {result['message']}")
            return result
        except Exception as e:
            msg = f"Unexpected error in IMM forward matrix calculation: {e}"
            logger.error(msg, exc_info=True)
            return {"status": "error", "message": msg, "rows": 0}
