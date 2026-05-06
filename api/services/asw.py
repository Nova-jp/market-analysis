"""
ASWスケジューラーサービス
日次のASW計算処理のビジネスロジックを提供
"""
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional
from sqlalchemy import text
from starlette.concurrency import run_in_threadpool
from core.db.engine import AsyncSessionLocal
from core.calculations.bond_math import QuantLibHelper
from core.db.sync_client import DatabaseManager
import json

logger = logging.getLogger(__name__)

class ASWSchedulerService:
    """日次ASW計算を実行するサービスクラス"""

    async def calculate_daily_asw(self, target_date: Optional[date] = None) -> Dict[str, Any]:
        """
        指定された日付（デフォルトは今日）のASWを計算してDBに保存
        """
        if target_date is None:
            target_date = datetime.now().date()
        
        date_str = target_date.strftime("%Y-%m-%d")
        logger.info(f"Starting daily ASW calculation for {date_str}")

        try:
            # 1. データの存在確認 (OIS) — 同期DBManagerをスレッドプールで実行してイベントループをブロックしない
            db = DatabaseManager()
            rows = await run_in_threadpool(db.get_ois_data, start_date=date_str, end_date=date_str)
            ois_data = [{'tenor': row['tenor'], 'rate': float(row['rate'])} for row in rows]

            if len(ois_data) < 4:
                return {
                    "status": "skipped",
                    "message": f"Insufficient OIS data for {date_str} (found {len(ois_data)})"
                }

            async with AsyncSessionLocal() as session:
                # 2. データの存在確認 (国債)
                bond_query = text("""
                    SELECT bond_code, due_date, ave_compound_yield FROM bond_data 
                    WHERE trade_date = :date AND ave_compound_yield IS NOT NULL
                """)
                bond_result = await session.execute(bond_query, {"date": target_date})
                bond_data = [dict(row._mapping) for row in bond_result]

                if not bond_data:
                    return {
                        "status": "skipped",
                        "message": f"No bond data found for {date_str}"
                    }

                # 3. QuantLibによる計算 (CPU集中的なため、スレッドプールで実行)
                def _compute_asw():
                    ql_helper = QuantLibHelper(date_str)
                    ql_helper.build_ois_curve(ois_data)
                    calc_results = []
                    for bond in bond_data:
                        try:
                            bond_yield = float(bond['ave_compound_yield'])
                            maturity_str = bond['due_date'].strftime("%Y-%m-%d")
                            mms_pa = ql_helper.calculate_mms(maturity_str, 'Annual', 'Act365')
                            mms_sa = ql_helper.calculate_mms(maturity_str, 'Semiannual', 'Act365')
                            if mms_pa is None or mms_sa is None:
                                continue
                            calc_results.append({
                                "trade_date": target_date,
                                "bond_code": bond['bond_code'],
                                "asw_act365_pa": round(bond_yield - mms_pa, 4),
                                "asw_act365_sa": round(bond_yield - mms_sa, 4),
                                "calculation_log": json.dumps({"curve_points": len(ois_data)})
                            })
                        except Exception:
                            continue
                    return calc_results

                results = await run_in_threadpool(_compute_asw)

                if not results:
                    return {"status": "success", "message": "No bonds to calculate", "count": 0}

                # 4. DB保存 (ASW_dataテーブル)
                insert_stmt = text("""
                    INSERT INTO "ASW_data" (
                        trade_date, bond_code, asw_act365_pa, asw_act365_sa, calculation_log
                    ) VALUES (
                        :trade_date, :bond_code, :asw_act365_pa, :asw_act365_sa, :calculation_log
                    )
                    ON CONFLICT (trade_date, bond_code) DO UPDATE SET
                        asw_act365_pa = EXCLUDED.asw_act365_pa,
                        asw_act365_sa = EXCLUDED.asw_act365_sa,
                        calculation_log = EXCLUDED.calculation_log,
                        updated_at = CURRENT_TIMESTAMP
                """)
                
                # SQLAlchemyのexecuteはリストを渡すとバッチ処理になる
                await session.execute(insert_stmt, results)
                await session.commit()

                return {
                    "status": "success",
                    "message": f"Calculated ASW for {len(results)} bonds on {date_str}",
                    "count": len(results)
                }

        except Exception as e:
            logger.error(f"ASW calculation error for {date_str}: {e}")
            return {"status": "error", "message": str(e)}
