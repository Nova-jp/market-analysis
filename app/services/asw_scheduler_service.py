"""
ASWスケジューラーサービス
日次のASW計算処理のビジネスロジックを提供
"""
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional
from sqlalchemy import text
from app.core.db import AsyncSessionLocal
from analysis.finance.quantlib_helper import QuantLibHelper
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
            async with AsyncSessionLocal() as session:
                # 1. データの存在確認 (OIS)
                ois_query = text("""
                    SELECT tenor, rate FROM irs_data 
                    WHERE trade_date = :date AND product_type = 'OIS'
                """)
                ois_result = await session.execute(ois_query, {"date": target_date})
                ois_data = [{'tenor': row[0], 'rate': float(row[1])} for row in ois_result]

                if len(ois_data) < 4:
                    return {
                        "status": "skipped",
                        "message": f"Insufficient OIS data for {date_str} (found {len(ois_data)})"
                    }

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

                # 3. QuantLibによる計算 (CPU集中的なため、同期的に実行)
                ql_helper = QuantLibHelper(date_str)
                ql_helper.build_ois_curve(ois_data)

                results = []
                for bond in bond_data:
                    try:
                        bond_yield = float(bond['ave_compound_yield'])
                        maturity_str = bond['due_date'].strftime("%Y-%m-%d")
                        
                        mms_pa = ql_helper.calculate_mms(maturity_str, 'Annual', 'Act365')
                        mms_sa = ql_helper.calculate_mms(maturity_str, 'Semiannual', 'Act365')
                        
                        if mms_pa is None or mms_sa is None:
                            continue

                        asw_pa = round(bond_yield - mms_pa, 4)
                        asw_sa = round(bond_yield - mms_sa, 4)

                        results.append({
                            "trade_date": target_date,
                            "bond_code": bond['bond_code'],
                            "asw_act365_pa": asw_pa,
                            "asw_act365_sa": asw_sa,
                            "calculation_log": json.dumps({"curve_points": len(ois_data)})
                        })

                    except:
                        continue

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
