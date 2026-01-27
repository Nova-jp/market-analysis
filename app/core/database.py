"""
統一データベース接続管理システム（Web API用 - Neon対応版）
SQLAlchemy + asyncpg を使用して Neon (PostgreSQL) に直接接続

注意: このモジュールはWeb API用（async対応）
スクリプト/バッチ処理では data/utils/database_manager.py を使用すること
"""
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime, date
from decimal import Decimal
from sqlalchemy import text
from app.core.db import AsyncSessionLocal
import json

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Web API用データベース管理クラス（SQLAlchemy非同期版）
    Supabase REST APIからNeon直接接続へ移行
    """

    def __init__(self):
        """
        初期化
        """
        pass
    
    def _parse_date(self, date_str: str) -> date:
        """文字列を日付オブジェクトに変換"""
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            return date_str
    
    def _convert_types(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        データベースからの取得データをJSONシリアライズ可能な型に変換
        - datetime.date -> str
        - Decimal -> float
        """
        for row in data:
            for key, value in row.items():
                if isinstance(value, date):
                    row[key] = str(value)
                elif isinstance(value, Decimal):
                    row[key] = float(value)
        return data

    async def execute_query(self, table: str = "bond_data",
                          params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        データベースクエリ実行（互換性維持のためのラッパー）
        """
        if params is None:
            params = {}
            
        limit = params.get("limit", 100)
        offset = params.get("offset", 0)
        
        allowed_tables = ["bond_data", "bond_auction", "economic_indicators", "boj_holdings", "irs_data", "bond_market_amount"]
        if table not in allowed_tables:
            return {"success": False, "error": f"Invalid table name: {table}"}

        try:
            async with AsyncSessionLocal() as session:
                order_clause = ""
                order_param = params.get("order")
                if order_param:
                    if "trade_date.desc" in order_param:
                        order_clause = "ORDER BY trade_date DESC"
                    elif "trade_date.asc" in order_param:
                        order_clause = "ORDER BY trade_date ASC"
                
                if not order_clause and table == "bond_data":
                    order_clause = "ORDER BY trade_date DESC"

                query_str = f"SELECT * FROM {table} {order_clause} LIMIT :limit OFFSET :offset"
                
                stmt = text(query_str)
                result = await session.execute(stmt, {"limit": limit, "offset": offset})
                
                data = [dict(row._mapping) for row in result]
                
                # 型変換
                data = self._convert_types(data)
                
                logger.info(f"Query successful: {len(data)} records from {table}")
                return {"success": True, "data": data}

        except Exception as e:
            error_msg = f"Database error: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

    async def get_bond_data(self, params: Dict[str, Any], table_name: str = "bond_data") -> Dict[str, Any]:
        """
        国債データを取得する専用メソッド
        APIエンドポイントからのリクエストパラメータを処理
        """
        # 許可されたテーブルのみに制限
        allowed_tables = ["bond_data", "bond_market_amount"]
        if table_name not in allowed_tables:
            table_name = "bond_data"

        try:
            limit = params.get("limit", 1000)
            conditions = []
            query_params = {"limit": limit}
            
            if "trade_date" in params:
                val = params["trade_date"]
                if val.startswith("eq."):
                    conditions.append("trade_date = :trade_date")
                    query_params["trade_date"] = self._parse_date(val[3:])
                elif val.startswith("gte."):
                    conditions.append("trade_date >= :start_date")
                    query_params["start_date"] = self._parse_date(val[4:])
                elif val.startswith("lte."):
                    conditions.append("trade_date <= :end_date")
                    query_params["end_date"] = self._parse_date(val[4:])
                else:
                    conditions.append("trade_date = :trade_date")
                    query_params["trade_date"] = self._parse_date(val)
                
            if "start_date" in params:
                conditions.append("trade_date >= :start_date")
                query_params["start_date"] = self._parse_date(params["start_date"])
                
            if "end_date" in params:
                conditions.append("trade_date <= :end_date")
                query_params["end_date"] = self._parse_date(params["end_date"])

            if "bond_code" in params:
                val = params["bond_code"]
                if val.startswith("eq."):
                    conditions.append("bond_code = :bond_code")
                    query_params["bond_code"] = val[3:]

            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

            order_clause = "ORDER BY trade_date DESC, bond_code ASC"
            if "order" in params:
                order_param = params["order"]
                if "trade_date.desc" in order_param:
                    order_clause = "ORDER BY trade_date DESC"
                elif "trade_date.asc" in order_param:
                    order_clause = "ORDER BY trade_date ASC"

            sql = f"""
                SELECT * FROM {table_name} 
                {where_clause}
                {order_clause}
                LIMIT :limit
            """
            
            async with AsyncSessionLocal() as session:
                stmt = text(sql)
                result = await session.execute(stmt, query_params)
                data = [dict(row._mapping) for row in result]
                
                if data:
                    logger.debug(f"Query on {table_name} returned {len(data)} rows")
                    # 型変換
                    data = self._convert_types(data)
                
                return {"success": True, "data": data}

        except Exception as e:
            logger.error(f"get_bond_data error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "error": str(e)}

    async def get_market_amount_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        市中残存額データを取得する専用メソッド
        bond_market_amount と bond_data を JOIN して取得
        """
        try:
            limit = params.get("limit", 1000)
            conditions = []
            query_params = {"limit": limit}
            
            # 取引日フィルタ
            if "trade_date" in params:
                val = params["trade_date"]
                if val.startswith("eq."):
                    conditions.append("m.trade_date = :trade_date")
                    query_params["trade_date"] = self._parse_date(val[3:])
                elif val.startswith("gte."):
                    conditions.append("m.trade_date >= :start_date")
                    query_params["start_date"] = self._parse_date(val[4:])
                elif val.startswith("lte."):
                    conditions.append("m.trade_date <= :end_date")
                    query_params["end_date"] = self._parse_date(val[4:])
            
            # 銘柄コードフィルタ
            if "bond_code" in params:
                val = params["bond_code"]
                if val.startswith("eq."):
                    conditions.append("m.bond_code = :bond_code")
                    query_params["bond_code"] = val[3:]

            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

            order_clause = "ORDER BY m.trade_date DESC, m.bond_code ASC"
            if "order" in params:
                order_param = params["order"]
                if "trade_date.desc" in order_param:
                    order_clause = "ORDER BY m.trade_date DESC"
                elif "trade_date.asc" in order_param:
                    order_clause = "ORDER BY m.trade_date ASC"

            sql = f"""
                SELECT 
                    m.trade_date, m.bond_code, m.market_amount,
                    b.bond_name, b.due_date
                FROM bond_market_amount m
                LEFT JOIN bond_data b ON m.trade_date = b.trade_date AND m.bond_code = b.bond_code
                {where_clause}
                {order_clause}
                LIMIT :limit
            """
            
            async with AsyncSessionLocal() as session:
                stmt = text(sql)
                result = await session.execute(stmt, query_params)
                data = [dict(row._mapping) for row in result]
                data = self._convert_types(data)
                return {"success": True, "data": data}

        except Exception as e:
            logger.error(f"get_market_amount_data error: {e}")
            return {"success": False, "error": str(e)}

    async def get_unique_bonds(self, limit: int = 100, query: Optional[str] = None) -> Dict[str, Any]:
        """
        全期間からユニークな銘柄一覧を取得（最新の属性を付随）
        """
        try:
            where_clause = ""
            query_params = {"limit": limit}
            if query:
                where_clause = "WHERE m.bond_code LIKE :query OR b.bond_name LIKE :query"
                query_params["query"] = f"%{query}%"

            # 各銘柄の最新取引日を取得し、その日のデータをJOINする
            sql = f"""
                WITH latest_dates AS (
                    SELECT bond_code, MAX(trade_date) as max_date
                    FROM bond_market_amount
                    GROUP BY bond_code
                )
                SELECT 
                    m.bond_code, m.trade_date as latest_trade_date, m.market_amount as latest_market_amount,
                    b.bond_name, b.due_date
                FROM bond_market_amount m
                JOIN latest_dates ld ON m.bond_code = ld.bond_code AND m.trade_date = ld.max_date
                LEFT JOIN bond_data b ON m.trade_date = b.trade_date AND m.bond_code = b.bond_code
                {where_clause}
                ORDER BY b.due_date DESC, m.bond_code ASC
                LIMIT :limit
            """
            
            async with AsyncSessionLocal() as session:
                stmt = text(sql)
                result = await session.execute(stmt, query_params)
                data = [dict(row._mapping) for row in result]
                data = self._convert_types(data)
                return {"success": True, "data": data}

        except Exception as e:
            logger.error(f"get_unique_bonds error: {e}")
            return {"success": False, "error": str(e)}

    async def get_bond_spreads(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        ASW (Asset Swap Spread) データを取得する専用メソッド
        ASW_data と bond_data を JOIN して詳細情報を取得
        """
        try:
            limit = params.get("limit", 1000)
            conditions = []
            query_params = {"limit": limit}
            
            if "trade_date" in params:
                val = params["trade_date"]
                if val.startswith("eq."):
                    conditions.append("a.trade_date = :trade_date")
                    query_params["trade_date"] = self._parse_date(val[3:])
                elif val.startswith("gte."):
                    conditions.append("a.trade_date >= :start_date")
                    query_params["start_date"] = self._parse_date(val[4:])
                elif val.startswith("lte."):
                    conditions.append("a.trade_date <= :end_date")
                    query_params["end_date"] = self._parse_date(val[4:])
                else:
                    conditions.append("a.trade_date = :trade_date")
                    query_params["trade_date"] = self._parse_date(val)

            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

            # JOIN with bond_data to get name, yield, maturity
            sql = f"""
                SELECT 
                    a.trade_date, a.bond_code, 
                    a.asw_act365_pa, a.asw_act365_sa,
                    b.bond_name, b.due_date, b.ave_compound_yield as yield_compound
                FROM "ASW_data" a
                JOIN bond_data b ON a.trade_date = b.trade_date AND a.bond_code = b.bond_code
                {where_clause}
                ORDER BY b.due_date ASC
                LIMIT :limit
            """
            
            async with AsyncSessionLocal() as session:
                stmt = text(sql)
                result = await session.execute(stmt, query_params)
                data = [dict(row._mapping) for row in result]
                
                # 型変換
                data = self._convert_types(data)
                
                return {"success": True, "data": data}

        except Exception as e:
            logger.error(f"get_bond_spreads error: {e}")
            return {"success": False, "error": str(e)}

    async def get_irs_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        IRSデータ（スワップ金利）を取得する専用メソッド
        """
        try:
            limit = params.get("limit", 1000)
            conditions = []
            query_params = {"limit": limit}
            
            if "trade_date" in params:
                val = params["trade_date"]
                if val.startswith("eq."):
                    conditions.append("trade_date = :trade_date")
                    query_params["trade_date"] = self._parse_date(val[3:])
                elif val.startswith("gte."):
                    conditions.append("trade_date >= :start_date")
                    query_params["start_date"] = self._parse_date(val[4:])
                elif val.startswith("lte."):
                    conditions.append("trade_date <= :end_date")
                    query_params["end_date"] = self._parse_date(val[4:])
            
            if "product_type" in params:
                val = params["product_type"]
                if val.startswith("eq."):
                    conditions.append("product_type = :product_type")
                    query_params["product_type"] = val[3:]

            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

            # Tenor sort might be tricky because it's string (1Y, 10Y). 
            # ideally we sort by a converted value, but for now simple fetch is enough.
            sql = f"SELECT * FROM irs_data {where_clause} ORDER BY trade_date DESC, tenor ASC LIMIT :limit"
            
            async with AsyncSessionLocal() as session:
                stmt = text(sql)
                result = await session.execute(stmt, query_params)
                data = [dict(row._mapping) for row in result]
                
                # 型変換
                data = self._convert_types(data)
                
                return {"success": True, "data": data}

        except Exception as e:
            logger.error(f"get_irs_data error: {e}")
            return {"success": False, "error": str(e)}

    async def get_ois_data_range(self, start_date: date, end_date: date, product_type: str = 'OIS') -> List[Dict[str, Any]]:
        """指定期間のOISデータを取得（テナーでソート）"""
        try:
            sql = """
                SELECT trade_date, tenor, rate 
                FROM irs_data 
                WHERE product_type = :product_type 
                AND trade_date >= :start_date 
                AND trade_date <= :end_date
                ORDER BY trade_date ASC, 
                         CASE 
                            WHEN tenor LIKE '%M' THEN CAST(REPLACE(tenor, 'M', '') AS INTEGER) * 30
                            WHEN tenor LIKE '%Y' THEN CAST(REPLACE(tenor, 'Y', '') AS INTEGER) * 365
                            ELSE 99999
                         END ASC
            """
            async with AsyncSessionLocal() as session:
                stmt = text(sql)
                result = await session.execute(stmt, {
                    "product_type": product_type,
                    "start_date": start_date,
                    "end_date": end_date
                })
                data = [dict(row._mapping) for row in result]
                return self._convert_types(data)
        except Exception as e:
            logger.error(f"get_ois_data_range error: {e}")
            return []

    async def health_check(self) -> Dict[str, Any]:
        """データベース接続のヘルスチェック"""
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text("SELECT 1"))
                return {
                    "database_status": "healthy",
                    "connection": "neon-direct"
                }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "database_status": "error",
                "error": str(e)
            }


# グローバルデータベースマネージャーインスタンス
db_manager = DatabaseManager()
db_manager_admin = db_manager