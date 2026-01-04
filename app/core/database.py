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
        
        allowed_tables = ["bond_data", "bond_auction", "economic_indicators", "boj_holdings", "irs_settlement_rates"]
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

    async def get_bond_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        国債データを取得する専用メソッド
        APIエンドポイントからのリクエストパラメータを処理
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
                else:
                    conditions.append("trade_date = :trade_date")
                    query_params["trade_date"] = self._parse_date(val)
                
            if "start_date" in params:
                conditions.append("trade_date >= :start_date")
                query_params["start_date"] = self._parse_date(params["start_date"])
                
            if "end_date" in params:
                conditions.append("trade_date <= :end_date")
                query_params["end_date"] = self._parse_date(params["end_date"])

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
                SELECT * FROM bond_data 
                {where_clause}
                {order_clause}
                LIMIT :limit
            """
            
            async with AsyncSessionLocal() as session:
                stmt = text(sql)
                result = await session.execute(stmt, query_params)
                data = [dict(row._mapping) for row in result]
                
                if data:
                    logger.info(f"Sample data keys: {list(data[0].keys())}")
                    # 型変換
                    data = self._convert_types(data)
                else:
                    logger.warning("No data found matching criteria")
                
                return {"success": True, "data": data}

        except Exception as e:
            logger.error(f"get_bond_data error: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
            sql = f"""
                SELECT * FROM irs_settlement_rates 
                {where_clause}
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
            logger.error(f"get_irs_data error: {e}")
            return {"success": False, "error": str(e)}

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