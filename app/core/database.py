"""
統一データベース接続管理システム
Supabaseとの接続を一元管理し、全てのAPIで共通利用
"""
import requests
from typing import Dict, Any, Optional
import logging
from .config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """統一データベース管理クラス"""

    def __init__(self):
        self.supabase_url = settings.supabase_url
        self.supabase_key = settings.supabase_key
        self.base_url = f"{self.supabase_url}/rest/v1"
        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json'
        }

    async def execute_query(self, table: str = "clean_bond_data",
                          params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        データベースクエリの共通実行関数

        Args:
            table: テーブル名（デフォルト: clean_bond_data）
            params: クエリパラメータ

        Returns:
            Dict containing success status and data/error
        """
        if params is None:
            params = {}

        try:
            url = f"{self.base_url}/{table}"

            logger.info(f"Database query to {table} with params: {params}")

            response = requests.get(url, params=params, headers=self.headers)

            if response.status_code == 200:
                data = response.json()
                logger.info(f"Query successful, returned {len(data)} records")
                return {"success": True, "data": data}
            else:
                error_msg = f"Database error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}

        except Exception as e:
            error_msg = f"Database connection error: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

    async def get_bond_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """国債データを取得する専用メソッド"""
        return await self.execute_query("clean_bond_data", params)

    async def health_check(self) -> Dict[str, Any]:
        """データベース接続のヘルスチェック"""
        try:
            result = await self.execute_query("clean_bond_data", {"limit": 1})
            if result["success"]:
                return {
                    "database_status": "healthy",
                    "url_configured": bool(self.supabase_url),
                    "key_configured": bool(self.supabase_key)
                }
            else:
                return {
                    "database_status": "error",
                    "error": result["error"],
                    "url_configured": bool(self.supabase_url),
                    "key_configured": bool(self.supabase_key)
                }
        except Exception as e:
            return {
                "database_status": "error",
                "error": str(e),
                "url_configured": bool(self.supabase_url),
                "key_configured": bool(self.supabase_key)
            }


# グローバルデータベースマネージャーインスタンス
db_manager = DatabaseManager()