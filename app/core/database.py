"""
統一データベース接続管理システム（Web API用）
Supabaseとの接続を一元管理し、FastAPI APIで共通利用

注意: このモジュールはWeb API用（async対応）
スクリプト/バッチ処理では data/utils/database_manager.py を使用すること
"""
import httpx
from typing import Dict, Any, Optional
import logging
from .config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Web API用データベース管理クラス（非同期対応）

    FastAPIのasyncエンドポイントで使用するため、httpx.AsyncClientを使用。
    スクリプトやバッチ処理では data/utils/database_manager.py を使用すること。
    """

    def __init__(self):
        """
        Service Role Keyを使用して全操作を実行
        本番環境ではCloud Run内部からのアクセスのみ許可
        RLSポリシーで追加の保護層を提供
        """
        self.supabase_url = settings.supabase_url
        self.supabase_key = settings.supabase_key
        self.base_url = f"{self.supabase_url}/rest/v1"
        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json'
        }
        # タイムアウト設定（接続: 10秒, 読み取り: 30秒）
        self._timeout = httpx.Timeout(10.0, read=30.0)

    async def execute_query(self, table: str = "bond_data",
                          params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        データベースクエリの共通実行関数（非同期版）

        Args:
            table: テーブル名（デフォルト: bond_data）
            params: クエリパラメータ

        Returns:
            Dict containing success status and data/error
        """
        if params is None:
            params = {}

        try:
            url = f"{self.base_url}/{table}"

            logger.info(f"Database query to {table} with params: {params}")

            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url, params=params, headers=self.headers)

            if response.status_code == 200:
                data = response.json()
                logger.info(f"Query successful, returned {len(data)} records")
                return {"success": True, "data": data}
            else:
                error_msg = f"Database error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}

        except httpx.TimeoutException as e:
            error_msg = f"Database request timeout: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        except httpx.RequestError as e:
            error_msg = f"Database connection error: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

    async def get_bond_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """国債データを取得する専用メソッド"""
        return await self.execute_query("bond_data", params)

    async def health_check(self) -> Dict[str, Any]:
        """データベース接続のヘルスチェック"""
        try:
            result = await self.execute_query("bond_data", {"limit": 1})
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
        except httpx.RequestError as e:
            return {
                "database_status": "error",
                "error": str(e),
                "url_configured": bool(self.supabase_url),
                "key_configured": bool(self.supabase_key)
            }


# グローバルデータベースマネージャーインスタンス
# Service Role Key使用（読み取り・書き込み共通）
db_manager = DatabaseManager()

# 後方互換性のためのエイリアス
db_manager_admin = db_manager
