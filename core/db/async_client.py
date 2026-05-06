"""
非同期データベースクライアント（Web API 用）
SQLAlchemy + asyncpg で Neon (PostgreSQL) に接続。

スクリプト/バッチ処理では core/db/sync_client.py を使用すること。
"""
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime, date
from decimal import Decimal
from sqlalchemy import text

from core.db.engine import AsyncSessionLocal
from core.db import ALLOWED_TABLES

logger = logging.getLogger(__name__)


class AsyncDatabaseClient:
    """Web API 用データベースクライアント（SQLAlchemy 非同期版）"""

    def _parse_date(self, date_str: str) -> date:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return date_str

    def _convert_types(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """datetime.date → str、Decimal → float に変換（JSON シリアライズ対応）"""
        for row in data:
            for key, value in row.items():
                if isinstance(value, date):
                    row[key] = str(value)
                elif isinstance(value, Decimal):
                    row[key] = float(value)
        return data

    async def get_bond_data(
        self,
        trade_date: str = None,
        start_date: str = None,
        end_date: str = None,
        bond_code: str = None,
        order: str = 'desc',
        limit: int = 1000,
        table_name: str = 'bond_data',
    ) -> Dict[str, Any]:
        """国債データ取得"""
        if table_name not in ALLOWED_TABLES:
            table_name = 'bond_data'
        try:
            conditions = []
            query_params: Dict[str, Any] = {'limit': limit}

            if trade_date:
                conditions.append('trade_date = :trade_date')
                query_params['trade_date'] = self._parse_date(trade_date)
            if start_date:
                conditions.append('trade_date >= :start_date')
                query_params['start_date'] = self._parse_date(start_date)
            if end_date:
                conditions.append('trade_date <= :end_date')
                query_params['end_date'] = self._parse_date(end_date)
            if bond_code:
                conditions.append('bond_code = :bond_code')
                query_params['bond_code'] = bond_code

            where = 'WHERE ' + ' AND '.join(conditions) if conditions else ''
            direction = 'DESC' if order == 'desc' else 'ASC'
            order_clause = f'ORDER BY trade_date {direction}, bond_code ASC'

            sql = f'SELECT * FROM {table_name} {where} {order_clause} LIMIT :limit'
            async with AsyncSessionLocal() as session:
                result = await session.execute(text(sql), query_params)
                data = self._convert_types([dict(row._mapping) for row in result])
                return {'success': True, 'data': data}
        except Exception as e:
            logger.error(f'get_bond_data error: {e}')
            return {'success': False, 'error': str(e)}

    async def get_market_amount_data(
        self,
        trade_date: str = None,
        start_date: str = None,
        end_date: str = None,
        bond_code: str = None,
        order: str = 'desc',
        limit: int = 1000,
    ) -> Dict[str, Any]:
        """市中残存額データ取得（bond_market_amount + bond_data JOIN）"""
        try:
            conditions = []
            query_params: Dict[str, Any] = {'limit': limit}

            if trade_date:
                conditions.append('m.trade_date = :trade_date')
                query_params['trade_date'] = self._parse_date(trade_date)
            if start_date:
                conditions.append('m.trade_date >= :start_date')
                query_params['start_date'] = self._parse_date(start_date)
            if end_date:
                conditions.append('m.trade_date <= :end_date')
                query_params['end_date'] = self._parse_date(end_date)
            if bond_code:
                conditions.append('m.bond_code = :bond_code')
                query_params['bond_code'] = bond_code

            where = 'WHERE ' + ' AND '.join(conditions) if conditions else ''
            direction = 'DESC' if order == 'desc' else 'ASC'
            order_clause = f'ORDER BY m.trade_date {direction}, m.bond_code ASC'

            sql = f"""
                SELECT m.trade_date, m.bond_code, m.market_amount, b.bond_name, b.due_date
                FROM bond_market_amount m
                LEFT JOIN bond_data b ON m.trade_date = b.trade_date AND m.bond_code = b.bond_code
                {where} {order_clause} LIMIT :limit
            """
            async with AsyncSessionLocal() as session:
                result = await session.execute(text(sql), query_params)
                data = self._convert_types([dict(row._mapping) for row in result])
                return {'success': True, 'data': data}
        except Exception as e:
            logger.error(f'get_market_amount_data error: {e}')
            return {'success': False, 'error': str(e)}

    async def get_unique_bonds(self, limit: int = 100, query: Optional[str] = None) -> Dict[str, Any]:
        """ユニーク銘柄一覧を取得（最新属性付き）"""
        try:
            where_clause = ''
            query_params: Dict[str, Any] = {'limit': limit}
            if query:
                where_clause = 'WHERE m.bond_code LIKE :query OR b.bond_name LIKE :query'
                query_params['query'] = f'%{query}%'

            sql = f"""
                WITH latest_dates AS (
                    SELECT bond_code, MAX(trade_date) as max_date FROM bond_market_amount GROUP BY bond_code
                )
                SELECT m.bond_code, m.trade_date as latest_trade_date, m.market_amount as latest_market_amount,
                       b.bond_name, b.due_date
                FROM bond_market_amount m
                JOIN latest_dates ld ON m.bond_code = ld.bond_code AND m.trade_date = ld.max_date
                LEFT JOIN bond_data b ON m.trade_date = b.trade_date AND m.bond_code = b.bond_code
                {where_clause}
                ORDER BY b.due_date DESC, m.bond_code ASC LIMIT :limit
            """
            async with AsyncSessionLocal() as session:
                result = await session.execute(text(sql), query_params)
                data = self._convert_types([dict(row._mapping) for row in result])
                return {'success': True, 'data': data}
        except Exception as e:
            logger.error(f'get_unique_bonds error: {e}')
            return {'success': False, 'error': str(e)}

    async def get_bond_spreads(
        self,
        trade_date: str = None,
        start_date: str = None,
        end_date: str = None,
        limit: int = 1000,
    ) -> Dict[str, Any]:
        """ASW データ取得（ASW_data + bond_data JOIN）"""
        try:
            conditions = []
            query_params: Dict[str, Any] = {'limit': limit}

            if trade_date:
                conditions.append('a.trade_date = :trade_date')
                query_params['trade_date'] = self._parse_date(trade_date)
            if start_date:
                conditions.append('a.trade_date >= :start_date')
                query_params['start_date'] = self._parse_date(start_date)
            if end_date:
                conditions.append('a.trade_date <= :end_date')
                query_params['end_date'] = self._parse_date(end_date)

            where = 'WHERE ' + ' AND '.join(conditions) if conditions else ''
            sql = f"""
                SELECT a.trade_date, a.bond_code, a.asw_act365_pa, a.asw_act365_sa,
                       b.bond_name, b.due_date, b.ave_compound_yield as yield_compound
                FROM "ASW_data" a
                JOIN bond_data b ON a.trade_date = b.trade_date AND a.bond_code = b.bond_code
                {where} ORDER BY b.due_date ASC LIMIT :limit
            """
            async with AsyncSessionLocal() as session:
                result = await session.execute(text(sql), query_params)
                data = self._convert_types([dict(row._mapping) for row in result])
                return {'success': True, 'data': data}
        except Exception as e:
            logger.error(f'get_bond_spreads error: {e}')
            return {'success': False, 'error': str(e)}

    async def get_irs_data(
        self,
        trade_date: str = None,
        product_type: str = None,
        start_date: str = None,
        end_date: str = None,
        limit: int = 1000,
    ) -> Dict[str, Any]:
        """IRS データ（スワップ金利）取得"""
        try:
            conditions = []
            query_params: Dict[str, Any] = {'limit': limit}

            if trade_date:
                conditions.append('trade_date = :trade_date')
                query_params['trade_date'] = self._parse_date(trade_date)
            if start_date:
                conditions.append('trade_date >= :start_date')
                query_params['start_date'] = self._parse_date(start_date)
            if end_date:
                conditions.append('trade_date <= :end_date')
                query_params['end_date'] = self._parse_date(end_date)
            if product_type:
                conditions.append('product_type = :product_type')
                query_params['product_type'] = product_type

            where = 'WHERE ' + ' AND '.join(conditions) if conditions else ''
            sql = f"""
                SELECT * FROM irs_data {where}
                ORDER BY trade_date DESC, tenor ASC LIMIT :limit
            """
            async with AsyncSessionLocal() as session:
                result = await session.execute(text(sql), query_params)
                data = self._convert_types([dict(row._mapping) for row in result])
                return {'success': True, 'data': data}
        except Exception as e:
            logger.error(f'get_irs_data error: {e}')
            return {'success': False, 'error': str(e)}

    async def get_ois_data_range(self, start_date: date, end_date: date, product_type: str = 'OIS') -> List[Dict[str, Any]]:
        """指定期間の OIS データを取得"""
        try:
            sql = """
                SELECT trade_date, tenor, rate FROM irs_data
                WHERE product_type = :product_type
                AND trade_date >= :start_date AND trade_date <= :end_date
                ORDER BY trade_date ASC,
                         CASE
                            WHEN tenor LIKE '%M' THEN CAST(REPLACE(tenor, 'M', '') AS INTEGER) * 30
                            WHEN tenor LIKE '%Y' THEN CAST(REPLACE(tenor, 'Y', '') AS INTEGER) * 365
                            ELSE 99999
                         END ASC
            """
            async with AsyncSessionLocal() as session:
                result = await session.execute(text(sql), {
                    'product_type': product_type,
                    'start_date': start_date,
                    'end_date': end_date,
                })
                return self._convert_types([dict(row._mapping) for row in result])
        except Exception as e:
            logger.error(f'get_ois_data_range error: {e}')
            return []

    async def health_check(self) -> Dict[str, Any]:
        """データベース接続のヘルスチェック"""
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text('SELECT 1'))
                return {'database_status': 'healthy', 'connection': 'neon-direct'}
        except Exception as e:
            logger.error(f'Health check failed: {e}')
            return {'database_status': 'error', 'error': str(e)}


# グローバルインスタンス
db_manager = AsyncDatabaseClient()
