#!/usr/bin/env python3
"""
同期データベースクライアント（スクリプト / バッチ処理用）
psycopg2 で Neon (PostgreSQL) に直接接続。

Web API 用（FastAPI async 対応）は core/db/async_client.py を使用すること。
"""
import logging
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from typing import List, Set, Dict, Any, Optional

from core.config import settings
from core.db import ALLOWED_TABLES


class DatabaseManager:
    """同期版データベースクライアント（スクリプト / バッチ処理用）"""

    ALLOWED_TABLES = ALLOWED_TABLES

    def __init__(self):
        self.db_url = settings.database_url
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"DatabaseManager initialized for {settings.db_host}")

    def _get_connection(self):
        return psycopg2.connect(self.db_url)

    def _validate_table_name(self, table_name: str) -> str:
        if table_name not in self.ALLOWED_TABLES:
            raise ValueError(f"Invalid table name: {table_name!r}")
        return table_name

    def get_total_record_count(self, table_name: str = 'bond_data') -> int:
        try:
            table_name = self._validate_table_name(table_name)
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                    return cur.fetchone()[0]
        except Exception as e:
            self.logger.error(f"総レコード数取得エラー ({table_name}): {e}")
            return 0

    def get_all_existing_dates(self, table_name: str = 'bond_data') -> Set[str]:
        try:
            table_name = self._validate_table_name(table_name)
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT DISTINCT trade_date FROM {table_name}")
                    return {str(row[0]) for row in cur.fetchall()}
        except Exception as e:
            self.logger.error(f"既存日付取得エラー ({table_name}): {e}")
            return set()

    def batch_insert_data(self, data_list: List[Dict[Any, Any]],
                          table_name: str = 'bond_data',
                          batch_size: int = 100,
                          conflict_target: str = None,
                          update_columns: List[str] = None) -> int:
        """バッチ挿入 (UPSERT 対応)"""
        if not data_list:
            return 0

        columns = list(data_list[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        col_names = ', '.join(columns)
        base_query = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

        if conflict_target:
            target = conflict_target if conflict_target.startswith('(') else f"({conflict_target})"
            if update_columns:
                updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                query = f"{base_query} ON CONFLICT {target} DO UPDATE SET {updates}"
            else:
                query = f"{base_query} ON CONFLICT {target} DO NOTHING"
        else:
            query = f"{base_query} ON CONFLICT DO NOTHING"

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    values = [tuple(item[col] for col in columns) for item in data_list]
                    execute_batch(cur, query, values, page_size=batch_size)
                    conn.commit()
                    return len(data_list)
        except Exception as e:
            self.logger.error(f"バッチ挿入エラー ({table_name}): {e}")
            return 0

    def get_date_range_info(self, table_name: str = 'bond_data') -> Dict[str, Any]:
        try:
            table_name = self._validate_table_name(table_name)
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM {table_name}")
                    min_date, max_date, count = cur.fetchone()
                    return {
                        'total_records': count,
                        'latest_date': str(max_date) if max_date else None,
                        'earliest_date': str(min_date) if min_date else None
                    }
        except Exception as e:
            self.logger.error(f"日付範囲情報取得エラー: {e}")
            return {'total_records': 0, 'latest_date': None, 'earliest_date': None}

    def execute_query(self, sql_query: str, params: tuple = None) -> List[tuple]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql_query, params)
                    return cur.fetchall()
        except Exception as e:
            self.logger.error(f"クエリ実行エラー: {e}")
            return []

    def select_as_dict(self, sql_query: str, params: tuple = None) -> List[Dict[str, Any]]:
        try:
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql_query, params)
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            self.logger.error(f"クエリ実行エラー (dict): {e}")
            return []

    def get_yield_curve_data(self, trade_date: str = None) -> List[Dict[str, Any]]:
        try:
            if trade_date is None:
                trade_date = self.get_date_range_info().get('latest_date')
            if not trade_date:
                return []
            sql = """
                SELECT trade_date, due_date, ave_compound_yield, bond_name, interest_payment_date
                FROM bond_data
                WHERE trade_date = %s
                  AND ave_compound_yield IS NOT NULL
                  AND ave_compound_yield BETWEEN 0 AND 10
                ORDER BY due_date ASC
            """
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql, (trade_date,))
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            self.logger.error(f"イールドカーブデータ取得エラー: {e}")
            return []

    def get_available_dates(self, limit: int = 10) -> List[str]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT DISTINCT trade_date FROM bond_data ORDER BY trade_date DESC LIMIT %s",
                        (limit,)
                    )
                    return [str(row[0]) for row in cur.fetchall()]
        except Exception as e:
            self.logger.error(f"利用可能日付取得エラー: {e}")
            return []

    def fetch_data(self, query: str, params: tuple = None) -> List[tuple]:
        return self.execute_query(query, params)

    def get_ois_data(self, start_date: str = None, end_date: str = None, product_type: str = 'OIS') -> List[Dict[str, Any]]:
        try:
            conditions = ["product_type = %s"]
            params = [product_type]
            if start_date:
                conditions.append("trade_date >= %s")
                params.append(start_date)
            if end_date:
                conditions.append("trade_date <= %s")
                params.append(end_date)

            where_clause = "WHERE " + " AND ".join(conditions)
            sql = f"""
                SELECT trade_date, tenor, rate FROM irs_data {where_clause}
                ORDER BY trade_date ASC,
                         CASE
                            WHEN tenor LIKE '%%M' THEN CAST(REPLACE(tenor, 'M', '') AS INTEGER) * 30
                            WHEN tenor LIKE '%%Y' THEN CAST(REPLACE(tenor, 'Y', '') AS INTEGER) * 365
                            ELSE 99999
                         END ASC
            """
            return self.select_as_dict(sql, tuple(params))
        except Exception as e:
            self.logger.error(f"OISデータ取得エラー: {e}")
            return []
