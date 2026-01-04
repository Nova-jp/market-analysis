#!/usr/bin/env python3
"""
Database Manager - スクリプト/バッチ処理用 PostgreSQL データベース操作クラス

このモジュールは同期処理用です。
- データ収集スクリプト (scripts/)
- バッチ処理
- 分析スクリプト (analysis/)
での使用を想定しています。

Web API用（FastAPI async対応）は app/core/database.py を使用すること。
"""

import os
import logging
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from typing import List, Set, Dict, Any, Optional
from app.core.config import settings

class DatabaseManager:
    """
    スクリプト/バッチ処理用 PostgreSQL データベース操作クラス（同期版）
    Neon (PostgreSQL) への直接接続をサポート。
    """

    def __init__(self):
        """設定を読み込み、ロガーを初期化"""
        self.db_url = settings.database_url
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"DatabaseManager initialized for {settings.db_host}")

    def _get_connection(self):
        """新しいデータベース接続を作成"""
        return psycopg2.connect(self.db_url)

    def get_total_record_count(self, table_name: str = 'bond_data') -> int:
        """テーブルの総レコード数を取得"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # テーブル名の動的埋め込みはSQLインジェクションに注意が必要だが、
                    # 呼び出し元が固定値であることを前提とする
                    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                    return cur.fetchone()[0]
        except Exception as e:
            self.logger.error(f"総レコード数取得エラー ({table_name}): {e}")
            return 0
    
    def get_all_existing_dates(self, table_name: str = 'bond_data') -> Set[str]:
        """すべての既存取引日を取得"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT DISTINCT trade_date FROM {table_name}")
                    rows = cur.fetchall()
                    return {str(row[0]) for row in rows}
        except Exception as e:
            self.logger.error(f"既存日付取得エラー ({table_name}): {e}")
            return set()
    
    def batch_insert_data(self, data_list: List[Dict[Any, Any]], 
                         table_name: str = 'bond_data', 
                         batch_size: int = 100) -> int:
        """バッチでデータを挿入"""
        if not data_list:
            return 0
        
        # 最初のレコードからカラム名を取得
        columns = data_list[0].keys()
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) " \
                f"ON CONFLICT DO NOTHING"  # 重複は無視
        
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # データの値をリストのリストに変換
                    values = [tuple(item[col] for col in columns) for item in data_list]
                    execute_batch(cur, query, values, page_size=batch_size)
                    conn.commit()
                    return len(data_list)
        except Exception as e:
            self.logger.error(f"バッチ挿入エラー ({table_name}): {e}")
            return 0
    
    def get_date_range_info(self, table_name: str = 'bond_data') -> Dict[str, Any]:
        """データベースの日付範囲情報を取得"""
        try:
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
        """SQLクエリを直接実行（互換性維持のため）"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql_query, params)
                    return cur.fetchall()
        except Exception as e:
            self.logger.error(f"クエリ実行エラー: {e}")
            return []

    def get_yield_curve_data(self, trade_date: str = None) -> List[Dict[str, Any]]:
        """イールドカーブ作成用のデータを取得"""
        try:
            if trade_date is None:
                info = self.get_date_range_info()
                trade_date = info.get('latest_date')
            
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
                    data = cur.fetchall()
                    return [dict(row) for row in data]
        except Exception as e:
            self.logger.error(f"イールドカーブデータ取得エラー: {e}")
            return []

    def get_available_dates(self, limit: int = 10) -> List[str]:
        """利用可能な日付一覧を取得 (降順)"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT trade_date FROM bond_data ORDER BY trade_date DESC LIMIT %s", (limit,))
                    rows = cur.fetchall()
                    return [str(row[0]) for row in rows]
        except Exception as e:
            self.logger.error(f"利用可能日付取得エラー: {e}")
            return []

    def fetch_data(self, query: str, params: tuple = None) -> List[tuple]:
        """PCA分析用データ取得"""
        # PCA分析スクリプトが独自のクエリ文字列を投げている場合があるが、
        # ここでは提供されたクエリをそのまま実行する
        return self.execute_query(query, params)
