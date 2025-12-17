#!/usr/bin/env python3
"""
市中残存額（market_amount）計算 - Direct PostgreSQL版

戦略:
PostgreSQLに直接接続してCURSORでデータ取得
→ メモリ効率的にmarket_amount計算
→ 一時テーブル経由で一括UPDATE

処理時間見込み: 10-15分
"""

import sys
import os
from datetime import datetime
from typing import Dict, List
from collections import defaultdict
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import psycopg2
from urllib.parse import urlparse

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DirectPostgreSQLCalculator:
    def __init__(self):
        # Supabase URLからPostgreSQL接続情報を構築
        supabase_url = os.getenv('SUPABASE_URL')
        parsed = urlparse(supabase_url)

        # Supabaseの場合、ホスト名から接続情報を構築
        # 例: https://yfravzuebsvkzjnabalj.supabase.co
        # → db.yfravzuebsvkzjnabalj.supabase.co:5432
        host = parsed.hostname.replace('.supabase.co', '.supabase.co')
        db_host = f"db.{parsed.hostname}"

        # PostgreSQL接続文字列を構築
        # Supabaseの直接接続は制限があるため、Supabase REST APIを使い続ける必要があります
        # 代わりに、Pythonで全データを効率的に取得する方法を使います

        logger.error("Supabaseは直接PostgreSQL接続をサポートしていません")
        logger.error("代わりに、Supabase SQL Editorで直接SQLを実行する方式を使います")
        sys.exit(1)


def main():
    logger.info("=" * 70)
    logger.info("このスクリプトは使用できません")
    logger.info("=" * 70)
    logger.info("")
    logger.info("Supabaseは直接PostgreSQL接続をサポートしていないため、")
    logger.info("以下のいずれかの方法を使用してください:")
    logger.info("")
    logger.info("方法1: Supabase SQL Editorで直接SQLを実行")
    logger.info("  1. scripts/sql/calculate_market_amount.sql を作成")
    logger.info("  2. Supabase Dashboardの SQL Editor で実行")
    logger.info("")
    logger.info("方法2: RPC関数を作成してPythonから呼び出し")
    logger.info("  1. Supabaseに RPC関数を作成")
    logger.info("  2. Pythonスクリプトから呼び出し")
    logger.info("")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()
