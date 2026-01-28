"""
対内対外証券投資（週次・財務省）データ収集クラス
"""
import requests
import io
import csv
import logging
import psycopg2
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List
from app.core.config import settings

logger = logging.getLogger(__name__)

class InternationalTransactionsCollector:
    DATA_URL = "https://www.mof.go.jp/policy/international_policy/reference/itn_transactions_in_securities/week.csv"

    def __init__(self):
        # データベース接続設定は環境変数から取得（設定ファイル経由）
        self.db_url = settings.DATABASE_URL
        # あるいは直接環境変数を参照することも可能だが、
        # ここではスクリプトと同じロジックを使うため、実行時に環境変数を参照する形にする
        # または、DatabaseManager を使うのがベストプラクティス
        pass

    def collect(self) -> Dict[str, Any]:
        """
        データを収集し、データベースに保存する
        """
        logger.info("Starting International Transactions data collection")
        
        try:
            # 1. ダウンロード
            content = self._download_csv()
            if not content:
                return {"status": "error", "message": "Failed to download CSV"}

            # 2. パース
            data_rows = self._parse_csv(content)
            if not data_rows:
                return {"status": "warning", "message": "No data found in CSV"}
            
            # 3. DB保存
            inserted, updated = self._save_to_db(data_rows)
            
            return {
                "status": "success",
                "message": f"Processed {len(data_rows)} rows",
                "details": {
                    "total": len(data_rows),
                    "inserted": inserted, # Note: Current logic doesn't distinguish insert/update count strictly
                    "updated": updated
                }
            }

        except Exception as e:
            logger.error(f"Collection failed: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    def _download_csv(self) -> Optional[str]:
        logger.info(f"Downloading from {self.DATA_URL}")
        try:
            response = requests.get(self.DATA_URL, timeout=30)
            response.raise_for_status()
            response.encoding = 'cp932'
            return response.text
        except Exception as e:
            logger.error(f"Download error: {e}")
            raise

    def _parse_date(self, date_str: str) -> Optional[datetime.date]:
        try:
            clean_str = date_str.strip().replace("．", ".").replace(" ", "")
            parts = clean_str.split('.')
            if len(parts) >= 3:
                return datetime(int(parts[0]), int(parts[1]), int(parts[2])).date()
            return None
        except:
            return None

    def _parse_period(self, period_str: str) -> Tuple[Optional[datetime.date], Optional[datetime.date]]:
        try:
            sep = "～" if "～" in period_str else "~"
            parts = period_str.split(sep)
            if len(parts) != 2:
                return None, None
            
            start_str = parts[0].strip()
            end_str = parts[1].strip()
            
            start_date = self._parse_date(start_str)
            
            if start_date:
                clean_end = end_str.strip().replace("．", ".").replace(" ", "")
                end_parts = clean_end.split('.')
                
                year = start_date.year
                if len(end_parts) == 2:
                    month = int(end_parts[0])
                    day = int(end_parts[1])
                    if start_date.month == 12 and month == 1:
                        year += 1
                    end_date = datetime(year, month, day).date()
                elif len(end_parts) >= 3:
                    end_date = self._parse_date(end_str)
                else:
                    end_date = None
            else:
                end_date = None
                
            return start_date, end_date
        except:
            return None, None

    def _parse_value(self, value_str: str) -> Optional[int]:
        if not value_str:
            return None
        clean_str = value_str.strip().replace(",", "")
        if clean_str == "" or clean_str == "-":
            return None
        try:
            return int(clean_str)
        except ValueError:
            return None

    def _parse_csv(self, content: str) -> List[Dict[str, Any]]:
        data_rows = []
        f = io.StringIO(content)
        reader = csv.reader(f)
        
        for row in reader:
            if not row:
                continue
            
            first_col = row[0].strip()
            if "．" in first_col and "～" in first_col:
                try:
                    start_date, end_date = self._parse_period(first_col)
                    if not start_date or not end_date:
                        continue
                    
                    record = {
                        'start_date': start_date,
                        'end_date': end_date,
                        'outward_equity_acquisition': self._parse_value(row[1]),
                        'outward_equity_disposition': self._parse_value(row[2]),
                        'outward_equity_net': self._parse_value(row[3]),
                        'outward_long_term_acquisition': self._parse_value(row[4]),
                        'outward_long_term_disposition': self._parse_value(row[5]),
                        'outward_long_term_net': self._parse_value(row[6]),
                        'outward_subtotal_net': self._parse_value(row[7]),
                        'outward_short_term_acquisition': self._parse_value(row[8]),
                        'outward_short_term_disposition': self._parse_value(row[9]),
                        'outward_short_term_net': self._parse_value(row[10]),
                        'outward_total_net': self._parse_value(row[11]),
                        'inward_equity_acquisition': self._parse_value(row[12]),
                        'inward_equity_disposition': self._parse_value(row[13]),
                        'inward_equity_net': self._parse_value(row[14]),
                        'inward_long_term_acquisition': self._parse_value(row[15]),
                        'inward_long_term_disposition': self._parse_value(row[16]),
                        'inward_long_term_net': self._parse_value(row[17]),
                        'inward_subtotal_net': self._parse_value(row[18]),
                        'inward_short_term_acquisition': self._parse_value(row[19]),
                        'inward_short_term_disposition': self._parse_value(row[20]),
                        'inward_short_term_net': self._parse_value(row[21]),
                        'inward_total_net': self._parse_value(row[22]),
                    }
                    data_rows.append(record)
                except Exception as e:
                    logger.warning(f"Row parse error: {row[0]} -> {e}")
                    continue
                    
        return data_rows

    def _save_to_db(self, data_rows: List[Dict[str, Any]]) -> Tuple[int, int]:
        # ここでは環境変数ではなく、アプリのDatabaseManagerを使うべきだが、
        # 取り急ぎpsycopg2で直接接続するロジックを維持（依存関係を減らすため）
        # ただし、appコンテキスト内なので settings.DATABASE_URL をパースして使う
        
        # settings.DATABASE_URL が SQLAlchemy用のURL (postgres://...) の場合があるため
        # 簡易的に環境変数から再構築するか、既存のロジックを流用する
        import os
        
        # Cloud Run環境などでは環境変数でDB接続情報が渡されることを想定
        db_host = os.getenv('DB_HOST') or os.getenv('CLOUD_SQL_HOST')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME') or os.getenv('CLOUD_SQL_DATABASE')
        db_user = os.getenv('DB_USER') or os.getenv('CLOUD_SQL_USER')
        db_password = os.getenv('DB_PASSWORD') or os.getenv('CLOUD_SQL_PASSWORD')

        if not db_host:
             # Fallback: try to parse settings.DATABASE_URL if available
             # ここでは簡易的にエラーとする
             raise ValueError("DB connection info not found in env vars")

        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        
        try:
            with conn.cursor() as cursor:
                insert_sql = """
                    INSERT INTO mof_international_transactions (
                        start_date, end_date,
                        outward_equity_acquisition, outward_equity_disposition, outward_equity_net,
                        outward_long_term_acquisition, outward_long_term_disposition, outward_long_term_net,
                        outward_subtotal_net,
                        outward_short_term_acquisition, outward_short_term_disposition, outward_short_term_net,
                        outward_total_net,
                        inward_equity_acquisition, inward_equity_disposition, inward_equity_net,
                        inward_long_term_acquisition, inward_long_term_disposition, inward_long_term_net,
                        inward_subtotal_net,
                        inward_short_term_acquisition, inward_short_term_disposition, inward_short_term_net,
                        inward_total_net,
                        updated_at
                    ) VALUES (
                        %(start_date)s, %(end_date)s,
                        %(outward_equity_acquisition)s, %(outward_equity_disposition)s, %(outward_equity_net)s,
                        %(outward_long_term_acquisition)s, %(outward_long_term_disposition)s, %(outward_long_term_net)s,
                        %(outward_subtotal_net)s,
                        %(outward_short_term_acquisition)s, %(outward_short_term_disposition)s, %(outward_short_term_net)s,
                        %(outward_total_net)s,
                        %(inward_equity_acquisition)s, %(inward_equity_disposition)s, %(inward_equity_net)s,
                        %(inward_long_term_acquisition)s, %(inward_long_term_disposition)s, %(inward_long_term_net)s,
                        %(inward_subtotal_net)s,
                        %(inward_short_term_acquisition)s, %(inward_short_term_disposition)s, %(inward_short_term_net)s,
                        %(inward_total_net)s,
                        NOW()
                    )
                    ON CONFLICT (start_date) DO UPDATE SET
                        end_date = EXCLUDED.end_date,
                        outward_equity_acquisition = EXCLUDED.outward_equity_acquisition,
                        outward_equity_disposition = EXCLUDED.outward_equity_disposition,
                        outward_equity_net = EXCLUDED.outward_equity_net,
                        outward_long_term_acquisition = EXCLUDED.outward_long_term_acquisition,
                        outward_long_term_disposition = EXCLUDED.outward_long_term_disposition,
                        outward_long_term_net = EXCLUDED.outward_long_term_net,
                        outward_subtotal_net = EXCLUDED.outward_subtotal_net,
                        outward_short_term_acquisition = EXCLUDED.outward_short_term_acquisition,
                        outward_short_term_disposition = EXCLUDED.outward_short_term_disposition,
                        outward_short_term_net = EXCLUDED.outward_short_term_net,
                        outward_total_net = EXCLUDED.outward_total_net,
                        inward_equity_acquisition = EXCLUDED.inward_equity_acquisition,
                        inward_equity_disposition = EXCLUDED.inward_equity_disposition,
                        inward_equity_net = EXCLUDED.inward_equity_net,
                        inward_long_term_acquisition = EXCLUDED.inward_long_term_acquisition,
                        inward_long_term_disposition = EXCLUDED.inward_long_term_disposition,
                        inward_long_term_net = EXCLUDED.inward_long_term_net,
                        inward_subtotal_net = EXCLUDED.inward_subtotal_net,
                        inward_short_term_acquisition = EXCLUDED.inward_short_term_acquisition,
                        inward_short_term_disposition = EXCLUDED.inward_short_term_disposition,
                        inward_short_term_net = EXCLUDED.inward_short_term_net,
                        inward_total_net = EXCLUDED.inward_total_net,
                        updated_at = NOW();
                """
                cursor.executemany(insert_sql, data_rows)
                
            return len(data_rows), 0 # 簡易的に全てinserted扱い
        finally:
            conn.close()
