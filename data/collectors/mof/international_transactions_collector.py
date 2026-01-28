"""
対内対外証券投資（週次・財務省）データ収集クラス
"""
import requests
import io
import csv
import logging
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List
from data.utils.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

class InternationalTransactionsCollector:
    DATA_URL = "https://www.mof.go.jp/policy/international_policy/reference/itn_transactions_in_securities/week.csv"

    def __init__(self):
        self.db = DatabaseManager()

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
            processed_count = self._save_to_db(data_rows)
            
            return {
                "status": "success",
                "message": f"Processed {processed_count} rows",
                "details": {
                    "total_processed": processed_count
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
                        'updated_at': datetime.now() # UPSERT用
                    }
                    data_rows.append(record)
                except Exception as e:
                    logger.warning(f"Row parse error: {row[0]} -> {e}")
                    continue
                    
        return data_rows

    def _save_to_db(self, data_rows: List[Dict[str, Any]]) -> int:
        # 更新対象のカラムリストを作成 (start_date以外全て)
        if not data_rows:
            return 0
            
        update_columns = [k for k in data_rows[0].keys() if k != 'start_date']
        
        return self.db.batch_insert_data(
            data_rows,
            table_name='mof_international_transactions',
            conflict_target='start_date',
            update_columns=update_columns
        )