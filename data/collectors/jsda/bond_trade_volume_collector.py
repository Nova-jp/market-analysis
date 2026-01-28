import os
import re
import pandas as pd
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
from io import BytesIO
from app.core.config import settings
from data.utils.database_manager import DatabaseManager

class BondTradeVolumeCollector:
    """
    JSDA 公社債店頭売買高データの収集・解析クラス
    """
    
    # シート名から取引タイプとサイドを判定するためのマッピング
    # (A)~(L)の大文字シート（全協会員）を対象とする
    SHEET_MAPPING = {
        '(Ａ)合計売買高': {'side': 'volume', 'transaction_type': 'total'},
        '(Ｂ)一般売買高': {'side': 'volume', 'transaction_type': 'outright'},
        '(Ｃ)現先売買高': {'side': 'volume', 'transaction_type': 'repo'},
        
        '(Ｄ)合計売付額': {'side': 'sell', 'transaction_type': 'total'},
        '(Ｅ)一般売付額': {'side': 'sell', 'transaction_type': 'outright'},
        '(Ｆ)現先売付額': {'side': 'sell', 'transaction_type': 'repo'},
        
        '(Ｇ)合計買付額': {'side': 'buy', 'transaction_type': 'total'},
        '(Ｈ)一般買付額': {'side': 'buy', 'transaction_type': 'outright'},
        '(Ｉ)現先買付額': {'side': 'buy', 'transaction_type': 'repo'},
        
        '(Ｊ)合計差引': {'side': 'net', 'transaction_type': 'total'},
        '(Ｋ)一般差引': {'side': 'net', 'transaction_type': 'outright'},
        '(Ｌ)現先差引': {'side': 'net', 'transaction_type': 'repo'},
    }

    # Excelの列インデックスとDBカラムのマッピング
    # NOTE: Excelの構造が変わった場合はここを修正する
    COLUMN_INDEX_MAP = {
        3: 'jgb_total',             # 国債
        4: 'jgb_super_long',        # 超長期
        5: 'jgb_long',              # 利付長期
        6: 'jgb_medium',            # 利付中期
        7: 'jgb_discount',          # 割引
        8: 'jgb_tbill',             # 国庫短期証券等
        9: 'municipal_public',      # 公募地方債
        10: 'govt_guaranteed',      # 政府保証債
        11: 'filp_agency',          # 財投機関債等
        12: 'bank_debenture',       # 金融債
        13: 'samurai',              # 円貨建外国債
        14: 'corporate_total',      # 社債
        15: 'corporate_electric',   # 電力債
        16: 'corporate_general',    # 一般債
        17: 'abs',                  # 特定社債
        18: 'convertible',          # 新株予約権付社債
        19: 'private_offering_total', # 非公募債
        20: 'private_municipal',    # 地方債
        21: 'private_others',       # その他
        22: 'grand_total',          # 合計
        25: 'cp_total',             # 短期社債等
        27: 'cp_foreign'            # うち非居住者
    }

    def __init__(self):
        self.db = DatabaseManager()
        self.base_url = "https://www.jsda.or.jp/shiryoshitsu/toukei/tentoubaibai"

    def fetch_and_process_year(self, year: int) -> bool:
        """指定年度のExcelを取得して処理する"""
        urls = [f"{self.base_url}/koushasai{year}.xlsx"]
        # 2018年は修正版があるため、優先的に試行リストに追加
        if year == 2018:
            urls.insert(0, f"{self.base_url}/koushasai2018r.xlsx")
            
        excel_data = None
        last_error = None
        
        for url in urls:
            print(f"Fetching: {url}")
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                excel_data = BytesIO(response.content)
                break # 取得できたらループを抜ける
            except Exception as e:
                print(f"Failed to fetch {url}: {e}")
                last_error = e
                continue
        
        if not excel_data:
            print(f"Could not fetch data for year {year}: {last_error}")
            return False
        
        try:
            # Excelファイルをロード
            xl = pd.ExcelFile(excel_data)
            
            all_data = []
            
            for sheet_name in xl.sheet_names:
                # 対象シートか確認（空白除去してチェック）
                clean_sheet_name = sheet_name.replace(" ", "").replace("　", "")
                
                # 完全一致または部分一致でマッピングを探す
                mapping = None
                for key, val in self.SHEET_MAPPING.items():
                    if key in clean_sheet_name:
                        mapping = val
                        break
                
                if not mapping:
                    print(f"Skipping sheet: {sheet_name}")
                    continue
                
                print(f"Processing sheet: {sheet_name} ({mapping['side']}, {mapping['transaction_type']})")
                sheet_data = self._parse_sheet(xl, sheet_name, mapping)
                all_data.extend(sheet_data)
            
            if not all_data:
                print("No data extracted.")
                return False
                
            # DBに保存
            self._save_to_db(all_data)
            return True
            
        except Exception as e:
            print(f"Error processing year {year}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _parse_sheet(self, xl: pd.ExcelFile, sheet_name: str, mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """シートをパースして辞書リストを返す"""
        # ヘッダーが複数行あるため、データ開始行（通常5行目=index 4）から読む
        # ただし、カラム位置は固定として扱う
        df = pd.read_excel(xl, sheet_name=sheet_name, header=None)
        
        extracted_data = []
        
        # データ行を探す
        # A列(index 0)が日付形式 (YYYY/MM) になっている行からデータとみなす
        for idx, row in df.iterrows():
            date_val = row[0]
            investor_val = row[1]
            
            # 日付チェック
            if not self._is_valid_date_str(date_val):
                continue
            
            # 投資家区分が空ならスキップ
            if pd.isna(investor_val):
                continue

            # 日付変換 (YYYY/MM -> YYYY-MM-01)
            try:
                # Excelの日付シリアル値の場合と文字列の場合がある
                if isinstance(date_val, datetime):
                    ref_date = date_val.date()
                else:
                    # 文字列 "2024/04" などを想定
                    ref_date = pd.to_datetime(date_val).date()
            except:
                print(f"Invalid date format: {date_val}")
                continue

            # 投資家区分のクリーニング
            investor_type = str(investor_val).replace("_x000D_", "").strip()
            # 改行コードなどが含まれる場合があるので除去
            investor_type = re.sub(r'\s+', '', investor_type) 

            row_data = {
                'reference_date': ref_date,
                'investor_type': investor_type,
                'transaction_type': mapping['transaction_type'],
                'side': mapping['side']
            }
            
            # 数値カラムの取得
            has_value = False
            for col_idx, col_name in self.COLUMN_INDEX_MAP.items():
                if col_idx < len(row):
                    val = row[col_idx]
                    parsed_val = self._parse_numeric(val)
                    row_data[col_name] = parsed_val
                    if parsed_val is not None:
                        has_value = True
                else:
                    row_data[col_name] = None
            
            if has_value:
                extracted_data.append(row_data)
        
        return extracted_data

    def _is_valid_date_str(self, val: Any) -> bool:
        """セル値が有効な日付（年月）か簡易チェック"""
        if pd.isna(val):
            return False
        # datetimeオブジェクトならOK
        if isinstance(val, datetime):
            return True
        # 文字列で YYYY/MM 形式かチェック
        s = str(val)
        return re.match(r'^\d{4}/\d{2}', s) is not None

    def _parse_numeric(self, val: Any) -> Optional[float]:
        """数値をパースする。ハイフンやNaNはNone(NULL)にする"""
        if pd.isna(val):
            return None
        if isinstance(val, str):
            val = val.strip()
            if val == '-' or val == '':
                return None
            try:
                # カンマ除去など
                return float(val.replace(',', ''))
            except ValueError:
                return None
        try:
            return float(val)
        except:
            return None

    def _save_to_db(self, data: List[Dict[str, Any]]):
        """データをDBに保存 (UPSERT)"""
        if not data:
            return
            
        print(f"Saving {len(data)} records to database...")
        
        # カラム名のリスト作成（全データ共通のキーを持つ前提だが、念のため安全に）
        # bond_trade_volume_by_investor の全カラムを網羅する必要がある
        keys = list(data[0].keys())
        
        # SQL構築
        columns = ", ".join(keys)
        placeholders = ", ".join(["%s"] * len(keys))
        
        # ON CONFLICT UPDATE のセット句を作成
        # reference_date, investor_type, transaction_type, side 以外を更新
        update_columns = [k for k in keys if k not in 
                         ['reference_date', 'investor_type', 'transaction_type', 'side']]
        
        update_stmt = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        sql = f"""
            INSERT INTO bond_trade_volume_by_investor ({columns})
            VALUES ({placeholders})
            ON CONFLICT (reference_date, investor_type, transaction_type, side)
            DO UPDATE SET {update_stmt}, updated_at = CURRENT_TIMESTAMP
        """
        
        # データの値リスト作成
        values_list = []
        for item in data:
            values_list.append(tuple(item[k] for k in keys))
            
        # 実行 (DatabaseManagerのexecute_batch相当の処理が必要だが、
        # ここでは直接 execute_batch を使うためにコネクションを取得して処理する)
        
        try:
            with self.db._get_connection() as conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, sql, values_list, page_size=1000)
                    conn.commit()
            print("Successfully saved.")
        except Exception as e:
            print(f"Database error: {e}")
            raise

import psycopg2.extras
