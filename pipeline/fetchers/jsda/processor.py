#!/usr/bin/env python3
"""
Bond Data Processor
JSDAからの債券データを処理するメインクラス
- 政府系債券（T-bills、国債）の25列データ取得
- 異常値を適切にNullに変換
- Neon PostgreSQL への保存（core.db.sync_client 経由）
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import requests
import io
import logging
from typing import Optional, Dict, List
import jpholiday

from core.db.sync_client import DatabaseManager


class BondDataProcessor:
    """政府系債券データ処理クラス（25列対応）"""

    def __init__(self):
        self.base_url = "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files"
        self.db = DatabaseManager()
        self._available_dates_cache = None  # HTML日付リストのキャッシュ

        # 異常値パターン定義
        self.invalid_values = {
            'coupon_rate': [99.999],
            'yields': [999.999],  # 全ての利回り系フィールド
            'prices': [999.99],   # 全ての価格系フィールド
            'changes': [],        # 変化系フィールドの異常値（未定義）
        }

        # 列マッピング（L,M,N,Tを除く25列）
        self.column_mapping = {
            0: 'trade_date',                # A: 日付
            1: 'issue_type',                # B: 銘柄種別
            2: 'bond_code',                 # C: 銘柄コード
            3: 'bond_name',                 # D: 銘柄名
            4: 'due_date',                  # E: 償還期日
            5: 'coupon_rate',               # F: 利率
            6: 'ave_compound_yield',        # G: 平均値複利
            7: 'ave_price',                 # H: 平均値単価
            8: 'price_change',              # I: 平均値単価前日比
            9: 'interest_payment_date',     # J: 利払日（MM/DD形式の文字列）
            10: 'interest_payment_day',     # K: 利払日（日）
            # L,M,N(11,12,13)はスキップ
            14: 'ave_simple_yield',         # O: 平均値単利
            15: 'high_price',               # P: 最高値単価
            16: 'high_simple_yield',        # Q: 最高値単利
            17: 'low_price',               # R: 最低値単価
            18: 'low_simple_yield',         # S: 最低値単利
            # T(19)はスキップ
            20: 'reporting_members',        # U: 報告社数
            21: 'highest_compound_yield',   # V: 最高値複利
            22: 'highest_price_change',     # W: 最高値単価前日比
            23: 'lowest_compound_yield',    # X: 最低値複利
            24: 'lowest_price_change',      # Y: 最低値単価前日比
            25: 'median_compound_yield',    # Z: 中央値複利
            26: 'median_simple_yield',      # AA: 中央値単利
            27: 'median_price',             # AB: 中央値単価
            28: 'median_price_change',      # AC: 中央値単価前日比
        }

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_latest_business_date(self) -> datetime:
        """最新の営業日を取得"""
        today = datetime.now()
        for i in range(7):
            check_date = today - timedelta(days=i)
            if check_date.weekday() < 5:  # 平日
                return check_date
        return today

    def get_previous_business_day(self, d: date) -> date:
        """1営業日前を取得（土日・祝日スキップ）"""
        prev = d - timedelta(days=1)
        while prev.weekday() >= 5 or jpholiday.is_holiday(prev):
            prev -= timedelta(days=1)
        return prev

    def determine_trade_date_from_html(self, target_date: date) -> Optional[date]:
        """
        HTMLの日付リストに基づいて、指定された公表日に対応する実際の取引日を特定する

        ルール: HTMLにある日付リストの中で、target_dateの一つ前の日付を取引日とする
        例: リスト [..., 2026-01-05, 2025-12-30, ...] で target_date が 2026-01-05 の場合、
            2025-12-30 を返す。
        """
        from core.utils.jsda_parser import JSDAParser

        try:
            if self._available_dates_cache is None:
                parser = JSDAParser()
                search_start_year = target_date.year
                if target_date.month <= 2:
                    search_start_year -= 1
                search_start_year = max(search_start_year, 2002)

                self.logger.info(f"HTML日付リストを取得中 (開始年: {search_start_year})...")
                self._available_dates_cache = parser.get_available_dates_from_html(start_year=search_start_year)

            available_dates = self._available_dates_cache

            if not available_dates:
                return None

            if target_date in available_dates:
                index = available_dates.index(target_date)
                if index + 1 < len(available_dates):
                    prev_date = available_dates[index + 1]
                    self.logger.info(f"HTML日付補正: 公表日 {target_date} -> 取引日 {prev_date}")
                    return prev_date
                else:
                    self.logger.warning(f"公表日 {target_date} はリストの最古です（これより前の日付が見つかりません）")
            else:
                self.logger.warning(f"公表日 {target_date} がHTMLの日付リストに見つかりません")

            return None

        except Exception as e:
            self.logger.error(f"HTML日付判定エラー: {e}")
            return None

    def build_csv_url(self, date_obj) -> tuple:
        """CSVファイルURLを構築し、実際の取引日を計算

        JSDAのCSVファイル名の日付 = 公表日（翌営業日）
        実際の取引日 = 公表日の1営業日前

        Args:
            date_obj: CSV公表日（JSDAサイトの表示日付） - datetime or date

        Returns:
            tuple: (url, filename, actual_trade_date)

        2019年以前: /files/YYYY/MM/S{YYMMDD}.csv
        2020年以降: /files/YYYY/S{YYMMDD}.csv
        """
        if isinstance(date_obj, datetime):
            target_date = date_obj.date()
        else:
            target_date = date_obj

        year = target_date.year
        month = target_date.month
        date_str = target_date.strftime("%y%m%d")
        filename = f"S{date_str}.csv"

        if year <= 2019:
            url = f"{self.base_url}/{year}/{month:02d}/{filename}"
        else:
            url = f"{self.base_url}/{year}/{filename}"

        actual_trade_date = self.get_previous_business_day(target_date)

        return url, filename, actual_trade_date

    def download_data_for_date(self, date_obj) -> Optional[pd.DataFrame]:
        """指定日のデータをダウンロード（フォルダ年またぎ対応）"""
        url, filename, actual_trade_date = self.build_csv_url(date_obj)

        df = self.download_csv_data(url)
        if df is not None:
            return df

        if isinstance(date_obj, datetime):
            target_date = date_obj.date()
        else:
            target_date = date_obj

        if target_date.month == 1:
            self.logger.info(f"標準URLで取得失敗。1月のため前年フォルダ({target_date.year - 1})を試行します。")

            year = target_date.year - 1
            month = target_date.month

            if year <= 2019:
                url_alt = f"{self.base_url}/{year}/{month:02d}/{filename}"
            else:
                url_alt = f"{self.base_url}/{year}/{filename}"

            df = self.download_csv_data(url_alt)
            if df is not None:
                self.logger.info(f"前年フォルダから取得成功: {url_alt}")
                return df

        return None

    def download_csv_data(self, url: str) -> Optional[pd.DataFrame]:
        """CSVデータをダウンロード"""
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()

            try:
                content = response.content.decode('shift_jis')
            except UnicodeDecodeError:
                content = response.content.decode('utf-8')

            df = pd.read_csv(io.StringIO(content), header=None)
            return df

        except Exception as e:
            self.logger.error(f"CSV取得失敗 {url}: {e}")
            return None

    def is_invalid_value(self, value, field_type: str) -> bool:
        """異常値かどうかを判定"""
        if pd.isna(value):
            return True

        try:
            float_val = float(value)
            if field_type == 'coupon_rate':
                return float_val in self.invalid_values['coupon_rate']
            elif field_type == 'yields':
                return float_val in self.invalid_values['yields']
            elif field_type == 'prices':
                return float_val in self.invalid_values['prices']
            return False
        except:
            return True

    def safe_convert(self, value, field_name: str):
        """安全な型変換（異常値を考慮）"""
        if pd.isna(value):
            return None

        try:
            if field_name in ['trade_date', 'due_date']:
                date_str = str(int(value)) if len(str(int(value))) == 8 else None
                if date_str:
                    return datetime.strptime(date_str, '%Y%m%d').date()
                return None

            elif field_name in ['issue_type', 'interest_payment_day', 'reporting_members']:
                try:
                    if isinstance(value, str) and value.strip() in ['--', '-----', 'nan', '']:
                        return None
                    float_val = float(value)
                    if pd.isna(float_val) or float_val < 0:
                        return None
                    return int(float_val)
                except:
                    return None

            elif field_name == 'interest_payment_date':
                try:
                    if isinstance(value, str) and value.strip() in ['--', '-----', 'nan', '']:
                        return None
                    if pd.isna(value):
                        return None

                    if isinstance(value, (int, float)):
                        date_int = int(value)
                        if date_int <= 0:
                            return None

                        if date_int >= 1000:
                            month = date_int // 100
                            day = date_int % 100
                            if 1 <= month <= 12 and 1 <= day <= 31:
                                return f"{month:02d}/{day:02d}"
                        elif date_int >= 100:
                            month = date_int // 100
                            day = date_int % 100
                            if 1 <= month <= 12 and 1 <= day <= 31:
                                return f"{month:02d}/{day:02d}"
                        else:
                            if 1 <= date_int <= 12:
                                return f"{date_int:02d}"
                            else:
                                return f"{date_int:02d}"

                        return None

                    elif isinstance(value, str):
                        return value.strip()

                    return None
                except:
                    return None

            elif field_name == 'coupon_rate':
                if self.is_invalid_value(value, 'coupon_rate'):
                    return None
                return float(value)

            elif 'yield' in field_name:
                if self.is_invalid_value(value, 'yields'):
                    return None
                return float(value)

            elif 'price' in field_name:
                if self.is_invalid_value(value, 'prices'):
                    return None
                return float(value)

            elif 'change' in field_name:
                return float(value)

            elif field_name == 'bond_code':
                code_str = str(value).strip()
                try:
                    code_int = int(code_str)
                    return f'{code_int:09d}'
                except ValueError:
                    return code_str
            elif field_name == 'bond_name':
                return str(value).strip()

            else:
                return float(value)

        except:
            return None

    def filter_target_bonds(self, df: pd.DataFrame) -> pd.DataFrame:
        """対象債券（issue_type 1,2）のみフィルタリング"""
        if df is None or df.empty:
            return pd.DataFrame()

        target_df = df[df[1].isin([1, 2])].copy()
        self.logger.info(f"対象債券フィルタリング結果: {len(target_df)}件")

        return target_df

    def process_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """生データを処理"""
        if df is None or df.empty:
            return pd.DataFrame()

        filtered_df = self.filter_target_bonds(df)

        if filtered_df.empty:
            return pd.DataFrame()

        processed_data = []

        for _, row in filtered_df.iterrows():
            try:
                processed_record = {}

                for col_idx, field_name in self.column_mapping.items():
                    if col_idx < len(row):
                        processed_record[field_name] = self.safe_convert(row[col_idx], field_name)
                    else:
                        processed_record[field_name] = None

                if all([
                    processed_record.get('trade_date'),
                    processed_record.get('issue_type'),
                    processed_record.get('bond_code'),
                    processed_record.get('bond_name')
                ]):
                    processed_data.append(processed_record)

            except Exception as e:
                self.logger.warning(f"行処理失敗: {e}")
                continue

        result_df = pd.DataFrame(processed_data)

        date_columns = ['trade_date', 'due_date']
        for col in date_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].apply(
                    lambda x: x.isoformat() if hasattr(x, 'isoformat') else x
                )

        result_df = result_df.astype(object).where(pd.notnull(result_df), None)

        self.logger.info(f"処理済みデータ: {len(result_df)}件")

        return result_df

    def save_to_db(self, df: pd.DataFrame) -> bool:
        """Neon PostgreSQL に保存（ON CONFLICT DO NOTHING）"""
        if df.empty:
            self.logger.warning("保存するデータがありません")
            return False

        data_list = df.to_dict(orient='records')
        saved = self.db.batch_insert_data(data_list, table_name='bond_data')
        self.logger.info(f"保存完了: {saved}/{len(data_list)}件")
        return saved > 0

    def process_latest_data(self) -> Dict:
        """最新データを処理"""
        result = {
            'success': False,
            'message': '',
            'processed_records': 0,
            'saved_records': 0
        }

        for i in range(5):  # 過去5日間を試行
            test_date = self.get_latest_business_date() - timedelta(days=i)
            if test_date.weekday() >= 5:
                continue

            raw_df = self.download_data_for_date(test_date)

            if raw_df is not None:
                processed_df = self.process_raw_data(raw_df)
                result['processed_records'] = len(processed_df)

                if not processed_df.empty:
                    if self.save_to_db(processed_df):
                        result['success'] = True
                        result['message'] = f"JSDAデータ処理完了: {test_date.strftime('%Y-%m-%d')}"
                        result['saved_records'] = len(processed_df)
                        return result

        result['message'] = "有効なデータが見つかりませんでした"
        return result


def main():
    """メイン実行"""
    processor = BondDataProcessor()

    print("=== 政府系債券データ処理 ===")
    print(f"実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    result = processor.process_latest_data()

    print(f"結果: {'成功' if result['success'] else '失敗'}")
    print(f"メッセージ: {result['message']}")
    print(f"処理件数: {result['processed_records']}")
    print(f"保存件数: {result['saved_records']}")

    return result['success']


if __name__ == "__main__":
    main()
