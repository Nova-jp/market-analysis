#!/usr/bin/env python3
"""
Bond Data Processor
JSDAからの債券データを処理するメインクラス
- 政府系債券（T-bills、国債）の25列データ取得
- 異常値を適切にNullに変換
- Supabaseデータベースへの保存
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import requests
import io
import logging
from typing import Optional, Dict, List
import os
from dotenv import load_dotenv
import jpholiday

load_dotenv()

class BondDataProcessor:
    """政府系債券データ処理クラス（25列対応）"""
    
    def __init__(self):
        self.base_url = "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files"
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        
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
            17: 'low_price',                # R: 最低値単価
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
        # datetime.date または datetime.datetime のどちらでも対応
        if isinstance(date_obj, datetime):
            target_date = date_obj.date()
        else:
            target_date = date_obj

        year = target_date.year
        month = target_date.month
        date_str = target_date.strftime("%y%m%d")
        filename = f"S{date_str}.csv"

        # 2019年以前は月別ディレクトリ構造
        if year <= 2019:
            url = f"{self.base_url}/{year}/{month:02d}/{filename}"
        else:
            # 2020年以降は年別ディレクトリのみ
            url = f"{self.base_url}/{year}/{filename}"

        # 実際の取引日を計算（公表日の1営業日前）
        actual_trade_date = self.get_previous_business_day(target_date)

        return url, filename, actual_trade_date
    
    def download_data_for_date(self, date_obj) -> Optional[pd.DataFrame]:
        """指定日のデータをダウンロード（フォルダ年またぎ対応）"""
        url, filename, actual_trade_date = self.build_csv_url(date_obj)
        
        # 1. 標準URLで試行
        df = self.download_csv_data(url)
        if df is not None:
            return df
            
        # 2. 失敗時、1月なら前年フォルダを試行
        # JSDAのフォルダ構造は「公表日」基準だが、年またぎの時期は「取引日（前年）」のフォルダに含まれることがある
        if isinstance(date_obj, datetime):
            target_date = date_obj.date()
        else:
            target_date = date_obj
            
        if target_date.month == 1:
            self.logger.info(f"標準URLで取得失敗。1月のため前年フォルダ({target_date.year - 1})を試行します。")
            
            # URL再構築
            year = target_date.year - 1
            month = target_date.month
            
            # 2019年以前の対応（月別）- 今回のケース(2025/2026)では関係ないが念のため
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
            # 日付フィールド
            if field_name in ['trade_date', 'due_date']:
                date_str = str(int(value)) if len(str(int(value))) == 8 else None
                if date_str:
                    return datetime.strptime(date_str, '%Y%m%d').date()
                return None
            
            # 整数フィールド
            elif field_name in ['issue_type', 'interest_payment_day', 'reporting_members']:
                try:
                    # 文字列の特殊値をチェック
                    if isinstance(value, str) and value.strip() in ['--', '-----', 'nan', '']:
                        return None
                    # 浮動小数点数として読み込んでから整数に変換
                    float_val = float(value)
                    if pd.isna(float_val) or float_val < 0:
                        return None
                    return int(float_val)
                except:
                    return None
            
            # 利払日フィールド（05/11形式の文字列をそのまま保存）
            elif field_name == 'interest_payment_date':
                try:
                    # 文字列の特殊値をチェック
                    if isinstance(value, str) and value.strip() in ['--', '-----', 'nan', '']:
                        return None
                    if pd.isna(value):
                        return None
                    
                    # 数値の場合、MM/DD形式の文字列に変換（例: 315 -> "03/15", 1201 -> "12/01"）
                    if isinstance(value, (int, float)):
                        date_int = int(value)
                        if date_int <= 0:
                            return None
                        
                        # 4桁の場合: MMDD形式
                        if date_int >= 1000:
                            month = date_int // 100
                            day = date_int % 100
                            # 月日の妥当性チェック
                            if 1 <= month <= 12 and 1 <= day <= 31:
                                return f"{month:02d}/{day:02d}"
                        # 3桁の場合: MDD形式
                        elif date_int >= 100:
                            month = date_int // 100
                            day = date_int % 100
                            # 月日の妥当性チェック
                            if 1 <= month <= 12 and 1 <= day <= 31:
                                return f"{month:02d}/{day:02d}"
                        # 2桁以下の場合: 月のみまたは日のみ
                        else:
                            if 1 <= date_int <= 12:
                                return f"{date_int:02d}"  # 月のみ
                            else:
                                return f"{date_int:02d}"  # 日のみ（月は不明）
                        
                        return None
                    
                    # 文字列の場合、そのまま返す
                    elif isinstance(value, str):
                        return value.strip()
                    
                    return None
                except:
                    return None
            
            # クーポン率
            elif field_name == 'coupon_rate':
                if self.is_invalid_value(value, 'coupon_rate'):
                    return None
                return float(value)
            
            # 利回り系フィールド
            elif 'yield' in field_name:
                if self.is_invalid_value(value, 'yields'):
                    return None
                return float(value)
            
            # 価格系フィールド
            elif 'price' in field_name:
                if self.is_invalid_value(value, 'prices'):
                    return None
                return float(value)
            
            # 変化系フィールド
            elif 'change' in field_name:
                return float(value)
            
            # 文字列フィールド
            elif field_name == 'bond_code':
                # bond_codeは9桁に0パディング
                code_str = str(value).strip()
                try:
                    # 数値に変換可能な場合は9桁にパディング
                    code_int = int(code_str)
                    return f'{code_int:09d}'
                except ValueError:
                    # 数値でない場合はそのまま返す
                    return code_str
            elif field_name == 'bond_name':
                return str(value).strip()
            
            # デフォルト（float）
            else:
                return float(value)
                
        except:
            return None
    
    def filter_target_bonds(self, df: pd.DataFrame) -> pd.DataFrame:
        """対象債券（issue_type 1,2）のみフィルタリング"""
        if df is None or df.empty:
            return pd.DataFrame()
        
        # issue_type 1または2のみ抽出
        target_df = df[df[1].isin([1, 2])].copy()
        self.logger.info(f"対象債券フィルタリング結果: {len(target_df)}件")
        
        return target_df
    
    def process_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """生データを処理"""
        if df is None or df.empty:
            return pd.DataFrame()
        
        # 対象債券のみフィルタリング
        filtered_df = self.filter_target_bonds(df)
        
        if filtered_df.empty:
            return pd.DataFrame()
        
        processed_data = []
        
        for _, row in filtered_df.iterrows():
            try:
                processed_record = {}
                
                # 各列を処理
                for col_idx, field_name in self.column_mapping.items():
                    if col_idx < len(row):
                        processed_record[field_name] = self.safe_convert(row[col_idx], field_name)
                    else:
                        processed_record[field_name] = None
                
                # 必須フィールドチェック
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
        
        # JSON serialization対応
        # 1. 日付カラムを文字列形式に変換
        date_columns = ['trade_date', 'due_date']
        for col in date_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].apply(
                    lambda x: x.isoformat() if hasattr(x, 'isoformat') else x
                )
        
        # 2. NaN値をNoneに変換 (object変換することで完全にNoneに置換)
        result_df = result_df.astype(object).where(pd.notnull(result_df), None)
        
        self.logger.info(f"処理済みデータ: {len(result_df)}件")
        
        return result_df
    
    def save_to_supabase(self, df: pd.DataFrame, batch_size: int = 100) -> bool:
        """Supabaseに保存"""
        if df.empty:
            self.logger.warning("保存するデータがありません")
            return False
        
        headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json'
        }
        
        success_count = 0
        total_count = len(df)
        
        for i in range(0, total_count, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_data = []
            
            for _, row in batch_df.iterrows():
                data = {}
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value):
                        data[col] = None
                    elif col in ['trade_date', 'due_date']:
                        data[col] = value.isoformat() if value else None
                    elif col in ['issue_type', 'interest_payment_month', 'interest_payment_day', 'reporting_members']:
                        # 整数フィールドを確実に整数型にする
                        if value is not None and not pd.isna(value):
                            try:
                                data[col] = int(float(value))
                            except:
                                data[col] = None
                        else:
                            data[col] = None
                    else:
                        data[col] = value
                
                batch_data.append(data)
            
            try:
                response = requests.post(
                    f"{self.supabase_url}/rest/v1/bond_data",
                    headers=headers,
                    json=batch_data
                )
                
                if response.status_code in [200, 201]:
                    success_count += len(batch_data)
                    self.logger.info(f"バッチ {i//batch_size + 1} 保存完了: {len(batch_data)}件")
                else:
                    self.logger.warning(f"バッチ保存失敗: {response.status_code} - {response.text}")
                    
            except Exception as e:
                self.logger.error(f"バッチ保存エラー: {e}")
                continue
        
        self.logger.info(f"保存完了: {success_count}/{total_count}件")
        return success_count > 0
    
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
            
            # download_data_for_dateを使用して取得（年またぎ対応）
            raw_df = self.download_data_for_date(test_date)
            
            if raw_df is not None:
                processed_df = self.process_raw_data(raw_df)
                result['processed_records'] = len(processed_df)
                
                if not processed_df.empty:
                    if self.save_to_supabase(processed_df):
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
    
    print(f"結果: {'✅ 成功' if result['success'] else '❌ 失敗'}")
    print(f"メッセージ: {result['message']}")
    print(f"処理件数: {result['processed_records']}")
    print(f"保存件数: {result['saved_records']}")
    
    return result['success']

if __name__ == "__main__":
    main()