"""
国債入札データ収集スクリプト

財務省の国債ヒストリカルデータ（Excelファイル）から入札結果を読み込み、
bond_auctionテーブルに保存する。

データソース: https://www.mof.go.jp/jgbs/reference/appendix/jgb_historical_data.xls
"""

import pandas as pd
import os
from typing import List, Dict, Optional
from datetime import datetime
import logging

# ロガー設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BondAuctionCollector:
    """国債入札データ収集クラス"""

    # 債券種類コードマッピング（既存DBに存在するもののみ）
    BOND_TYPE_CODES = {
        '2年債': '0042',
        '5年債': '0045',
        '10年債': '0067',
        '20年債': '0069',
        '30年債': '0068',
        '40年債': '0054',
        'GX10年債': '0058',
        'GX5年債': '0057',
        'TB ': '0074',  # スペース付き（Excelシート名）
        '4年債': '0044',
        '6年債': '0061',
    }

    def __init__(self, excel_path: str):
        """
        初期化

        Args:
            excel_path: 財務省ExcelファイルのパスまたはURL
        """
        self.excel_path = excel_path
        self.xls = None

    def load_excel(self) -> None:
        """Excelファイルを読み込む"""
        logger.info(f'Excelファイル読み込み中: {self.excel_path}')
        self.xls = pd.ExcelFile(self.excel_path)
        logger.info(f'シート数: {len(self.xls.sheet_names)}')

    def generate_bond_code(self, issue_number: int, bond_type: str) -> str:
        """
        銘柄コード（9桁）を生成

        形式: IIIIICCCCC
          IIIII = 回号（5桁、0パディング）
          CCCC = 債券種類コード（4桁）

        Args:
            issue_number: 回号
            bond_type: 債券種類（'2年債', '10年債'など）

        Returns:
            9桁の銘柄コード

        Raises:
            ValueError: 不明な債券種類の場合
        """
        type_code = self.BOND_TYPE_CODES.get(bond_type)
        if type_code is None:
            raise ValueError(f'不明な債券種類: {bond_type}')

        # 回号を5桁に0パディング
        issue_part = f'{issue_number:05d}'

        return f'{issue_part}{type_code}'

    def parse_sheet(self, sheet_name: str) -> List[Dict]:
        """
        シートを解析してデータを抽出

        Args:
            sheet_name: シート名

        Returns:
            入札データのリスト
        """
        logger.info(f'シート解析中: {sheet_name}')

        # 債券種類コードを取得
        if sheet_name not in self.BOND_TYPE_CODES:
            logger.warning(f'スキップ: {sheet_name}（種類コード未定義）')
            return []

        # ヘッダー行を探す
        df_raw = pd.read_excel(self.xls, sheet_name=sheet_name, header=None, nrows=10)
        header_row = None
        for idx, row in df_raw.iterrows():
            if '回号' in str(row.values):
                header_row = idx
                break

        if header_row is None:
            logger.warning(f'ヘッダー行が見つかりません: {sheet_name}')
            return []

        # データを読み込み
        df = pd.read_excel(self.xls, sheet_name=sheet_name, header=header_row)
        df = df.dropna(how='all')

        # 単位行を削除（「（％）」などを含む行）
        df = df[df['回号'] != '（％）']
        df = df[df['回号'].notna()]

        # 回号を数値に変換
        df['回号'] = pd.to_numeric(df['回号'], errors='coerce')
        df = df[df['回号'].notna()]

        # データを辞書リストに変換
        records = []
        for _, row in df.iterrows():
            try:
                issue_number = int(row['回号'])
                bond_code = self.generate_bond_code(issue_number, sheet_name)

                # 日付を変換
                auction_date = pd.to_datetime(row['入札日']).date() if pd.notna(row['入札日']) else None
                issue_date = pd.to_datetime(row['発行日']).date() if pd.notna(row['発行日']) else None
                maturity_date = pd.to_datetime(row['償還日']).date() if pd.notna(row['償還日']) else None

                if auction_date is None or issue_date is None or maturity_date is None:
                    logger.warning(f'日付欠損をスキップ: {sheet_name} 第{issue_number}回')
                    continue

                # 数値フィールドの変換（「―」はNoneに）
                def to_numeric(value):
                    if pd.isna(value) or value == '―':
                        return None
                    try:
                        return float(value)
                    except:
                        return None

                # 基本フィールド
                allocated_amount = to_numeric(row.get('落札・割当額'))
                type1_noncompetitive = to_numeric(row.get('第Ⅰ非価格競争'))
                type2_noncompetitive = to_numeric(row.get('第Ⅱ非価格競争'))

                # total_amount計算（NULL値は0として扱う）
                total_amount = (
                    (allocated_amount or 0) +
                    (type1_noncompetitive or 0) +
                    (type2_noncompetitive or 0)
                )

                record = {
                    'bond_code': bond_code,
                    'auction_date': auction_date,
                    'issue_number': issue_number,
                    'issue_date': issue_date,
                    'maturity_date': maturity_date,
                    'coupon_rate': to_numeric(row.get('表面利率')),
                    'planned_amount': int(row['発行予定額']) if pd.notna(row.get('発行予定額')) else None,
                    'offered_amount': int(row['応募額']) if pd.notna(row.get('応募額')) else None,
                    'allocated_amount': allocated_amount,
                    'average_price': to_numeric(row.get('平均価格')),
                    'average_yield': to_numeric(row.get('平均利回')),
                    'lowest_price': to_numeric(row.get('最低価格', row.get('最低価格*'))),
                    'highest_yield': to_numeric(row.get('最高利回')),
                    'fixed_rate_or_noncompetitive': to_numeric(row.get('定率/非競争')),
                    'type1_noncompetitive': type1_noncompetitive,
                    'type2_noncompetitive': type2_noncompetitive,
                    'total_amount': total_amount,
                    'data_source': 'MOF_JGB_Historical_Data'
                }

                records.append(record)

            except Exception as e:
                logger.error(f'レコード処理エラー: {sheet_name} 第{issue_number}回 - {e}')
                continue

        logger.info(f'{sheet_name}: {len(records)}件のレコードを抽出')
        return records

    def collect_all_data(self) -> List[Dict]:
        """
        全シートからデータを収集

        Returns:
            全入札データのリスト
        """
        if self.xls is None:
            self.load_excel()

        all_records = []

        for sheet_name in self.xls.sheet_names:
            if sheet_name in self.BOND_TYPE_CODES:
                records = self.parse_sheet(sheet_name)
                all_records.extend(records)

        logger.info(f'総レコード数: {len(all_records)}件')
        return all_records


if __name__ == '__main__':
    # テスト実行
    excel_path = '/tmp/jgb_historical_data.xls'

    collector = BondAuctionCollector(excel_path)
    collector.load_excel()

    # 10年債のみテスト
    records = collector.parse_sheet('10年債')

    print(f'\n=== サンプルレコード（最初の3件） ===\n')
    for i, record in enumerate(records[:3], 1):
        print(f'{i}. {record["bond_code"]} - {record["auction_date"]}')
        print(f'   回号: {record["issue_number"]}, 表面利率: {record["coupon_rate"]}%')
        print(f'   発行額: {record["planned_amount"]}億円, 平均利回: {record["average_yield"]}%')
        print()
