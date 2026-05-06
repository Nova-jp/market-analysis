#!/usr/bin/env python3
"""
TDB（割引短期国債）ヒストリカルデータ収集モジュール

財務省のTDBヒストリカルデータ（Excel）から:
1. 全28シート（平成11年度～令和7年度）のデータを取得
2. 各銘柄をbond_auctionテーブルに登録（auction_type='TDB'）

データソース:
- https://www.mof.go.jp/jgbs/reference/appendix/fb_historical_data.xls
"""

import pandas as pd
import requests
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TDBCollector:
    """TDB（割引短期国債）データ収集クラス"""

    EXCEL_URL = "https://www.mof.go.jp/jgbs/reference/appendix/fb_historical_data.xls"
    TDB_TYPE_CODE = "0074"  # TDB銘柄コード

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })

    def download_excel(self, save_path: Optional[str] = None) -> str:
        """
        Excelファイルをダウンロード

        Args:
            save_path: 保存先パス（Noneの場合は一時ファイル）

        Returns:
            ダウンロードしたファイルのパス
        """
        logger.info(f"📥 TDB Excelファイルをダウンロード中: {self.EXCEL_URL}")

        response = self.session.get(self.EXCEL_URL, timeout=30)
        response.raise_for_status()

        if save_path is None:
            import tempfile
            save_path = tempfile.mktemp(suffix='.xls')

        with open(save_path, 'wb') as f:
            f.write(response.content)

        logger.info(f"✅ ダウンロード完了: {save_path}")
        return save_path

    def parse_excel_all_sheets(self, excel_path: str) -> List[Dict[str, Any]]:
        """
        Excelから全シート（全年度）のデータを抽出

        Args:
            excel_path: Excelファイルパス

        Returns:
            全TDBデータのリスト
        """
        logger.info(f"📊 TDB Excelファイルをパース中: {excel_path}")

        # 全シート名を取得
        xl_file = pd.ExcelFile(excel_path)
        sheet_names = xl_file.sheet_names

        logger.info(f"✅ {len(sheet_names)} シートを検出")

        all_tdb_data = []

        for sheet_name in sheet_names:
            logger.info(f"\n📄 シート: {sheet_name}")

            # シートを読み込み（header=2: 2行目からヘッダー）
            df = pd.read_excel(excel_path, sheet_name=sheet_name, header=2)

            # データクレンジング
            df = df[df['入札日'].notna()]  # 入札日がNaNの行を除外

            logger.info(f"  データ行数: {len(df)}件")

            for idx, row in df.iterrows():
                try:
                    tdb_record = self._parse_row(row, sheet_name)
                    if tdb_record:
                        all_tdb_data.append(tdb_record)

                except Exception as e:
                    logger.error(f"❌ シート {sheet_name} 行 {idx} のパースエラー: {e}")
                    continue

        logger.info(f"\n✅ 全シート合計: {len(all_tdb_data)} 件のTDBデータを抽出")
        return all_tdb_data

    def _parse_row(self, row: pd.Series, sheet_name: str) -> Optional[Dict[str, Any]]:
        """
        1行のデータをパース

        Args:
            row: データ行
            sheet_name: シート名

        Returns:
            TDBデータ辞書
        """
        # 回号を抽出
        issue_number_str = str(row.get('回号', ''))
        if not issue_number_str or pd.isna(row.get('回号')):
            return None

        # 回号を整数に変換
        try:
            issue_number = int(float(issue_number_str))
        except:
            logger.warning(f"⚠️  回号の変換失敗: {issue_number_str}")
            return None

        # 銘柄コード生成: 回号（5桁0パディング） + '0074'
        bond_code = f"{issue_number:05d}{self.TDB_TYPE_CODE}"

        # 日付のパース
        auction_date = self._parse_date(row.get('入札日'))
        issue_date = self._parse_date(row.get('発行日'))
        maturity_date = self._parse_date(row.get('償還日'))

        if not auction_date or not issue_date or not maturity_date:
            logger.warning(f"⚠️  日付パース失敗: {sheet_name}, 回号{issue_number}")
            return None

        # 発行額フィールド
        allocated_amount = self._parse_amount(row.get('募入決定額'))
        type1_noncompetitive = self._parse_amount(row.get('第Ⅰ非価格競争'))

        # total_amount計算（NULL値は0として扱う）
        total_amount = (
            (allocated_amount or 0) +
            (type1_noncompetitive or 0)
        )

        tdb_data = {
            # 主キー
            'bond_code': bond_code,
            'auction_date': str(auction_date),

            # 基本情報
            'issue_number': issue_number,
            'issue_date': str(issue_date),
            'maturity_date': str(maturity_date),
            'coupon_rate': None,  # TDBは割引債のため利率なし

            # 発行規模
            'planned_amount': None,  # Excelにデータなし
            'offered_amount': self._parse_amount(row.get('応募額')),
            'allocated_amount': allocated_amount,

            # 価格・利回り
            'average_price': self._parse_price(row.get('平均価格')),
            'average_yield': self._parse_yield(row.get('平均利回')),
            'lowest_price': self._parse_price(row.get('最低価格')),
            'highest_yield': self._parse_yield(row.get('最高利回')),

            # 非価格競争
            'fixed_rate_or_noncompetitive': None,
            'type1_noncompetitive': type1_noncompetitive,
            'type2_noncompetitive': None,
            'total_amount': total_amount,

            # メタデータ
            'data_source': f'MOF_TDB_{sheet_name}',
            'auction_type': 'TDB'
        }

        return tdb_data

    def _parse_date(self, value: Any) -> Optional[datetime]:
        """日付パース"""
        if pd.isna(value):
            return None

        try:
            if isinstance(value, datetime):
                return value.date()
            elif isinstance(value, str):
                return pd.to_datetime(value).date()
            else:
                return pd.to_datetime(str(value)).date()
        except:
            return None

    def _parse_amount(self, value: Any) -> Optional[float]:
        """金額パース（億円単位）"""
        if pd.isna(value):
            return None

        try:
            # "6,000" → 6000.0
            amount_str = str(value).replace(',', '').strip()
            return float(amount_str)
        except:
            return None

    def _parse_price(self, value: Any) -> Optional[float]:
        """価格パース（例: 99.980）"""
        if pd.isna(value):
            return None

        try:
            return float(str(value).replace(',', '').strip())
        except:
            return None

    def _parse_yield(self, value: Any) -> Optional[float]:
        """利回りパース（例: 0.123）"""
        if pd.isna(value):
            return None

        try:
            return float(str(value).replace(',', '').strip())
        except:
            return None

    def collect_all_data(self) -> List[Dict[str, Any]]:
        """
        全TDBデータを収集

        Returns:
            各TDBの詳細データリスト
        """
        logger.info("=" * 70)
        logger.info("TDB（割引短期国債）データ収集開始")
        logger.info("=" * 70)

        # Step 1: Excelダウンロード
        excel_path = self.download_excel()

        # Step 2: 全シートパース
        all_tdb_data = self.parse_excel_all_sheets(excel_path)

        logger.info("\n" + "=" * 70)
        logger.info(f"✅ 収集完了: 合計 {len(all_tdb_data)} 件のTDBデータ")
        logger.info("=" * 70)

        return all_tdb_data


if __name__ == "__main__":
    # テスト実行
    collector = TDBCollector()

    print("\n🧪 テスト: 令和7年度のデータを収集\n")

    excel_path = collector.download_excel()

    # 令和7年度シートのみテスト
    df = pd.read_excel(excel_path, sheet_name='令和7年度', header=2)
    df = df[df['入札日'].notna()]

    print(f"令和7年度: {len(df)}件のTDB")

    # 最初の3件を表示
    for idx, row in df.head(3).iterrows():
        issue_number = int(float(row['回号']))
        bond_code = f"{issue_number:05d}0074"
        print(f"\n第{issue_number}回:")
        print(f"  銘柄コード: {bond_code}")
        print(f"  入札日: {row['入札日']}")
        print(f"  募入決定額: {row['募入決定額']}億円")
        print(f"  平均価格: {row['平均価格']}")
        print(f"  平均利回: {row['平均利回']}")
