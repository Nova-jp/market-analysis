#!/usr/bin/env python3
"""
流動性供給入札データ収集モジュール

財務省の流動性供給入札ヒストリカルデータ（Excel）から:
1. 入札サマリー情報を取得
2. Excel内のハイパーリンクから追加発行銘柄詳細を取得
3. 各銘柄をbond_auctionテーブルに登録（auction_type='tap'）

データソース:
- https://www.mof.go.jp/jgbs/reference/appendix/ryudousei_historical_data.xls
"""

import pandas as pd
import xlrd
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging
import time
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LiquiditySupplyCollector:
    """流動性供給入札データ収集クラス"""

    EXCEL_URL = "https://www.mof.go.jp/jgbs/reference/appendix/ryudousei_historical_data.xls"
    BASE_URL = "https://www.mof.go.jp"

    # 国立国会図書館 WARP アーカイブのpid候補（期間ごとに異なる可能性）
    WARP_PIDS = [
        '11949862',  # 2020-2021年頃
        '11424711',  # 2019-2020年頃
        '12654644',  # 別の期間候補
        '258151',    # 別の期間候補
        '11618512',  # 別の期間候補
    ]

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
        logger.info(f"📥 Excelファイルをダウンロード中: {self.EXCEL_URL}")

        response = self.session.get(self.EXCEL_URL, timeout=30)
        response.raise_for_status()

        if save_path is None:
            import tempfile
            save_path = tempfile.mktemp(suffix='.xls')

        with open(save_path, 'wb') as f:
            f.write(response.content)

        logger.info(f"✅ ダウンロード完了: {save_path}")
        return save_path

    def parse_excel_summary(self, excel_path: str) -> List[Dict[str, Any]]:
        """
        Excelから入札サマリー情報を抽出

        Args:
            excel_path: Excelファイルパス

        Returns:
            入札サマリーのリスト
        """
        logger.info(f"📊 Excelファイルをパース中: {excel_path}")

        # pandasで読み込み（データ抽出用）
        df = pd.read_excel(excel_path, sheet_name='流動性', header=1)

        # 列名の改行を削除
        df.columns = df.columns.str.replace('\n', '')

        # データクレンジング
        df = df[df['入札日'].notna()]  # ヘッダー行を除外

        summaries = []

        for idx, row in df.iterrows():
            try:
                # 回号を抽出（例: "第437回" → 437）
                round_str = str(row.iloc[0])
                round_match = re.search(r'第?(\d+)回?', round_str)
                round_number = int(round_match.group(1)) if round_match else None

                if round_number is None:
                    logger.warning(f"⚠️  回号が抽出できません: {round_str}")
                    continue

                summary = {
                    'round_number': round_number,
                    'auction_date': pd.to_datetime(row['入札日']).date(),
                    'issue_date': pd.to_datetime(row['発行日']).date(),
                    'planned_amount': self._parse_amount(row['発行予定額']),
                    'offered_amount': self._parse_amount(row['応募額']),
                    'allocated_amount': self._parse_amount(row['募入決定額']),
                    'highest_yield': self._parse_percentage(row.get('募入最大利回格差')),  # 最大利回格差→highest_yield
                    'prorate_ratio': self._parse_percentage(row.get('案分比率')),
                    'average_yield': self._parse_percentage(row.get('募入平均利回格差')),  # 平均利回格差→average_yield
                    'row_index': idx + 2,  # pandasのDataFrame index + header(2行)
                    'xlrd_row_index': idx + 2  # xlrdの実際の行番号（0始まり + header 2行）
                }

                summaries.append(summary)

            except Exception as e:
                logger.error(f"❌ 行 {idx} のパースエラー: {e}")
                continue

        logger.info(f"✅ {len(summaries)} 件のサマリーを抽出")
        return summaries

    def extract_hyperlink_from_excel(self, excel_path: str, round_number: int, auction_date: Any) -> Optional[str]:
        """
        入札日からURLを生成

        Args:
            excel_path: Excelファイルパス（未使用だが互換性のため残す）
            round_number: 回号
            auction_date: 入札日（date型またはdatetime型）

        Returns:
            ハイパーリンクURL（絶対パス）
        """
        try:
            # auction_dateを文字列に変換
            if hasattr(auction_date, 'strftime'):
                date_str = auction_date.strftime('%Y%m%d')
            else:
                # 文字列の場合
                from datetime import datetime
                date_obj = datetime.strptime(str(auction_date), '%Y-%m-%d')
                date_str = date_obj.strftime('%Y%m%d')

            url = f"{self.BASE_URL}/jgbs/auction/calendar/nyusatsu/resul{date_str}a.htm"
            logger.debug(f"第{round_number}回: {url}")
            return url

        except Exception as e:
            logger.error(f"❌ URL生成エラー (第{round_number}回): {e}")
            return None

    def scrape_issued_bonds(self, url: str) -> List[Dict[str, Any]]:
        """
        追加発行銘柄ページをスクレイピング
        404エラー時はWARPアーカイブからも試行

        Args:
            url: 追加発行銘柄ページのURL

        Returns:
            発行銘柄のリスト
        """
        logger.info(f"🌐 追加発行銘柄ページを取得中: {url}")

        # まず財務省のURLを試す
        bonds = self._scrape_from_url(url)

        # 404エラーの場合、WARPアーカイブを試す
        if not bonds and '404' in str(getattr(self, '_last_error', '')):
            logger.info(f"📚 WARPアーカイブから取得を試みます")
            bonds = self._scrape_from_warp_archive(url)

        return bonds

    def _scrape_from_url(self, url: str) -> List[Dict[str, Any]]:
        """
        指定URLから銘柄データをスクレイピング

        Args:
            url: スクレイピング対象URL

        Returns:
            発行銘柄のリスト
        """
        try:
            time.sleep(2)  # サーバー負荷軽減

            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            # BeautifulSoupに元のバイトデータを渡し、from_encodingでShift-JISを指定
            soup = BeautifulSoup(response.content, 'html.parser', from_encoding='shift_jis')

            # テーブルを探す（データテーブルを特定）
            # WARPアーカイブのバナーテーブルを避けるため、border="1"を持つテーブルを探す
            table = soup.find('table', border="1")

            # border属性がない場合、中央に配置されたテーブルを探す
            if not table:
                center = soup.find('center')
                if center:
                    table = center.find('table')

            if not table:
                logger.warning(f"⚠️  テーブルが見つかりません: {url}")
                return []

            bonds = []
            rows = table.find_all('tr')[1:]  # ヘッダー行をスキップ

            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    bond_type = cells[0].get_text(strip=True)
                    issue_number_str = cells[1].get_text(strip=True)
                    amount_str = cells[2].get_text(strip=True)

                    # 回号を抽出
                    issue_number = int(issue_number_str) if issue_number_str.isdigit() else None

                    # 発行額を抽出（カンマ除去）
                    amount = float(amount_str.replace(',', '')) if amount_str.replace(',', '').replace('.', '').isdigit() else None

                    if issue_number and amount:
                        bonds.append({
                            'bond_type': bond_type,
                            'issue_number': issue_number,
                            'amount': amount
                        })

            if bonds:
                logger.info(f"✅ {len(bonds)} 銘柄を抽出")
            return bonds

        except Exception as e:
            self._last_error = str(e)
            logger.error(f"❌ スクレイピングエラー: {e}")
            return []

    def _scrape_from_warp_archive(self, original_url: str) -> List[Dict[str, Any]]:
        """
        国立国会図書館WARPアーカイブから銘柄データを取得

        Args:
            original_url: 元の財務省URL

        Returns:
            発行銘柄のリスト
        """
        # 元のURLからパス部分を抽出
        # 例: https://www.mof.go.jp/jgbs/auction/calendar/nyusatsu/resul20190822a.htm
        # → www.mof.go.jp/jgbs/auction/calendar/nyusatsu/resul20190822a.htm
        if original_url.startswith('https://'):
            url_path = original_url.replace('https://', '')
        elif original_url.startswith('http://'):
            url_path = original_url.replace('http://', '')
        else:
            url_path = original_url

        # 複数のpidを試す
        for pid in self.WARP_PIDS:
            warp_url = f"https://warp.da.ndl.go.jp/info:ndljp/pid/{pid}/{url_path}"
            logger.info(f"  🔍 WARP (pid={pid}): {warp_url}")

            bonds = self._scrape_from_url(warp_url)
            if bonds:
                logger.info(f"  ✅ WARPアーカイブから取得成功 (pid={pid})")
                return bonds

        logger.warning(f"  ⚠️  全てのWARPアーカイブで取得失敗")
        return []

    def generate_bond_code(self, issue_number: int, bond_type: str) -> Optional[str]:
        """
        銘柄コードを生成

        Args:
            issue_number: 回号
            bond_type: 債券種類（例: "2年債", "5年債"）

        Returns:
            銘柄コード（9桁）
        """
        # 債券種類から種類コードを取得
        type_code_map = {
            '2年債': '0042',
            '4年債': '0061',
            '5年債': '0045',
            '6年債': '0061',
            '10年債': '0067',
            '20年債': '0069',
            '30年債': '0068',
            '40年債': '0054',
        }

        type_code = type_code_map.get(bond_type)
        if not type_code:
            logger.warning(f"⚠️  未対応の債券種類: {bond_type}")
            return None

        # 銘柄コード = 回号（5桁0パディング） + 種類コード（4桁）
        bond_code = f"{issue_number:05d}{type_code}"
        return bond_code

    def collect_all_data(self) -> List[Dict[str, Any]]:
        """
        全データを収集（Excel + Webスクレイピング）

        Returns:
            各銘柄の詳細データリスト
        """
        logger.info("=" * 70)
        logger.info("流動性供給入札データ収集開始")
        logger.info("=" * 70)

        # Step 1: Excelダウンロード
        excel_path = self.download_excel()

        # Step 2: サマリー抽出
        summaries = self.parse_excel_summary(excel_path)

        # Step 3: 各サマリーについて追加発行銘柄を取得
        all_bonds = []

        for summary in summaries:
            logger.info(f"\n📋 第{summary['round_number']}回 ({summary['auction_date']})")

            # URLを生成
            url = self.extract_hyperlink_from_excel(
                excel_path,
                summary['round_number'],
                summary['auction_date']
            )

            if not url:
                logger.warning(f"⚠️  URL生成失敗（第{summary['round_number']}回）")
                continue

            # 追加発行銘柄をスクレイピング
            issued_bonds = self.scrape_issued_bonds(url)

            # 各銘柄にサマリー情報を付加
            for bond in issued_bonds:
                bond_code = self.generate_bond_code(bond['issue_number'], bond['bond_type'])

                if not bond_code:
                    continue

                bond_data = {
                    # 主キー
                    'bond_code': bond_code,
                    'auction_date': summary['auction_date'],

                    # 基本情報
                    'issue_number': bond['issue_number'],
                    'bond_type_label': bond['bond_type'],

                    # 発行額（個別銘柄）
                    'allocated_amount': bond['amount'],

                    # 利回り（利回格差）
                    'highest_yield': summary['highest_yield'],  # 最大利回格差
                    'average_yield': summary['average_yield'],  # 平均利回格差

                    # サマリー情報
                    'liquidity_round': summary['round_number'],
                    'issue_date': summary['issue_date'],
                    'total_planned_amount': summary['planned_amount'],
                    'total_offered_amount': summary['offered_amount'],
                    'total_allocated_amount': summary['allocated_amount'],
                    'prorate_ratio': summary['prorate_ratio'],

                    # auction_type
                    'auction_type': 'tap'
                }

                all_bonds.append(bond_data)

        logger.info("\n" + "=" * 70)
        logger.info(f"✅ 収集完了: 合計 {len(all_bonds)} 銘柄")
        logger.info("=" * 70)

        return all_bonds

    def _parse_amount(self, value: Any) -> Optional[float]:
        """金額パース（億円単位）"""
        if pd.isna(value):
            return None

        try:
            # "6,000億円" → 6000.0
            amount_str = str(value).replace(',', '').replace('億円', '').replace('兆円', '0000').strip()
            return float(amount_str)
        except:
            return None

    def _parse_percentage(self, value: Any) -> Optional[float]:
        """パーセンテージパース"""
        if pd.isna(value):
            return None

        try:
            # "-0.015%" → -0.015
            pct_str = str(value).replace('%', '').strip()
            return float(pct_str)
        except:
            return None


if __name__ == "__main__":
    # テスト実行
    collector = LiquiditySupplyCollector()

    # 最新データのみテスト
    print("\n🧪 テスト: 最新3回分のデータを収集\n")

    excel_path = collector.download_excel()
    summaries = collector.parse_excel_summary(excel_path)

    # 最新1件のみテスト
    for summary in summaries[-1:]:
        print(f"\n第{summary['round_number']}回:")
        print(f"  入札日: {summary['auction_date']}")
        print(f"  発行額: {summary['allocated_amount']}億円")

        url = collector.extract_hyperlink_from_excel(
            excel_path,
            summary['round_number'],
            summary['auction_date']
        )
        if url:
            print(f"  URL: {url}")
            bonds = collector.scrape_issued_bonds(url)
            print(f"  発行銘柄: {len(bonds)}件")

            # 20年債第140回を探す
            bond_140 = None
            for bond in bonds:
                if '20年債' in bond['bond_type'] and bond['issue_number'] == 140:
                    bond_140 = bond
                    break

            if bond_140:
                bond_code = collector.generate_bond_code(bond_140['issue_number'], bond_140['bond_type'])
                print(f"\n  ✅ 20年債第140回を発見:")
                print(f"     発行額: {bond_140['amount']}億円")
                print(f"     bond_code: {bond_code}")
            else:
                print(f"\n  ⚠️ 20年債第140回が見つかりません")

            # 全銘柄を表示
            print("\n  全銘柄:")
            for i, bond in enumerate(bonds, 1):
                bond_code = collector.generate_bond_code(bond['issue_number'], bond['bond_type'])
                print(f"    {i:2d}. {bond['bond_type']:8s} 第{bond['issue_number']:3d}回 "
                      f"{bond['amount']:6.0f}億円 → {bond_code}")
