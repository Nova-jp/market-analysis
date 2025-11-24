#!/usr/bin/env python3
"""
BOJ Holdings Collector - 日銀保有国債銘柄別残高データ収集クラス
データソース: https://www.boj.or.jp/statistics/boj/other/mei/index.htm
"""

import pandas as pd
import requests
import re
import io
import time
import zipfile
import tempfile
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Tuple, Any
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

load_dotenv()


class BOJHoldingsCollector:
    """日銀保有国債銘柄別残高データ収集クラス"""

    BASE_URL = "https://www.boj.or.jp"
    INDEX_URL_TEMPLATE = "https://www.boj.or.jp/statistics/boj/other/mei/release/{year}/index.htm"

    # 国債種別の正規化マッピング
    BOND_TYPE_MAPPING = {
        '2年債': '2年債',
        '2-Year JGB': '2年債',
        '5年債': '5年債',
        '5-Year JGB': '5年債',
        '10年債': '10年債',
        '10-Year JGB': '10年債',
        '20年債': '20年債',
        '20-Year JGB': '20年債',
        '30年債': '30年債',
        '30-Year JGB': '30年債',
        '40年債': '40年債',
        '40-Year JGB': '40年債',
        '4年債': '4年債',
        '6年債': '6年債',
        '物価連動債': '物価連動債',
        'Inflation-indexed JGB': '物価連動債',
        '5年クライメート・トランジション国債': '5年CT債',
        '5-Year Japan Climate Transition Bond': '5年CT債',
        '10年クライメート・トランジション国債': '10年CT債',
        '10-Year Japan Climate Transition Bond': '10年CT債',
        '変動利付債': '変動利付債',
        'Floating-rate JGB': '変動利付債',
    }

    # 国債種別 → 銘柄コード末尾3桁のマッピング
    BOND_TYPE_CODE_SUFFIX = {
        '2年債': '042',
        '5年債': '045',
        '10年債': '067',
        '20年債': '069',
        '30年債': '068',
        '40年債': '054',
        '4年債': '044',
        '6年債': '061',
        '物価連動債': '046',
        '変動利付債': '063',
        '5年CT債': '055',
        '10年CT債': '056',
    }

    def __init__(self, delay_seconds: float = 5.0):
        """
        Args:
            delay_seconds: リクエスト間隔（秒）。日銀サーバー保護のため5秒以上を推奨
        """
        self.delay_seconds = delay_seconds
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json'
        }

        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def generate_bond_code(self, bond_type: str, issue_number: int) -> Optional[str]:
        """
        国債種別と回号から9桁の銘柄コードを生成

        Args:
            bond_type: 国債種別（例: "2年債", "10年債"）
            issue_number: 回号（例: 444）

        Returns:
            9桁の銘柄コード（例: "004440042"）、生成できない場合はNone
        """
        suffix = self.BOND_TYPE_CODE_SUFFIX.get(bond_type)
        if suffix is None:
            self.logger.warning(f"未知の国債種別: {bond_type}")
            return None

        # 回号を5桁でゼロパディング + "0" + 種別コード3桁
        # 例: 444回 2年債 → 00444 + 0 + 042 → 004440042
        bond_code = f"{issue_number:05d}0{suffix}"
        return bond_code

    def get_file_links_for_year(self, year: int) -> List[Dict[str, str]]:
        """
        指定年の一覧ページからファイルリンクを取得

        Returns:
            List[Dict]: {'url': str, 'date_str': str, 'format': str}
        """
        url = self.INDEX_URL_TEMPLATE.format(year=year)
        self.logger.info(f"一覧ページ取得中: {url}")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            links = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                # mei[YYMMDD].xlsx, .zip, .pdf, .htm パターンを検出
                match = re.search(r'mei(\d+)\.(xlsx|zip|pdf|htm)', href)
                if match:
                    file_url = href if href.startswith('http') else self.BASE_URL + href
                    date_code = match.group(1)
                    file_format = match.group(2)

                    # 日付文字列を抽出（リンクテキストから）
                    link_text = a_tag.get_text(strip=True)
                    date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', link_text)
                    if date_match:
                        y, m, d = date_match.groups()
                        data_date = f"{y}-{int(m):02d}-{int(d):02d}"
                    else:
                        # date_codeから日付を推測
                        data_date = self._parse_date_code(date_code, year)

                    links.append({
                        'url': file_url,
                        'date_str': data_date,
                        'format': file_format,
                        'date_code': date_code
                    })

            self.logger.info(f"{year}年: {len(links)}件のファイルを検出")
            return links

        except Exception as e:
            self.logger.error(f"一覧ページ取得エラー ({year}年): {e}")
            return []

    def _parse_date_code(self, date_code: str, year: int) -> str:
        """date_code (例: 241230, 1312, 0112) から日付文字列を生成"""
        if len(date_code) == 6:
            # YYMMDD形式
            yy = int(date_code[:2])
            mm = int(date_code[2:4])
            dd = int(date_code[4:6])
            full_year = 2000 + yy if yy < 50 else 1900 + yy
            return f"{full_year}-{mm:02d}-{dd:02d}"
        elif len(date_code) == 4:
            # YYMM形式（古い形式）
            yy = int(date_code[:2])
            mm = int(date_code[2:4])
            full_year = 2000 + yy if yy < 50 else 1900 + yy
            # 月末日を仮定
            return f"{full_year}-{mm:02d}-01"
        else:
            return f"{year}-01-01"

    def download_and_parse(self, file_info: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        ファイルをダウンロードしてパース

        Returns:
            List[Dict]: パースされたデータリスト
        """
        url = file_info['url']
        file_format = file_info['format']
        data_date = file_info['date_str']

        self.logger.info(f"ダウンロード中: {url}")

        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            if file_format == 'xlsx':
                return self._parse_xlsx(response.content, data_date)
            elif file_format == 'zip':
                return self._parse_zip(response.content, data_date)
            elif file_format == 'htm':
                return self._parse_html(response.content, data_date)
            elif file_format == 'pdf':
                self.logger.warning(f"PDFパースは未実装: {url}")
                return []
            else:
                self.logger.warning(f"未対応フォーマット: {file_format}")
                return []

        except Exception as e:
            self.logger.error(f"ダウンロード/パースエラー: {e}")
            return []

    def _parse_xlsx(self, content: bytes, data_date: str) -> List[Dict[str, Any]]:
        """XLSXファイルをパース"""
        df = pd.read_excel(io.BytesIO(content), header=None)
        return self._extract_data_from_df(df, data_date)

    def _parse_zip(self, content: bytes, data_date: str) -> List[Dict[str, Any]]:
        """ZIPファイルを展開してパース"""
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, 'temp.zip')
            with open(zip_path, 'wb') as f:
                f.write(content)

            with zipfile.ZipFile(zip_path, 'r') as zf:
                for name in zf.namelist():
                    if name.endswith(('.xls', '.xlsx')):
                        with zf.open(name) as excel_file:
                            df = pd.read_excel(excel_file, header=None)
                            return self._extract_data_from_df(df, data_date)
        return []

    def _parse_html(self, content: bytes, data_date: str) -> List[Dict[str, Any]]:
        """HTMLファイルをパース"""
        soup = BeautifulSoup(content, 'html.parser')
        records = []

        # テーブルを探す
        tables = soup.find_all('table')

        for table in tables:
            # ヘッダー行から列ごとの国債種別を特定
            # HTMLテーブルは2列ペア（銘柄、保有残高）で複数種別が並ぶ
            # ヘッダー: [銘柄(2年債), 保有残高, 銘柄(4年債), 保有残高]
            # データ:   [cell0,       cell1,    cell2,       cell3   ]
            col_to_bond_type = {}
            headers = table.find_all('th')

            bond_type_count = 0
            for idx, header in enumerate(headers):
                header_text = header.get_text()
                match = re.search(r'銘柄（([^）]+)）', header_text)
                if match:
                    bond_type_key = match.group(1)
                    if bond_type_key in self.BOND_TYPE_MAPPING:
                        # ヘッダー位置から実際のデータ列位置を計算
                        # 銘柄ヘッダーは偶数インデックス(0, 2, 4...)に対応
                        col_to_bond_type[bond_type_count * 2] = self.BOND_TYPE_MAPPING[bond_type_key]
                        bond_type_count += 1

            # データ行を処理
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                # 2列ペアで処理
                for i in range(0, len(cells), 2):
                    if i + 1 < len(cells):
                        issue_cell = cells[i].get_text(strip=True)
                        value_cell = cells[i + 1].get_text(strip=True)

                        # このペアの国債種別を特定
                        bond_type = col_to_bond_type.get(i)
                        if not bond_type:
                            continue

                        # 回号を抽出
                        issue_match = re.search(r'(\d+)回', issue_cell)
                        if issue_match and value_cell:
                            issue_number = int(issue_match.group(1))
                            try:
                                face_value = int(float(value_cell.replace(',', '')))
                                bond_code = self.generate_bond_code(bond_type, issue_number)
                                if bond_code:
                                    records.append({
                                        'data_date': data_date,
                                        'bond_code': bond_code,
                                        'bond_type': bond_type,
                                        'issue_number': issue_number,
                                        'face_value': face_value
                                    })
                            except ValueError:
                                pass

        self.logger.info(f"HTMLから{len(records)}件のレコードを抽出")
        return records

    def _extract_data_from_df(self, df: pd.DataFrame, data_date: str) -> List[Dict[str, Any]]:
        """DataFrameからデータを抽出（XLSX/XLS共通）"""
        records = []

        # まずヘッダー行を全て探して、セクションごとの列→国債種別マッピングを作成
        # 形式: {header_row_idx: {col_idx: bond_type}}
        sections = []

        for idx, row in df.iterrows():
            section_mapping = {}
            for col_idx, cell in enumerate(row):
                if pd.isna(cell):
                    continue
                cell_str = str(cell)

                # ヘッダー行の検出（「銘柄（2年債）」など）
                header_match = re.search(r'銘柄（([^）]+)）', cell_str)
                if header_match:
                    bond_type_key = header_match.group(1)
                    if bond_type_key in self.BOND_TYPE_MAPPING:
                        section_mapping[col_idx] = self.BOND_TYPE_MAPPING[bond_type_key]

            if section_mapping:
                sections.append({'header_row': idx, 'col_mapping': section_mapping})

        # 古い形式（セクションが見つかった場合）
        if sections:
            for i, section in enumerate(sections):
                header_row = section['header_row']
                col_mapping = section['col_mapping']

                # 次のセクションの開始行を特定
                if i + 1 < len(sections):
                    next_header_row = sections[i + 1]['header_row']
                else:
                    next_header_row = len(df)

                # このセクションのデータ行を処理
                for idx in range(header_row + 1, next_header_row):
                    row = df.iloc[idx]
                    for col_idx, bond_type in col_mapping.items():
                        if col_idx >= len(row):
                            continue
                        cell = row.iloc[col_idx]
                        if pd.isna(cell):
                            continue

                        cell_str = str(cell).strip()

                        # 回号を検出（例: " 312回債"）
                        issue_match = re.match(r'^\s*(\d+)回債?\s*$', cell_str)
                        if issue_match:
                            issue_number = int(issue_match.group(1))
                            # 次の列に保有残高
                            if col_idx + 1 < len(row):
                                value_cell = row.iloc[col_idx + 1]
                                if pd.notna(value_cell):
                                    try:
                                        face_value = int(float(value_cell))
                                        bond_code = self.generate_bond_code(bond_type, issue_number)
                                        if bond_code:
                                            records.append({
                                                'data_date': data_date,
                                                'bond_code': bond_code,
                                                'bond_type': bond_type,
                                                'issue_number': issue_number,
                                                'face_value': face_value
                                            })
                                    except (ValueError, TypeError):
                                        pass
        else:
            # 新しい形式（2016年以降）: 列2に国債種別、列3に回号、列4に保有残高
            current_bond_type = None
            for idx, row in df.iterrows():
                for col_idx, cell in enumerate(row):
                    if pd.isna(cell):
                        continue
                    cell_str = str(cell)

                    # 国債種別を検出（例: "2年債", "10年債"）
                    for key, value in self.BOND_TYPE_MAPPING.items():
                        if key == cell_str.strip() or (key in cell_str and '銘柄' not in cell_str and 'Issue' not in cell_str):
                            current_bond_type = value
                            break

                    # 回号を検出（数字のみ、または "XXX回債"）
                    issue_match = re.match(r'^\s*(\d+)\s*$', cell_str)
                    if not issue_match:
                        issue_match = re.match(r'^\s*(\d+)回債?\s*$', cell_str)

                    if issue_match and current_bond_type:
                        issue_number = int(issue_match.group(1))
                        # 次の列に保有残高
                        if col_idx + 1 < len(row):
                            value_cell = row.iloc[col_idx + 1]
                            if pd.notna(value_cell):
                                try:
                                    face_value = int(float(value_cell))
                                    bond_code = self.generate_bond_code(current_bond_type, issue_number)
                                    if bond_code:
                                        records.append({
                                            'data_date': data_date,
                                            'bond_code': bond_code,
                                            'bond_type': current_bond_type,
                                            'issue_number': issue_number,
                                            'face_value': face_value
                                        })
                                except (ValueError, TypeError):
                                    pass

        self.logger.info(f"DataFrameから{len(records)}件のレコードを抽出")
        return records

    def save_to_database(self, records: List[Dict[str, Any]], batch_size: int = 100) -> int:
        """データベースに保存"""
        if not records:
            return 0

        success_count = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            try:
                response = requests.post(
                    f'{self.supabase_url}/rest/v1/boj_holdings',
                    headers=self.headers,
                    json=batch
                )

                if response.status_code in [200, 201]:
                    success_count += len(batch)
                elif "duplicate key" in response.text:
                    # 重複は正常扱い
                    success_count += len(batch)
                else:
                    self.logger.warning(f"保存失敗: {response.status_code} - {response.text[:200]}")

            except Exception as e:
                self.logger.error(f"保存エラー: {e}")

        return success_count

    def collect_year(self, year: int, save_to_db: bool = True) -> Tuple[int, int]:
        """
        指定年のデータを収集

        Returns:
            Tuple[int, int]: (ファイル数, 保存レコード数)
        """
        file_links = self.get_file_links_for_year(year)
        if not file_links:
            return 0, 0

        total_records = 0
        for i, file_info in enumerate(file_links):
            self.logger.info(f"処理中 ({i+1}/{len(file_links)}): {file_info['date_str']}")

            records = self.download_and_parse(file_info)
            if records and save_to_db:
                saved = self.save_to_database(records)
                total_records += saved
                self.logger.info(f"  → {saved}件保存")
            elif records:
                total_records += len(records)

            # リクエスト間隔
            if i < len(file_links) - 1:
                time.sleep(self.delay_seconds)

        return len(file_links), total_records

    def collect_all(self, start_year: int = 2001, end_year: int = None,
                    save_to_db: bool = True) -> Dict[str, Any]:
        """
        全期間のデータを収集

        Args:
            start_year: 開始年
            end_year: 終了年（Noneの場合は現在の年）
            save_to_db: データベースに保存するか
        """
        if end_year is None:
            end_year = datetime.now().year

        results = {
            'start_year': start_year,
            'end_year': end_year,
            'total_files': 0,
            'total_records': 0,
            'by_year': {}
        }

        for year in range(start_year, end_year + 1):
            self.logger.info(f"=== {year}年 処理開始 ===")
            files, records = self.collect_year(year, save_to_db)
            results['by_year'][year] = {'files': files, 'records': records}
            results['total_files'] += files
            results['total_records'] += records

            # 年の間に少し待機
            time.sleep(self.delay_seconds)

        self.logger.info(f"=== 収集完了: {results['total_files']}ファイル, {results['total_records']}レコード ===")
        return results


if __name__ == '__main__':
    # テスト実行
    collector = BOJHoldingsCollector(delay_seconds=5.0)

    # 2024年の最新ファイルのみテスト
    file_links = collector.get_file_links_for_year(2024)
    if file_links:
        # 最新1件のみテスト
        latest = file_links[0]
        print(f"テスト対象: {latest}")
        records = collector.download_and_parse(latest)
        print(f"抽出レコード数: {len(records)}")
        if records:
            print(f"サンプル: {records[:3]}")
