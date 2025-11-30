"""
国債入札結果Web収集スクリプト

財務省の入札結果ページから最新の入札データを取得する。
"""

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, date
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BondAuctionWebCollector:
    """入札結果Web収集クラス"""

    BASE_URL = "https://www.mof.go.jp"

    # 債券種類コードマッピング
    BOND_TYPE_MAPPING = {
        '10年利付国債': '0067',
        '20年利付国債': '0069',
        '30年利付国債': '0068',
        '40年利付国債': '0054',
        '2年利付国債': '0042',
        '5年利付国債': '0045',
        '国庫短期証券': '0074',
        '国庫短期証券（3ヶ月）': '0074',
        '国庫短期証券（6ヶ月）': '0074',
        '国庫短期証券（1年）': '0074',
        '4年利付国債': '0044',
        '6年利付国債': '0061',
        'GX経済移行債（10年）': '0058',
        'GX経済移行債（5年）': '0057',
        '10年クライメート・トランジション利付国債': '0058',
        '5年クライメート・トランジション利付国債': '0057',
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        })

    def fetch_calendar_page(self, year: int, month: int) -> Optional[str]:
        """
        月別入札カレンダーページを取得

        Args:
            year: 年（西暦）
            month: 月

        Returns:
            HTMLテキスト
        """
        # 西暦の下2桁 + 月（01-12）
        year_code = year % 100  # 2024 -> 24
        ym_code = f"{year_code:02d}{month:02d}"

        url = f"{self.BASE_URL}/jgbs/auction/calendar/{ym_code}.htm"
        logger.info(f"カレンダーページ取得: {url}")

        try:
            response = self.session.get(url, timeout=30)
            response.encoding = response.apparent_encoding
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"カレンダーページ取得エラー: {e}")
            return None

    def extract_auction_schedule(self, html: str, target_date: date) -> List[Dict]:
        """
        カレンダーHTMLから特定日の入札予定を抽出

        Args:
            html: カレンダーページのHTML
            target_date: 対象日付

        Returns:
            入札予定のリスト
        """
        soup = BeautifulSoup(html, 'html.parser')
        schedules = []

        # テーブルを探す
        tables = soup.find_all('table')

        for table in tables:
            rows = table.find_all('tr')

            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 6:
                    continue

                # 入札日を確認
                date_cell = cells[0].get_text(strip=True)

                # 日付をパース（例: "10月3日（木）"）
                match = re.search(r'(\d+)月(\d+)日', date_cell)
                if not match:
                    continue

                month = int(match.group(1))
                day = int(match.group(2))

                if target_date.month != month or target_date.day != day:
                    continue

                # 債券名と回号を抽出
                bond_cell = cells[1].get_text(strip=True)

                # 回号を抽出（例: "10年利付国債(第376回)"）
                bond_match = re.search(r'(.+?)[（\(]第?(\d+)回[）\)]', bond_cell)
                if not bond_match:
                    logger.warning(f"回号が抽出できません: {bond_cell}")
                    continue

                bond_name = bond_match.group(1).strip()
                issue_number = int(bond_match.group(2))

                # 入札発行URL（offer）と入札結果URL（resul）を抽出
                offer_url = None
                result_links = []
                is_tb = '国庫短期証券' in bond_name
                is_gx = 'クライメート・トランジション' in bond_name or 'GX経済移行債' in bond_name

                for cell in cells[2:]:  # 入札発行についてのセルから開始
                    links = cell.find_all('a', href=True)
                    for link in links:
                        href = link['href']
                        # 絶対URLか相対URLか判定
                        if href.startswith('http'):
                            full_url = href
                        else:
                            full_url = self.BASE_URL + href

                        if 'offer' in href:
                            offer_url = full_url
                        elif 'resul' in href:
                            result_links.append(full_url)

                if result_links:
                    schedules.append({
                        'date': target_date,
                        'bond_name': bond_name,
                        'issue_number': issue_number,
                        'offer_url': offer_url,
                        'result_urls': result_links,
                        'is_tb': is_tb,  # TB判定フラグ
                        'is_gx': is_gx   # GX債判定フラグ
                    })
                    logger.info(f"入札予定検出: {bond_name} 第{issue_number}回")

        return schedules

    def fetch_auction_result(self, url: str, is_tb: bool = False, is_gx: bool = False) -> Optional[Dict]:
        """
        入札結果ページを取得して解析

        Args:
            url: 入札結果ページのURL
            is_tb: 国庫短期証券の場合True
            is_gx: GX債の場合True

        Returns:
            入札結果データ
        """
        logger.info(f"入札結果取得: {url}")

        try:
            response = self.session.get(url, timeout=30)
            response.encoding = response.apparent_encoding
            response.raise_for_status()

            return self.parse_auction_result(response.text, url, is_tb=is_tb, is_gx=is_gx)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.info(f"入札結果未公開（404）: {url}")
            else:
                logger.error(f"HTTPエラー: {e}")
            return None
        except Exception as e:
            logger.error(f"入札結果取得エラー: {e}")
            return None

    def parse_auction_result(self, html: str, url: str, is_tb: bool = False, is_gx: bool = False) -> Dict:
        """
        入札結果HTMLを解析

        Args:
            html: 入札結果ページのHTML
            url: URL（ログ用）

        Returns:
            解析済みデータ
        """
        result = {
            'source_url': url,
            'coupon_rate': None,
            'issue_date': None,
            'maturity_date': None,
            'planned_amount': None,
            'offered_amount': None,
            'allocated_amount': None,
            'lowest_price': None,
            'highest_yield': None,
            'average_price': None,
            'average_yield': None,
            'type1_noncompetitive': None,
            'type2_noncompetitive': None,
        }

        # 正規表現で全文から抽出

        # 表面利率 - "年0.9パーセント" (TBには存在しない)
        if not is_tb:
            match = re.search(r'年([\d.]+)パーセント', html)
            if match:
                result['coupon_rate'] = float(match.group(1))

        # 発行日 - "令和6年10月4日"
        match = re.search(r'>発行日</span>.*?>(令和\d+年\d+月\d+日)<', html, re.DOTALL)
        if match:
            result['issue_date'] = self._parse_japanese_date(match.group(1))

        # 償還期限 - "令和16年9月20日"
        match = re.search(r'>償還期限</span>.*?>(令和\d+年\d+月\d+日)<', html, re.DOTALL)
        if match:
            result['maturity_date'] = self._parse_japanese_date(match.group(1))

        # 応募額 - "6兆9,207億円" (価格競争入札の1番目) - INTEGER型
        # TBは全角括弧（）を使用、GX債は番号なし
        if is_gx:
            pattern = r'>応募額</span>.*?<span[^>]*>([\d兆億万円,]+)</span>'
        elif is_tb:
            pattern = r'[（\(]1[）\)]応募額</span>.*?<span[^>]*>([\d兆億万円,]+)</span>'
        else:
            pattern = r'\(1\)応募額</span>.*?<span[^>]*>(\d+兆[\d,]+億円|[\d,]+億円)</span>'
        match = re.search(pattern, html, re.DOTALL)
        if match:
            result['offered_amount'] = self._parse_amount(match.group(1), as_float=False)

        # 募入決定額 - "1兆9,612億円" (価格競争入札) - NUMERIC(10, 2)型
        if is_gx:
            pattern = r'>募入決定額</span>.*?<span[^>]*>([\d兆億万円,]+)</span>'
        elif is_tb:
            pattern = r'[（\(]2[）\)]募入決定額</span>.*?<span[^>]*>([\d兆億万円,]+)</span>'
        else:
            pattern = r'\(2\)募入決定額</span>.*?<span[^>]*>(\d+兆[\d,]+億円|[\d,]+億円)</span>'
        match = re.search(pattern, html, re.DOTALL)
        if match:
            result['allocated_amount'] = self._parse_amount(match.group(1), as_float=True)

        # 最低価格 - "100円24銭" または "99円99銭4厘5毛" (TB)
        # GX債は「発行価格」を average_price として使用
        if is_gx:
            # GX債は「額面金額100円につき100円51銭」形式
            pattern = r'>発行価格</span>.*?<span[^>]*>額面金額100円につき(\d+円[\d銭]+)</span>'
            match = re.search(pattern, html, re.DOTALL)
            if match:
                result['average_price'] = self._parse_price(match.group(1))
        else:
            pattern = r'[（\(]3[）\)]募入最低価格</span>.*?<span[^>]*>(\d+円[\d銭厘毛]+)</span>' if is_tb else r'\(3\)募入最低価格</span>.*?<span[^>]*>(\d+円\d+銭)</span>'
            match = re.search(pattern, html, re.DOTALL)
            if match:
                result['lowest_price'] = self._parse_price(match.group(1))

        # 最高利回 - "（0.873％）"
        # GX債はダッチ方式のため最高利回りは存在しない（スキップ）
        if not is_gx:
            if is_tb:
                pattern = r'募入最高利回り[）\)].*?<span[^>]*>[（\(](\d+\.\d+)％[）\)]</span>'
            else:
                pattern = r'募入最高利回り）</span>.*?<span[^>]*>（(\d+\.\d+)％）</span>'
            match = re.search(pattern, html, re.DOTALL)
            if match:
                result['highest_yield'] = float(match.group(1))

        # 平均価格 - "100円26銭" または "99円99銭8厘0毛" (TB)
        # GX債には平均価格なし（発行価格を使用）
        if not is_gx:
            pattern = r'[（\(]5[）\)]募入平均価格</span>.*?<span[^>]*>(\d+円[\d銭厘毛]+)</span>' if is_tb else r'\(5\)募入平均価格</span>.*?<span[^>]*>(\d+円\d+銭)</span>'
            match = re.search(pattern, html, re.DOTALL)
            if match:
                result['average_price'] = self._parse_price(match.group(1))

        # 平均利回 - "（0.871％）"
        # GX債は「応募者利回り」を使用
        if is_gx:
            pattern = r'>応募者利回り</span>.*?<span[^>]*>([\d.]+)％</span>'
        elif is_tb:
            pattern = r'募入平均利回り[）\)].*?<span[^>]*>[（\(](\d+\.\d+)％[）\)]</span>'
        else:
            pattern = r'募入平均利回り）</span>.*?（(\d+\.\d+)％）'
        match = re.search(pattern, html, re.DOTALL)
        if match:
            result['average_yield'] = float(match.group(1))

        # 発行予定額 - "額面金額で2兆6,000億円程度" (offerページのみ) - INTEGER型
        if 'offer' in url:
            match = re.search(r'額面金額で(\d+兆)?[\d,]+億円', html)
            if match:
                result['planned_amount'] = self._parse_amount(match.group(0), as_float=False)

        # 第Ⅰ非価格競争入札の募入決定額 - "6,355億円" - NUMERIC(10, 2)型
        # 価格競争入札ページ（resulページ、aなし）にある
        if 'resul' in url and 'a.htm' not in url:
            # 第Ⅰ非価格競争入札セクション内の<span>タグ内の億円が募入決定額
            match = re.search(r'第Ⅰ非価格競争入札について.*?</table>', html, re.DOTALL)
            if match:
                section = match.group(0)
                # <span>タグ内の億円を抽出（これが募入決定額）
                amounts = re.findall(r'<span[^>]*>([\d,]+億円)</span>', section)
                if len(amounts) >= 1:
                    # 最初のものが募入決定額
                    result['type1_noncompetitive'] = self._parse_amount(amounts[0], as_float=True)

        # 第Ⅱ非価格競争入札の募入決定額 - "2,114億円" - NUMERIC(10, 2)型
        # 第Ⅱ非価格競争入札ページ（resulXXXXXXXXa.htm）にある
        if 'resul' in url and 'a.htm' in url:
            match = re.search(r'第Ⅱ非価格競争入札.*?<span[^>]*>([\d,]+億円)</span>', html, re.DOTALL)
            if match:
                result['type2_noncompetitive'] = self._parse_amount(match.group(1), as_float=True)

        # total_amount計算（NULL値は0として扱う）
        result['total_amount'] = (
            (result.get('allocated_amount') or 0) +
            (result.get('type1_noncompetitive') or 0) +
            (result.get('type2_noncompetitive') or 0)
        )

        return result

    def _parse_japanese_date(self, date_str: str) -> Optional[date]:
        """和暦を西暦に変換"""
        # 令和X年Y月Z日 形式
        match = re.search(r'令和(\d+)年(\d+)月(\d+)日', date_str)
        if match:
            reiwa_year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            year = 2018 + reiwa_year
            return date(year, month, day)
        return None

    def _parse_amount(self, amount_str: str, as_float: bool = False):
        """
        金額文字列を億円単位の数値に変換

        Args:
            amount_str: 金額文字列
                例: "6兆9,207億円" → 69207
                例: "3兆5,141億6,000万円" → 35141.6 (TBの場合)
            as_float: Trueの場合floatで返す、Falseの場合intで返す

        Returns:
            整数または小数（億円単位）
        """
        amount_str = amount_str.replace(',', '')

        # 兆・億・万の組み合わせ（TB用）: "3兆5141億6000万円"
        if '万' in amount_str:
            cho = 0
            oku = 0
            man = 0

            # 兆を抽出
            cho_match = re.search(r'(\d+)兆', amount_str)
            if cho_match:
                cho = int(cho_match.group(1))

            # 億を抽出
            oku_match = re.search(r'(\d+)億', amount_str)
            if oku_match:
                oku = int(oku_match.group(1))

            # 万を抽出
            man_match = re.search(r'(\d+)万', amount_str)
            if man_match:
                man = int(man_match.group(1))

            # 億円単位に変換: 1兆 = 10000億、1万 = 0.0001億
            value = cho * 10000 + oku + man / 10000
            return float(value) if as_float or man > 0 else int(value)

        # 兆円を含む場合
        if '兆' in amount_str:
            match = re.search(r'(\d+)兆(\d+)?億', amount_str)
            if match:
                cho = int(match.group(1))
                oku = int(match.group(2)) if match.group(2) else 0
                value = cho * 10000 + oku
                return float(value) if as_float else value

        # 億円のみ
        match = re.search(r'(\d+)億', amount_str)
        if match:
            value = int(match.group(1))
            return float(value) if as_float else value

        return None

    def _parse_price(self, price_str: str) -> Optional[float]:
        """
        価格文字列を数値に変換

        Args:
            price_str: 価格文字列
                例: "100円24銭" → 100.24
                例: "99円99銭4厘5毛" → 99.9945 (TB用)

        Returns:
            価格（float）
        """
        # TB形式: "99円99銭4厘5毛" (円・銭・厘・毛)
        match = re.search(r'(\d+)円(\d+)銭(\d+)厘(\d+)毛', price_str)
        if match:
            yen = int(match.group(1))
            sen = int(match.group(2))
            rin = int(match.group(3))
            mo = int(match.group(4))
            # 1銭 = 0.01円、1厘 = 0.001円、1毛 = 0.0001円
            return round(yen + sen / 100 + rin / 1000 + mo / 10000, 4)

        # 標準形式: "100円24銭"
        match = re.search(r'(\d+)円(\d+)銭', price_str)
        if match:
            yen = int(match.group(1))
            sen = int(match.group(2))
            return round(yen + sen / 100, 2)

        return None

    def generate_bond_code(self, bond_name: str, issue_number: int) -> Optional[str]:
        """
        bond_codeを生成

        Args:
            bond_name: 債券名（例: "10年利付国債"）
            issue_number: 回号

        Returns:
            9桁のbond_code
        """
        type_code = self.BOND_TYPE_MAPPING.get(bond_name)
        if not type_code:
            logger.warning(f"未対応の債券種類: {bond_name}")
            return None

        # 回号を5桁に0パディング
        issue_part = f'{issue_number:05d}'

        return f'{issue_part}{type_code}'

    def collect_auction_data(self, auction_date: date) -> List[Dict]:
        """
        指定日の入札データを収集

        Args:
            auction_date: 入札日

        Returns:
            入札データのリスト
        """
        # カレンダーページを取得
        html = self.fetch_calendar_page(auction_date.year, auction_date.month)
        if not html:
            return []

        # 入札予定を抽出
        schedules = self.extract_auction_schedule(html, auction_date)
        if not schedules:
            logger.info(f"{auction_date}の入札予定なし")
            return []

        # 各入札の結果を取得
        all_results = []

        for schedule in schedules:
            bond_code = self.generate_bond_code(
                schedule['bond_name'],
                schedule['issue_number']
            )

            if not bond_code:
                continue

            # 複数の結果URLを統合
            combined_result = {
                'bond_code': bond_code,
                'auction_date': schedule['date'],
                'issue_number': schedule['issue_number'],
            }

            is_tb = schedule.get('is_tb', False)
            is_gx = schedule.get('is_gx', False)

            # offerページから発行予定額を取得
            if schedule.get('offer_url'):
                result = self.fetch_auction_result(schedule['offer_url'], is_tb=is_tb, is_gx=is_gx)
                if result:
                    for key, value in result.items():
                        if value is not None:
                            combined_result[key] = value
                    logger.info(f"URL {schedule['offer_url']} からデータ取得: {len([k for k, v in result.items() if v is not None])}件のフィールド")

            # 入札結果ページからデータを取得
            for url in schedule['result_urls']:
                result = self.fetch_auction_result(url, is_tb=is_tb, is_gx=is_gx)
                if result:
                    # Noneでない値のみをマージ（既存の値を上書きしないため）
                    for key, value in result.items():
                        if value is not None:
                            combined_result[key] = value
                    logger.info(f"URL {url} からデータ取得: {len([k for k, v in result.items() if v is not None])}件のフィールド")

            logger.info(f"統合結果: {len([k for k, v in combined_result.items() if v is not None])}件のフィールド")

            # データ完了チェック（TBは表面利率なし、それ以外は必須）
            if is_tb:
                # TBは発行日と償還日があれば OK
                if combined_result.get('issue_date') and combined_result.get('maturity_date'):
                    all_results.append(combined_result)
                    logger.info(f"入札データ収集完了: {bond_code}")
                else:
                    logger.warning(f"必須データが取得できませんでした: {bond_code}")
            else:
                # 利付債は表面利率が必須
                if 'coupon_rate' in combined_result and combined_result['coupon_rate'] is not None:
                    all_results.append(combined_result)
                    logger.info(f"入札データ収集完了: {bond_code}")
                else:
                    logger.warning(f"表面利率が取得できませんでした: {bond_code}")

        return all_results


if __name__ == '__main__':
    # テスト: 2024年10月3日の10年債入札結果を取得
    collector = BondAuctionWebCollector()

    test_date = date(2024, 10, 3)
    results = collector.collect_auction_data(test_date)

    print(f'\n=== {test_date} の入札結果 ===\n')
    for result in results:
        print(f'銘柄コード: {result["bond_code"]}')
        print(f'回号: {result["issue_number"]}')
        print(f'表面利率: {result.get("coupon_rate")}%')
        print(f'発行日: {result.get("issue_date")}')
        print(f'償還日: {result.get("maturity_date")}')
        print(f'発行予定額: {result.get("planned_amount")}億円')
        print(f'応募額: {result.get("offered_amount")}億円')
        print(f'落札額: {result.get("allocated_amount")}億円')
        print(f'最低価格: {result.get("lowest_price")}円')
        print(f'最高利回: {result.get("highest_yield")}%')
        print(f'平均価格: {result.get("average_price")}円')
        print(f'平均利回: {result.get("average_yield")}%')
        print(f'第Ⅰ非価格競争: {result.get("type1_noncompetitive")}億円')
        print(f'第Ⅱ非価格競争: {result.get("type2_noncompetitive")}億円')
        print(f'\n取得フィールド数: {len([v for v in result.values() if v is not None])}/{len(result)}')
        print()
