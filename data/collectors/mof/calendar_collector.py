#!/usr/bin/env python3
"""
MOF (財務省) 入札カレンダー収集モジュール

月次の入札カレンダーをスクレイピングして、入札予定日・種類を取得する。

URL パターン: https://www.mof.go.jp/jgbs/auction/calendar/YYMM.htm
例: 2024年11月 → 2411.htm
"""

import requests
from bs4 import BeautifulSoup
from datetime import date
from typing import List, Dict, Optional
import logging
import re

logger = logging.getLogger(__name__)


class AuctionCalendarCollector:
    """財務省入札カレンダー収集クラス"""

    BASE_URL = "https://www.mof.go.jp/jgbs/auction/calendar"

    # キャッシュ: {(year, month): [auctions]}
    _calendar_cache: Dict[tuple, List[Dict]] = {}

    # 入札種類のマッピング
    AUCTION_TYPE_MAP = {
        '2年利付国債': ('normal', '2年債'),
        '5年利付国債': ('normal', '5年債'),
        '10年利付国債': ('normal', '10年債'),
        '20年利付国債': ('normal', '20年債'),
        '30年利付国債': ('normal', '30年債'),
        '40年利付国債': ('normal', '40年債'),
        '物価連動国債': ('normal', '物価連動'),
        '流動性供給入札': ('tap', '流動性供給'),
        '国庫短期証券': ('tdb', 'TDB'),
        # GX経済移行債
        'GX経済移行債': ('gx', 'GX'),
        'クライメート・トランジション': ('gx', 'GX'),
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })

    def get_calendar_url(self, year: int, month: int) -> str:
        """カレンダーURLを生成"""
        yy = str(year)[-2:]
        mm = f"{month:02d}"
        return f"{self.BASE_URL}/{yy}{mm}.htm"

    def fetch_calendar(self, year: int, month: int, use_cache: bool = True) -> Optional[List[Dict]]:
        """
        指定月のカレンダーを取得

        Args:
            year: 年
            month: 月
            use_cache: キャッシュを使用するか（デフォルト: True）

        Returns:
            [
                {
                    'auction_date': date,
                    'auction_type': 'normal' | 'tap' | 'tdb',
                    'bond_type': '10年債' | '流動性供給' など,
                    'description': '10年利付国債（第XXX回）'
                },
                ...
            ]
        """
        cache_key = (year, month)

        # キャッシュ確認
        if use_cache and cache_key in self._calendar_cache:
            logger.debug(f"カレンダーキャッシュ使用: {year}/{month}")
            return self._calendar_cache[cache_key]

        url = self.get_calendar_url(year, month)

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'
        except requests.RequestException as e:
            logger.error(f"カレンダー取得エラー: {url} - {e}")
            return None

        result = self._parse_calendar(response.text, year, month)

        # キャッシュに保存
        if result is not None:
            self._calendar_cache[cache_key] = result
            logger.info(f"カレンダーキャッシュ保存: {year}/{month} ({len(result)}件)")

        return result

    def clear_cache(self):
        """キャッシュをクリア"""
        self._calendar_cache.clear()
        logger.info("カレンダーキャッシュをクリアしました")

    def _parse_calendar(self, html: str, year: int, month: int) -> List[Dict]:
        """HTMLをパースしてカレンダー情報を抽出"""
        soup = BeautifulSoup(html, 'html.parser')
        auctions = []

        # テーブルを探す（国債及び国庫短期証券のセクション）
        tables = soup.find_all('table')

        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                # 日付を探す（XX日 形式）
                first_cell = cells[0].get_text(strip=True)
                date_match = re.search(r'(\d{1,2})日', first_cell)
                if not date_match:
                    continue

                day = int(date_match.group(1))
                try:
                    auction_date = date(year, month, day)
                except ValueError:
                    continue

                # 入札種類を探す
                second_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""

                # 入札種類を判定
                auction_info = self._classify_auction(second_cell)
                if auction_info:
                    auctions.append({
                        'auction_date': auction_date,
                        'auction_type': auction_info['type'],
                        'bond_type': auction_info['bond_type'],
                        'description': second_cell
                    })

        return auctions

    def _classify_auction(self, description: str) -> Optional[Dict]:
        """入札種類を分類"""
        for keyword, (auction_type, bond_type) in self.AUCTION_TYPE_MAP.items():
            if keyword in description:
                return {
                    'type': auction_type,
                    'bond_type': bond_type
                }
        return None

    def get_auctions_for_date(self, target_date: date) -> List[Dict]:
        """
        指定日の入札一覧を取得

        Args:
            target_date: 対象日

        Returns:
            その日に予定されている入札のリスト
        """
        calendar = self.fetch_calendar(target_date.year, target_date.month)
        if not calendar:
            return []

        return [
            auction for auction in calendar
            if auction['auction_date'] == target_date
        ]

    def has_auction_today(self, target_date: date = None) -> bool:
        """今日入札があるかどうかを確認"""
        if target_date is None:
            target_date = date.today()

        auctions = self.get_auctions_for_date(target_date)
        return len(auctions) > 0

    def get_auction_types_for_date(self, target_date: date) -> List[str]:
        """
        指定日の入札種類一覧を取得

        Returns:
            ['normal', 'tap'] など
        """
        auctions = self.get_auctions_for_date(target_date)
        return list(set(a['auction_type'] for a in auctions))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    collector = AuctionCalendarCollector()

    # 現在の月のカレンダーを取得
    today = date.today()
    calendar = collector.fetch_calendar(today.year, today.month)

    if calendar:
        print(f"\n{today.year}年{today.month}月の入札予定:")
        print("-" * 60)
        for auction in calendar:
            print(f"{auction['auction_date']}: {auction['auction_type']:8} - {auction['description'][:40]}")

    # 今日の入札を確認
    print(f"\n今日({today})の入札:")
    today_auctions = collector.get_auctions_for_date(today)
    if today_auctions:
        for a in today_auctions:
            print(f"  - {a['auction_type']}: {a['description']}")
    else:
        print("  なし")
