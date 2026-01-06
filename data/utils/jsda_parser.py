#!/usr/bin/env python3
"""
JSDA Parser - JSDAサイトからデータ利用可能日付を取得
"""

import requests
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime, date
from typing import List, Set, Optional, Dict
import logging
import json
import os
from pathlib import Path

class JSDAParser:
    """JSDAサイトからデータ利用可能日付を解析するクラス"""
    
    # キャッシュファイルのパス（プロジェクトルートからの相対パス）
    CACHE_FILE_PATH = "data_files/jsda_available_dates.json"
    
    def __init__(self):
        self.base_url = "https://market.jsda.or.jp"
        self.index_url = "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/index.html"
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def _get_cache_path(self) -> Path:
        """キャッシュファイルの絶対パスを取得"""
        # このファイル(data/utils/jsda_parser.py)からプロジェクトルート(../../)へ
        current_dir = Path(__file__).resolve().parent
        project_root = current_dir.parent.parent
        return project_root / self.CACHE_FILE_PATH

    def _load_cache(self) -> Optional[List[date]]:
        """キャッシュファイルから日付リストを読み込む"""
        cache_path = self._get_cache_path()
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            dates_str = data.get('dates', [])
            # 文字列 -> dateオブジェクト
            dates = [date.fromisoformat(d) for d in dates_str]
            self.logger.info(f"📂 キャッシュ読み込み成功: {len(dates)}日分 (更新: {data.get('last_updated')})")
            return dates
        except Exception as e:
            self.logger.warning(f"キャッシュ読み込みエラー: {e}")
            return None

    def _save_cache(self, dates: List[date]):
        """日付リストをキャッシュファイルに保存"""
        cache_path = self._get_cache_path()
        try:
            # ディレクトリ作成
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 日付を文字列に変換して降順ソート
            sorted_dates = sorted(list(set(dates)), reverse=True)
            dates_str = [d.isoformat() for d in sorted_dates]
            
            data = {
                "last_updated": datetime.now().isoformat(),
                "dates": dates_str
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"💾 キャッシュ保存完了: {len(dates_str)}日分 -> {cache_path}")
        except Exception as e:
            self.logger.error(f"キャッシュ保存エラー: {e}")

    def get_available_dates_from_html(self, start_year: int = 2002, use_cache: bool = True, force_refresh: bool = False) -> List[date]:
        """
        JSDAから利用可能なデータ日付を取得
        メインページ + アーカイブページから日付を取得
        Args:
            start_year: アーカイブ取得開始年（デフォルト: 2002）
            use_cache: キャッシュを使用するかどうか（デフォルト: True）
            force_refresh: キャッシュを無視して全件取得するか（デフォルト: False）
        """
        try:
            # 1. キャッシュの使用を試みる
            if use_cache and not force_refresh:
                cached_dates = self._load_cache()
                if cached_dates:
                    # キャッシュがある場合、メインページだけ確認して差分更新（高速化）
                    self.logger.info("📄 メインページから最新日付のみ確認中...")
                    main_dates = self._parse_main_page()
                    
                    # 新しい日付があるか確認
                    new_dates_found = False
                    current_dates_set = set(cached_dates)
                    
                    for d in main_dates:
                        if d not in current_dates_set:
                            self.logger.info(f"🆕 新しい日付を発見: {d}")
                            current_dates_set.add(d)
                            new_dates_found = True
                    
                    # 更新があれば保存
                    if new_dates_found:
                        updated_dates = list(current_dates_set)
                        self._save_cache(updated_dates)
                        return sorted(updated_dates, reverse=True)
                    else:
                        self.logger.info("✅ キャッシュは最新です")
                        return cached_dates

            # 2. キャッシュがない、または強制更新の場合は全取得
            self.logger.info("📡 JSDAサイトから全期間のデータ日付を取得中...")
            
            available_dates = set()
            today = date.today()
            current_year = today.year
            
            # メインページから最新の日付を取得
            self.logger.info("📄 メインページから最新日付を取得...")
            main_dates = self._parse_main_page()
            available_dates.update(main_dates)
            
            # アーカイブページから各年のデータを取得
            real_start_year = max(start_year, 2002)
            
            for year in range(real_start_year, current_year + 1):
                self.logger.info(f"📄 {year}年アーカイブページを取得中...")
                archive_dates = self._parse_archive_page(year)
                available_dates.update(archive_dates)
                
                # JSDA接続制限回避のため待機
                if year < current_year and real_start_year < current_year:
                    self.logger.info(f"  ⏱️  30秒待機中...")
                    time.sleep(30)
            
            if len(available_dates) > 0:
                # 日付を降順でソート
                sorted_dates = sorted(list(available_dates), reverse=True)
                
                # 現在日より未来の日付を除外
                filtered_dates = [d for d in sorted_dates if d <= today]
                
                self.logger.info(f"✅ JSDA HTML解析成功: {len(filtered_dates)}日分")
                if filtered_dates:
                    self.logger.info(f"📅 最新: {filtered_dates[0]} ～ 最古: {filtered_dates[-1]}")
                    # キャッシュに保存
                    self._save_cache(filtered_dates)
                
                return filtered_dates
            
        except Exception as e:
            self.logger.error(f"JSDAサイト解析エラー: {e}")
            # エラー時でもキャッシュがあればそれを返す
            if use_cache:
                cached = self._load_cache()
                if cached:
                    self.logger.info("⚠️ エラー発生のため、既存のキャッシュを使用します")
                    return cached
        
        # HTMLからの取得に失敗した場合
        if start_year <= 2020:
             return self._get_dates_by_file_checking()
        else:
             return []
    
    def _get_dates_by_file_checking(self) -> List[date]:
        """
        JSDAのファイル構造に基づいて存在するデータ日付を確認
        2002年から現在まで系統的にチェック
        """
        self.logger.info("🔍 ファイル存在確認による日付取得を開始...")
        
        available_dates = set()
        today = date.today()
        
        # 年別にチェック（2002年から現在まで）
        for year in range(2002, today.year + 1):
            year_dates = 0
            
            # 月別チェック
            for month in range(1, 13):
                if year == today.year and month > today.month:
                    break
                
                # 各月の営業日をチェック
                from calendar import monthrange
                _, days_in_month = monthrange(year, month)
                
                for day in range(1, days_in_month + 1):
                    if year == today.year and month == today.month and day > today.day:
                        break
                    
                    check_date = date(year, month, day)
                    
                    # 営業日のみチェック（土日を除外）
                    if not self.is_business_day(check_date):
                        continue
                    
                    # CSVファイルの存在確認
                    if self._check_csv_file_exists(check_date):
                        available_dates.add(check_date)
                        year_dates += 1
                        
                        # JSDA保護のため20秒間隔でファイル存在確認
                        time.sleep(20)
                    
                    # 進行状況表示（100日ごと）
                    if len(available_dates) % 100 == 0 and len(available_dates) > 0:
                        self.logger.info(f"  📊 {len(available_dates)}日分確認済み...")
            
            if year_dates > 0:
                self.logger.info(f"  ✅ {year}年: {year_dates}日分")
            elif year >= 2010:  # 2010年以降でデータが0日は異常
                self.logger.warning(f"  ⚠️ {year}年: データなし")
        
        # 日付を降順でソート（今日から昔へ）
        sorted_dates = sorted(list(available_dates), reverse=True)
        
        self.logger.info(f"✅ ファイル存在確認完了: {len(sorted_dates)}日分")
        if sorted_dates:
            self.logger.info(f"📅 最新: {sorted_dates[0]} ～ 最古: {sorted_dates[-1]}")
        
        return sorted_dates
    
    def _check_csv_file_exists(self, check_date: date) -> bool:
        """
        特定日のCSVファイルが存在するかチェック
        2019年以前: /files/YYYY/MM/S{YYMMDD}.csv
        2020年以降: /files/YYYY/S{YYMMDD}.csv
        """
        year = check_date.year
        month = check_date.month
        date_str = check_date.strftime("%y%m%d")
        
        # 2019年以前は月別ディレクトリ構造
        if year <= 2019:
            url = f"https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files/{year}/{month:02d}/S{date_str}.csv"
        else:
            # 2020年以降は年別ディレクトリのみ
            url = f"https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files/{year}/S{date_str}.csv"
        
        try:
            response = requests.head(url, timeout=15)
            return response.status_code == 200
        except:
            return False
    
    def _generate_fallback_dates(self) -> List[date]:
        """
        JSDAサイトからの取得に失敗した場合のフォールバック
        直近の営業日を生成
        """
        self.logger.info("🔄 フォールバック: 直近営業日を生成中...")
        
        today = date.today()
        dates = []
        current = today
        
        # 直近3年分の営業日を生成
        for _ in range(800):  # 約3年分
            # 平日のみ追加（土日を除外）
            if current.weekday() < 5:
                dates.append(current)
            
            # 1日前に移動
            from datetime import timedelta
            current = current - timedelta(days=1)
            
            if current.year < 2015:  # 2015年より古いデータは対象外
                break
        
        self.logger.info(f"✅ フォールバック日付生成完了: {len(dates)}日分")
        return dates
    
    def is_business_day(self, check_date: date) -> bool:
        """
        営業日かどうかを簡易判定（土日を除外）
        """
        return check_date.weekday() < 5
    
    def _parse_main_page(self) -> Set[date]:
        """メインページから最新の日付を取得"""
        available_dates = set()
        
        try:
            response = requests.get(self.index_url, timeout=15)
            response.raise_for_status()
            
            if response.encoding is None or response.encoding == 'ISO-8859-1':
                response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                if href and ('S' in href and '.csv' in href):
                    match = re.search(r'S(\d{6})\.csv', href)
                    if match:
                        date_str = match.group(1)
                        try:
                            if len(date_str) == 6:
                                year = int('20' + date_str[:2])
                                month = int(date_str[2:4])
                                day = int(date_str[4:6])
                                parsed_date = date(year, month, day)
                                available_dates.add(parsed_date)
                        except ValueError:
                            continue
            
            self.logger.info(f"  メインページ: {len(available_dates)}日分")
            
        except Exception as e:
            self.logger.warning(f"メインページ解析エラー: {e}")
        
        return available_dates
    
    def _parse_archive_page(self, year: int) -> Set[date]:
        """指定年のアーカイブページから日付を取得（リトライ機構付き）"""
        available_dates = set()
        archive_url = f"{self.base_url}/shijyo/saiken/baibai/baisanchi/archive{year}.html"
        
        # リトライ設定
        max_retries = 3
        timeout_seconds = 45  # タイムアウトを45秒に延長
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"  📡 {year}年データ取得中... (試行 {attempt + 1}/{max_retries})")
                
                response = requests.get(archive_url, timeout=timeout_seconds)
                if response.status_code == 404:
                    self.logger.info(f"  {year}年: アーカイブページがまだありません(404)")
                    return set()
                response.raise_for_status()
                
                if response.encoding is None or response.encoding == 'ISO-8859-1':
                    response.encoding = 'utf-8'
                
                soup = BeautifulSoup(response.text, 'html.parser')
                break  # 成功したらループ終了
                
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 30  # 30秒、60秒、90秒待機
                    self.logger.warning(f"  ⚠️  {year}年: 接続エラー (試行 {attempt + 1}), {wait_time}秒後にリトライ...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.warning(f"  ❌ {year}年: 最大リトライ数到達、スキップ")
                    return available_dates
        
        try:
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                if href and ('S' in href and '.csv' in href):
                    match = re.search(r'S(\d{6})\.csv', href)
                    if match:
                        date_str = match.group(1)
                        try:
                            if len(date_str) == 6:
                                parsed_year = int('20' + date_str[:2])
                                month = int(date_str[2:4])
                                day = int(date_str[4:6])
                                
                                # 指定年と一致する場合のみ追加
                                if parsed_year == year:
                                    parsed_date = date(parsed_year, month, day)
                                    available_dates.add(parsed_date)
                        except ValueError:
                            continue
            
            self.logger.info(f"  {year}年: {len(available_dates)}日分")
            
        except Exception as e:
            if "404" in str(e) or "Not Found" in str(e):
                self.logger.info(f"  {year}年: アーカイブなし")
            else:
                self.logger.warning(f"  {year}年アーカイブ解析エラー: {e}")
        
        return available_dates
