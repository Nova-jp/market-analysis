#!/usr/bin/env python3
"""
Database Manager - Supabaseデータベース操作の統一クラス
API制限を回避して全データを取得する機能も含む
"""

import requests
import os
import json
from dotenv import load_dotenv
from typing import List, Set, Dict, Any, Optional
import logging

load_dotenv()

class DatabaseManager:
    """Supabaseデータベース操作を統一するクラス"""
    
    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json'
        }
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def get_total_record_count(self, table_name: str = 'clean_bond_data') -> int:
        """テーブルの総レコード数を取得（API制限回避）"""
        headers = self.headers.copy()
        headers['Prefer'] = 'count=exact'
        
        try:
            response = requests.head(
                f'{self.supabase_url}/rest/v1/{table_name}',
                headers=headers
            )
            
            if 'content-range' in response.headers:
                content_range = response.headers['content-range']
                parts = content_range.split('/')
                if len(parts) == 2 and parts[1] != '*':
                    return int(parts[1])
            
            return 0
        except Exception as e:
            self.logger.error(f"総レコード数取得エラー: {e}")
            return 0
    
    def get_all_existing_dates(self, table_name: str = 'clean_bond_data') -> Set[str]:
        """
        API制限を回避してすべての既存日付を取得
        """
        existing_dates = set()
        offset = 0
        limit = 1000  # ページサイズ
        
        try:
            while True:
                response = requests.get(
                    f'{self.supabase_url}/rest/v1/{table_name}',
                    params={
                        'select': 'trade_date',
                        'order': 'trade_date.desc',
                        'offset': offset,
                        'limit': limit
                    },
                    headers=self.headers
                )
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                if not data:  # データなし = 終了
                    break
                
                batch_dates = {row['trade_date'] for row in data}
                existing_dates.update(batch_dates)
                
                # 最後のページの場合は終了
                if len(data) < limit:
                    break
                
                offset += limit
                
                # 進捗ログ（1000件ごと）
                if offset % 10000 == 0:
                    self.logger.info(f"既存データ確認中... {len(existing_dates)}日分取得済み")
            
            self.logger.info(f"✅ 既存データ確認完了: {len(existing_dates)}日分")
            return existing_dates
        
        except Exception as e:
            self.logger.error(f"既存データ取得エラー: {e}")
            return existing_dates
    
    def batch_insert_data(self, data_list: List[Dict[Any, Any]], 
                         table_name: str = 'clean_bond_data', 
                         batch_size: int = 100) -> int:
        """
        バッチでデータを挿入（効率化）
        """
        if not data_list:
            return 0
        
        success_count = 0
        total_count = len(data_list)
        
        for i in range(0, total_count, batch_size):
            batch_data = data_list[i:i+batch_size]
            
            # データ型変換とクリーニング
            cleaned_batch = []
            for record in batch_data:
                cleaned_record = {}
                for key, value in record.items():
                    # デバッグ出力（整数フィールドのみ）
                    if key in ['issue_type', 'reporting_members', 'interest_payment_day']:
                        self.logger.debug(f"変換前 {key}: {repr(value)} ({type(value).__name__})")
                    
                    # NaNとNoneのチェック
                    if value is None or str(value).lower() in ['nan', 'none']:
                        cleaned_record[key] = None
                    # 整数フィールドの確実な変換
                    elif key in ['issue_type', 'reporting_members', 'interest_payment_day']:
                        try:
                            # まずintかどうかチェック
                            if isinstance(value, int) and not isinstance(value, bool):
                                cleaned_record[key] = value
                            # 次にfloatからの変換
                            elif isinstance(value, float):
                                cleaned_record[key] = int(value)
                            # 文字列からの変換
                            elif isinstance(value, str):
                                if value.strip() == '':
                                    cleaned_record[key] = None
                                else:
                                    cleaned_record[key] = int(float(value))
                            else:
                                # その他の型は文字列経由で変換
                                cleaned_record[key] = int(float(str(value)))
                        except (ValueError, TypeError, AttributeError):
                            cleaned_record[key] = None
                        
                        # デバッグ出力
                        if key in ['issue_type', 'reporting_members', 'interest_payment_day']:
                            self.logger.debug(f"変換後 {key}: {repr(cleaned_record[key])} ({type(cleaned_record[key]).__name__})")
                    else:
                        cleaned_record[key] = value
                cleaned_batch.append(cleaned_record)
            
            try:
                # JSON送信前のレコード詳細ログ（最初の1件のみ）
                if len(cleaned_batch) > 0:
                    first_record = cleaned_batch[0]
                    self.logger.info(f"JSON送信前サンプル: issue_type={repr(first_record.get('issue_type'))}, reporting_members={repr(first_record.get('reporting_members'))}")
                
                response = requests.post(
                    f'{self.supabase_url}/rest/v1/{table_name}',
                    headers=self.headers,
                    json=cleaned_batch
                )
                
                if response.status_code in [200, 201]:
                    success_count += len(batch_data)
                else:
                    # 重複エラーは正常として扱う
                    if "duplicate key" in response.text:
                        success_count += len(batch_data)
                    else:
                        # 詳細エラー情報を出力
                        self.logger.warning(f"バッチ保存失敗: {response.status_code}")
                        self.logger.warning(f"レスポンス詳細: {response.text}")
                        if len(cleaned_batch) > 0:
                            self.logger.warning(f"送信データサンプル: {json.dumps(cleaned_batch[0], ensure_ascii=False)[:300]}")
                        return 0  # エラー発生時はここで中断
                        
            except Exception as e:
                self.logger.error(f"バッチ保存エラー: {e}")
                continue
        
        return success_count
    
    def get_date_range_info(self, table_name: str = 'clean_bond_data') -> Dict[str, Any]:
        """データベースの日付範囲情報を取得"""
        try:
            response = requests.get(
                f'{self.supabase_url}/rest/v1/{table_name}',
                params={
                    'select': 'trade_date',
                    'order': 'trade_date.desc',
                    'limit': 1
                },
                headers=self.headers
            )
            
            latest_date = None
            if response.status_code == 200 and response.json():
                latest_date = response.json()[0]['trade_date']
            
            response = requests.get(
                f'{self.supabase_url}/rest/v1/{table_name}',
                params={
                    'select': 'trade_date',
                    'order': 'trade_date.asc',
                    'limit': 1
                },
                headers=self.headers
            )
            
            earliest_date = None
            if response.status_code == 200 and response.json():
                earliest_date = response.json()[0]['trade_date']
            
            total_records = self.get_total_record_count(table_name)
            
            return {
                'total_records': total_records,
                'latest_date': latest_date,
                'earliest_date': earliest_date
            }
            
        except Exception as e:
            self.logger.error(f"日付範囲情報取得エラー: {e}")
            return {
                'total_records': 0,
                'latest_date': None,
                'earliest_date': None
            }
    
    def execute_query(self, sql_query: str, params: tuple = None) -> List[tuple]:
        """
        SQL風のクエリインターフェース（PostgRESTベース）
        従来のSQLライクな呼び出しをサポート
        
        Args:
            sql_query (str): SQL風クエリ文字列
            params (tuple): パラメータ（現在未使用）
            
        Returns:
            List[tuple]: 結果をタプル形式で返す（従来との互換性）
        """
        try:
            # clean_bond_dataテーブルから直接クエリする場合の処理
            if "FROM clean_bond_data" in sql_query.upper() or "from clean_bond_data" in sql_query:
                results = self._execute_simple_query(sql_query)
                # Dict形式の結果をtuple形式に変換
                if results and isinstance(results[0], dict):
                    # 最初の結果からキーを取得してタプルに変換
                    return [tuple(row.values()) for row in results]
                return results
            else:
                # その他のクエリは基本的な情報取得として処理
                return []
                
        except Exception as e:
            self.logger.error(f"クエリ実行エラー: {e}")
            return []
    
    def _execute_simple_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """
        簡単なSQLクエリをPostgRESTで実行する代替実装
        
        Args:
            sql_query (str): SQLクエリ
            
        Returns:
            List[Dict[str, Any]]: 結果データ
        """
        try:
            # GROUP BY issue_type のクエリの場合
            if "GROUP BY issue_type" in sql_query.upper():
                return self._get_issue_type_stats(sql_query)
            
            # 特定日付のクエリの場合
            elif "WHERE trade_date" in sql_query:
                return self._get_date_specific_data(sql_query)
            
            # 日付一覧のクエリの場合
            elif "GROUP BY trade_date" in sql_query.upper():
                return self._get_date_summary()
            
            # COUNT(*)クエリの場合
            elif "COUNT(*)" in sql_query.upper():
                count = self.get_total_record_count()
                return [{"count": count}]
            
            else:
                # デフォルト: 最初の10件を取得
                response = requests.get(
                    f'{self.supabase_url}/rest/v1/clean_bond_data?limit=10',
                    headers=self.headers
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    return []
                    
        except Exception as e:
            self.logger.error(f"簡易クエリ実行エラー: {e}")
            return []
    
    def _get_issue_type_stats(self, sql_query: str) -> List[Dict[str, Any]]:
        """issue_type別の統計を取得"""
        try:
            # trade_dateの条件を抽出
            date_condition = ""
            if "WHERE trade_date" in sql_query:
                import re
                match = re.search(r"trade_date = '([^']+)'", sql_query)
                if match:
                    date_condition = f"&trade_date=eq.{match.group(1)}"
            
            results = []
            for issue_type in [1, 2]:
                response = requests.get(
                    f'{self.supabase_url}/rest/v1/clean_bond_data?select=bond_name&issue_type=eq.{issue_type}{date_condition}',
                    headers=self.headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    count = len(data)
                    sample_bond_name = data[0]['bond_name'] if data else None
                    
                    results.append({
                        'issue_type': issue_type,
                        'count': count,
                        'sample_bond_name': sample_bond_name
                    })
            
            return results
            
        except Exception as e:
            self.logger.error(f"issue_type統計取得エラー: {e}")
            return []
    
    def _get_date_specific_data(self, sql_query: str) -> List[Dict[str, Any]]:
        """特定日付のデータを取得"""
        try:
            import re
            
            # 日付とissue_typeの条件を抽出
            date_match = re.search(r"trade_date = '([^']+)'", sql_query)
            issue_type_match = re.search(r"issue_type = (\d+)", sql_query)
            
            if not date_match:
                return []
            
            date_str = date_match.group(1)
            conditions = f"trade_date=eq.{date_str}"
            
            if issue_type_match:
                issue_type = issue_type_match.group(1)
                conditions += f"&issue_type=eq.{issue_type}"
            
            # SELECT句から必要な列を抽出
            select_columns = "bond_name,due_date,ave_price,ave_simple_yield,reporting_members,coupon_rate,ave_compound_yield"
            if "coupon_rate" in sql_query:
                select_columns = "bond_name,due_date,coupon_rate,ave_compound_yield,ave_price,reporting_members"
            
            # LIMIT句を確認
            limit = "5"  # デフォルト
            limit_match = re.search(r"LIMIT (\d+)", sql_query.upper())
            if limit_match:
                limit = limit_match.group(1)
            
            response = requests.get(
                f'{self.supabase_url}/rest/v1/clean_bond_data?select={select_columns}&{conditions}&order=bond_name&limit={limit}',
                headers=self.headers
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"日付特定データ取得エラー: {e}")
            return []
    
    def _get_date_summary(self) -> List[Dict[str, Any]]:
        """日付別サマリーを取得"""
        try:
            # 既存の日付取得メソッドを活用
            existing_dates = self.get_all_existing_dates()
            
            results = []
            for date_str in sorted(existing_dates, reverse=True)[:20]:  # 最新20件
                # 各日付のレコード数を取得
                response = requests.get(
                    f'{self.supabase_url}/rest/v1/clean_bond_data?select=issue_type&trade_date=eq.{date_str}',
                    headers=self.headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    record_count = len(data)
                    
                    issue_types = [item['issue_type'] for item in data if item.get('issue_type')]
                    min_issue_type = min(issue_types) if issue_types else None
                    max_issue_type = max(issue_types) if issue_types else None
                    
                    results.append({
                        'trade_date': date_str,
                        'record_count': record_count,
                        'min_issue_type': min_issue_type,
                        'max_issue_type': max_issue_type
                    })
            
            return results
            
        except Exception as e:
            self.logger.error(f"日付サマリー取得エラー: {e}")
            return []
    
    def get_yield_curve_data(self, trade_date: str = None) -> List[Dict[str, Any]]:
        """
        イールドカーブ作成用のデータを取得
        
        Args:
            trade_date (str): 対象日付 (YYYY-MM-DD)。Noneの場合は最新日付
            
        Returns:
            List[Dict[str, Any]]: イールドカーブデータ
        """
        try:
            # 最新日付を取得（trade_dateが指定されていない場合）
            if trade_date is None:
                date_info = self.get_date_range_info()
                trade_date = date_info.get('latest_date')
                
                if not trade_date:
                    self.logger.error("最新日付の取得に失敗しました")
                    return []
            
            # 指定日付のイールドカーブデータを取得
            response = requests.get(
                f'{self.supabase_url}/rest/v1/clean_bond_data',
                params={
                    'select': 'trade_date,due_date,ave_compound_yield,bond_name,interest_payment_date',
                    'trade_date': f'eq.{trade_date}',
                    'ave_compound_yield': 'not.is.null',
                    'due_date': 'not.is.null',
                    'order': 'due_date.asc'
                },
                headers=self.headers
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # フィルタリング（異常値除去）
                filtered_data = []
                for row in data:
                    yield_rate = row.get('ave_compound_yield')
                    if yield_rate is not None and 0 < yield_rate < 10:  # 0-10%の範囲
                        filtered_data.append(row)
                
                self.logger.info(f"✅ イールドカーブデータ取得: {len(filtered_data)}件 ({trade_date})")
                return filtered_data
            else:
                self.logger.error(f"データ取得失敗: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"イールドカーブデータ取得エラー: {e}")
            return []
    
    def get_available_dates(self, limit: int = 10) -> List[str]:
        """
        利用可能な日付一覧を取得
        
        Args:
            limit (int): 取得する日付数
            
        Returns:
            List[str]: 日付リスト (降順)
        """
        try:
            response = requests.get(
                f'{self.supabase_url}/rest/v1/clean_bond_data',
                params={
                    'select': 'trade_date',
                    'order': 'trade_date.desc',
                    'limit': limit
                },
                headers=self.headers
            )
            
            if response.status_code == 200:
                data = response.json()
                # ユニークな日付を取得
                unique_dates = list(dict.fromkeys([row['trade_date'] for row in data]))
                self.logger.info(f"✅ 利用可能日付取得: {len(unique_dates)}日分")
                return unique_dates
            else:
                self.logger.error(f"日付取得失敗: {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.error(f"日付取得エラー: {e}")
            return []
    
    def fetch_data(self, query: str, params: tuple = None) -> List[tuple]:
        """
        SQL風のクエリをSupabase REST APIに変換してデータを取得
        主成分分析用のデータ取得メソッド
        
        Args:
            query (str): SQL風のクエリ文字列（簡易的な解析）
            params (tuple): パラメータ（start_date, end_date）
        
        Returns:
            List[tuple]: 取得されたデータのタプルリスト
        """
        try:
            if params and len(params) >= 2:
                start_date, end_date = params[0], params[1]
                
                # Supabase REST APIでデータ取得
                url = f'{self.supabase_url}/rest/v1/clean_bond_data'
                
                # URLパラメータを手動で構築
                query_params = [
                    f'select=trade_date,maturity_years,ave_compound_yield,bond_name',
                    f'trade_date=gte.{start_date}',
                    f'trade_date=lte.{end_date}',
                    f'maturity_years=gte.0.2',
                    f'ave_compound_yield=not.is.null',
                    f'order=trade_date,maturity_years'
                ]
                
                full_url = f"{url}?{'&'.join(query_params)}"
                response = requests.get(full_url, headers=self.headers)
                
                if response.status_code == 200:
                    data = response.json()
                    # タプルのリストに変換
                    result = []
                    for item in data:
                        result.append((
                            item['trade_date'],
                            item['maturity_years'],
                            item['ave_compound_yield'],
                            item['bond_name']
                        ))
                    return result
                else:
                    self.logger.error(f"データ取得失敗: {response.status_code}")
                    self.logger.error(f"URL: {full_url}")
                    self.logger.error(f"Response: {response.text}")
                    return []
            else:
                self.logger.error("パラメータが不足しています")
                return []
                
        except Exception as e:
            self.logger.error(f"fetch_data エラー: {e}")
            return []