"""
bond_codeの正しい桁数を特定するスクリプト
"""
import os
import sys
import requests
from collections import Counter
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

def load_environment() -> Tuple[str, str]:
    """環境変数を読み込み、SupabaseのURLとキーを返す"""
    # プロジェクトルートの特定（このスクリプトは notebooks/pca/ にあると仮定）
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent.parent
    env_path = project_root / '.env'
    
    load_dotenv(env_path)
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        print("エラー: .envファイルからSUPABASE_URLまたはSUPABASE_KEYが見つかりません。")
        sys.exit(1)
        
    return supabase_url, supabase_key

def get_headers(api_key: str) -> Dict[str, str]:
    """APIリクエスト用のヘッダーを作成"""
    return {
        'apikey': api_key,
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

def fetch_latest_dates(base_url: str, headers: Dict[str, str], limit: int = 10) -> List[str]:
    """最新の取引日付を取得"""
    url = f'{base_url}/rest/v1/bond_data'
    params = {
        'select': 'trade_date',
        'order': 'trade_date.desc',
        'limit': 2000  # 重複除去前にある程度多めに取得
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # ユニークな日付を取得してソート
        dates = sorted(list(set([row['trade_date'] for row in data])), reverse=True)
        return dates[:limit]
    except requests.RequestException as e:
        print(f"日付データの取得中にエラーが発生しました: {e}")
        sys.exit(1)

def fetch_bond_codes_for_date(base_url: str, headers: Dict[str, str], date: str, limit: int = 500) -> List[str]:
    """指定した日付のbond_codeを取得"""
    url = f'{base_url}/rest/v1/bond_data'
    params = {
        'select': 'bond_code',
        'trade_date': f'eq.{date}',
        'limit': limit
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        codes = []
        for row in data:
            bond_code = row.get('bond_code')
            if bond_code is not None:
                codes.append(str(bond_code))
        return codes
    except requests.RequestException as e:
        print(f"日付 {date} のデータ取得中にエラーが発生しました: {e}")
        return []

def analyze_bond_codes(base_url: str, headers: Dict[str, str], dates: List[str]) -> List[Dict[str, Any]]:
    """各日付のbond_code桁数を分析"""
    stats = []
    
    for date in dates:
        codes = fetch_bond_codes_for_date(base_url, headers, date)
        
        length_counter = Counter([len(code) for code in codes])
        
        stats.append({
            'date': date,
            'total': len(codes),
            'length_dist': dict(length_counter),
            'sample': codes[:5] if codes else []
        })
        
    return stats

def print_results(stats: List[Dict[str, Any]]):
    """分析結果を表示"""
    print('\n' + '=' * 100)
    print('【桁数分布】')
    print('=' * 100)
    
    all_lengths = []
    
    for stat in stats:
        print(f"\n日付: {stat['date']} (総件数: {stat['total']})")
        print(f"  桁数分布: {stat['length_dist']}")
        print(f"  サンプル: {stat['sample']}")
        
        for length, count in stat['length_dist'].items():
            all_lengths.extend([length] * count)
            
    # 全体集計
    length_counter_total = Counter(all_lengths)
    most_common = length_counter_total.most_common(1)
    most_common_length = most_common[0][0] if most_common else None
    
    print('\n' + '=' * 100)
    print('【結論】')
    print('=' * 100)
    print(f'全体での桁数分布: {dict(length_counter_total)}')
    
    if most_common_length:
        print(f'最頻桁数: {most_common_length}桁')
        print(f'\n【推奨】全てのbond_codeを{most_common_length}桁に0パディングして正規化')
        print(f'  例: "10054" → "{int("10054"):0{most_common_length}d}" = "{"10054".zfill(most_common_length)}"')

def main():
    print('=' * 100)
    print('【bond_code桁数分析】複数日のデータを調査')
    print('=' * 100)
    
    supabase_url, supabase_key = load_environment()
    headers = get_headers(supabase_key)
    
    print("データ取得中...")
    dates = fetch_latest_dates(supabase_url, headers)
    print(f'\n調査対象日付（最新10日）: {dates}')
    
    stats = analyze_bond_codes(supabase_url, headers, dates)
    print_results(stats)
    print('=' * 100)

if __name__ == "__main__":
    main()
