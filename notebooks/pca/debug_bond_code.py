"""
bond_codeのDB保存形式を診断するスクリプト
"""
import os
import sys
import requests
from dotenv import load_dotenv

# 環境変数読み込み
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
load_dotenv(os.path.join(project_root, '.env'))

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# ヘッダー設定
headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}'
}

# 最新の2日分の日付を取得
print('=' * 100)
print('【ステップ1】最新の営業日を取得中...')
print('=' * 100)

response = requests.get(
    f'{SUPABASE_URL}/rest/v1/bond_data',
    params={'select': 'trade_date', 'order': 'trade_date.desc', 'limit': 1000},
    headers=headers
)

dates = sorted(list(set([row['trade_date'] for row in response.json()])), reverse=True)
latest_date = dates[0]
previous_date = dates[1]

print(f'最新日: {latest_date}')
print(f'前日: {previous_date}')

# 最新日のデータを取得
print('\n' + '=' * 100)
print(f'【ステップ2】最新日 ({latest_date}) のbond_codeを取得中...')
print('=' * 100)

response_latest = requests.get(
    f'{SUPABASE_URL}/rest/v1/bond_data',
    params={
        'select': 'bond_code,ave_compound_yield,due_date,trade_date',
        'trade_date': f'eq.{latest_date}',
        'order': 'bond_code.asc',
        'limit': 300
    },
    headers=headers
)

data_latest_raw = response_latest.json()

print(f'取得件数: {len(data_latest_raw)}件')
print(f'\n先頭10件のbond_code:')
print('-' * 100)
for i, row in enumerate(data_latest_raw[:10]):
    bond_code = row.get('bond_code')
    print(f'{i+1:2d}. bond_code: "{bond_code}" | Python型: {type(bond_code).__name__} | 長さ: {len(str(bond_code)) if bond_code else 0}')

# 前日のデータを取得
print('\n' + '=' * 100)
print(f'【ステップ3】前日 ({previous_date}) のbond_codeを取得中...')
print('=' * 100)

response_previous = requests.get(
    f'{SUPABASE_URL}/rest/v1/bond_data',
    params={
        'select': 'bond_code,ave_compound_yield,due_date,trade_date',
        'trade_date': f'eq.{previous_date}',
        'order': 'bond_code.asc',
        'limit': 300
    },
    headers=headers
)

data_previous_raw = response_previous.json()

print(f'取得件数: {len(data_previous_raw)}件')
print(f'\n先頭10件のbond_code:')
print('-' * 100)
for i, row in enumerate(data_previous_raw[:10]):
    bond_code = row.get('bond_code')
    print(f'{i+1:2d}. bond_code: "{bond_code}" | Python型: {type(bond_code).__name__} | 長さ: {len(str(bond_code)) if bond_code else 0}')

# 先頭0の有無をチェック
print('\n' + '=' * 100)
print('【ステップ4】先頭0の有無をチェック')
print('=' * 100)

latest_codes_with_zero = []
latest_codes_without_zero = []

for row in data_latest_raw:
    bond_code = row.get('bond_code')
    if bond_code is not None:
        bond_code_str = str(bond_code)
        if bond_code_str.startswith('0'):
            latest_codes_with_zero.append(bond_code)
        else:
            latest_codes_without_zero.append(bond_code)

previous_codes_with_zero = []
previous_codes_without_zero = []

for row in data_previous_raw:
    bond_code = row.get('bond_code')
    if bond_code is not None:
        bond_code_str = str(bond_code)
        if bond_code_str.startswith('0'):
            previous_codes_with_zero.append(bond_code)
        else:
            previous_codes_without_zero.append(bond_code)

print(f'最新日:')
print(f'  先頭0あり: {len(latest_codes_with_zero)}件')
if latest_codes_with_zero:
    print(f'    例（先頭5件）: {latest_codes_with_zero[:5]}')
print(f'  先頭0なし: {len(latest_codes_without_zero)}件')
if latest_codes_without_zero:
    print(f'    例（先頭5件）: {latest_codes_without_zero[:5]}')

print(f'\n前日:')
print(f'  先頭0あり: {len(previous_codes_with_zero)}件')
if previous_codes_with_zero:
    print(f'    例（先頭5件）: {previous_codes_with_zero[:5]}')
print(f'  先頭0なし: {len(previous_codes_without_zero)}件')
if previous_codes_without_zero:
    print(f'    例（先頭5件）: {previous_codes_without_zero[:5]}')

# データ型の分布をチェック
print('\n' + '=' * 100)
print('【ステップ5】データ型の分布')
print('=' * 100)

type_counts_latest = {}
for row in data_latest_raw:
    bond_code = row.get('bond_code')
    t = type(bond_code).__name__
    type_counts_latest[t] = type_counts_latest.get(t, 0) + 1

type_counts_previous = {}
for row in data_previous_raw:
    bond_code = row.get('bond_code')
    t = type(bond_code).__name__
    type_counts_previous[t] = type_counts_previous.get(t, 0) + 1

print(f'最新日のPython型分布: {type_counts_latest}')
print(f'前日のPython型分布: {type_counts_previous}')

# 共通のbond_codeを探す
print('\n' + '=' * 100)
print('【ステップ6】両日で共通のbond_codeを探す（マッチング検証）')
print('=' * 100)

latest_codes_set = set()
for row in data_latest_raw:
    bond_code = row.get('bond_code')
    if bond_code is not None:
        latest_codes_set.add(str(bond_code))

previous_codes_set = set()
for row in data_previous_raw:
    bond_code = row.get('bond_code')
    if bond_code is not None:
        previous_codes_set.add(str(bond_code))

common_codes = latest_codes_set & previous_codes_set

print(f'最新日のユニークbond_code数: {len(latest_codes_set)}')
print(f'前日のユニークbond_code数: {len(previous_codes_set)}')
print(f'両日共通のbond_code数: {len(common_codes)}')
print(f'\n共通bond_code（先頭10件）: {sorted(list(common_codes))[:10]}')

print('\n' + '=' * 100)
print('【診断完了】')
print('=' * 100)
