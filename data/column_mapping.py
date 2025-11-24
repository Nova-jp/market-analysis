#!/usr/bin/env python3
"""
JSDAデータの列マッピング定義
JSDAの標準的なデータフォーマットに基づく正式な列名定義
"""

# JSDAデータの正式な列定義（29列）
JSDA_COLUMNS = {
    0: {
        'name': 'trade_date',
        'japanese_name': '売買日',
        'description': '取引日（YYYYMMDD形式）',
        'data_type': 'DATE'
    },
    1: {
        'name': 'bond_type',
        'japanese_name': '銘柄区分',
        'description': '債券の種類区分（1:国債、10:長期国債等）',
        'data_type': 'INTEGER'
    },
    2: {
        'name': 'bond_code',
        'japanese_name': '銘柄コード',
        'description': '債券固有の識別コード',
        'data_type': 'VARCHAR(50)'
    },
    3: {
        'name': 'bond_name',
        'japanese_name': '銘柄名',
        'description': '債券の正式名称',
        'data_type': 'VARCHAR(200)'
    },
    4: {
        'name': 'maturity_date',
        'japanese_name': '償還期限',
        'description': '債券の満期日（YYYYMMDD形式）',
        'data_type': 'DATE'
    },
    5: {
        'name': 'bid_price',
        'japanese_name': '買気配値段',
        'description': '買い手の提示価格',
        'data_type': 'DECIMAL(10,3)'
    },
    6: {
        'name': 'ask_price', 
        'japanese_name': '売気配値段',
        'description': '売り手の提示価格',
        'data_type': 'DECIMAL(10,3)'
    },
    7: {
        'name': 'reference_price',
        'japanese_name': '基準値段',
        'description': '基準となる価格（最終取引価格等）',
        'data_type': 'DECIMAL(10,3)'
    },
    8: {
        'name': 'reference_yield',
        'japanese_name': '基準利回り',
        'description': '基準となる利回り（最終利回り）',
        'data_type': 'DECIMAL(8,3)'
    },
    9: {
        'name': 'price_change_mark',
        'japanese_name': '前日比価格符号',
        'description': '前日比の価格変動符号（+/-/変わらず）',
        'data_type': 'VARCHAR(10)'
    },
    10: {
        'name': 'yield_change_mark',
        'japanese_name': '前日比利回り符号',
        'description': '前日比の利回り変動符号（+/-/変わらず）',
        'data_type': 'VARCHAR(10)'
    },
    11: {
        'name': 'trading_status',
        'japanese_name': '売買成立区分',
        'description': '取引成立状況（0:未成立、1:成立）',
        'data_type': 'INTEGER'
    },
    12: {
        'name': 'bid_volume',
        'japanese_name': '買気配数量',
        'description': '買い注文の数量',
        'data_type': 'DECIMAL(15,1)'
    },
    13: {
        'name': 'ask_volume',
        'japanese_name': '売気配数量', 
        'description': '売り注文の数量',
        'data_type': 'DECIMAL(15,1)'
    },
    14: {
        'name': 'current_yield',
        'japanese_name': '最終利回り',
        'description': '最新の取引利回り',
        'data_type': 'DECIMAL(8,3)'
    },
    15: {
        'name': 'time1_price',
        'japanese_name': '時刻1価格',
        'description': '特定時刻（通常9:00）の価格',
        'data_type': 'VARCHAR(20)'
    },
    16: {
        'name': 'time1_yield',
        'japanese_name': '時刻1利回り',
        'description': '特定時刻（通常9:00）の利回り',
        'data_type': 'VARCHAR(20)'
    },
    17: {
        'name': 'time2_price',
        'japanese_name': '時刻2価格',
        'description': '特定時刻（通常15:00）の価格',
        'data_type': 'VARCHAR(20)'
    },
    18: {
        'name': 'time2_yield',
        'japanese_name': '時刻2利回り',
        'description': '特定時刻（通常15:00）の利回り',
        'data_type': 'VARCHAR(20)'
    },
    19: {
        'name': 'reserved_field',
        'japanese_name': '予約フィールド',
        'description': '将来拡張用の予約領域',
        'data_type': 'VARCHAR(10)'
    },
    20: {
        'name': 'market_sector',
        'japanese_name': '市場区分',
        'description': '取引市場の区分（5-9の値）',
        'data_type': 'INTEGER'
    },
    21: {
        'name': 'depth1_bid_price',
        'japanese_name': '気配1買値段',
        'description': '板情報の1番目買気配価格',
        'data_type': 'VARCHAR(20)'
    },
    22: {
        'name': 'depth1_bid_yield',
        'japanese_name': '気配1買利回り',
        'description': '板情報の1番目買気配利回り',
        'data_type': 'VARCHAR(20)'
    },
    23: {
        'name': 'depth2_bid_price',
        'japanese_name': '気配2買値段',
        'description': '板情報の2番目買気配価格',
        'data_type': 'VARCHAR(20)'
    },
    24: {
        'name': 'depth2_bid_yield',
        'japanese_name': '気配2買利回り',
        'description': '板情報の2番目買気配利回り',
        'data_type': 'VARCHAR(20)'
    },
    25: {
        'name': 'depth3_bid_price',
        'japanese_name': '気配3買値段',
        'description': '板情報の3番目買気配価格',
        'data_type': 'VARCHAR(20)'
    },
    26: {
        'name': 'depth3_bid_yield',
        'japanese_name': '気配3買利回り',
        'description': '板情報の3番目買気配利回り',
        'data_type': 'VARCHAR(20)'
    },
    27: {
        'name': 'depth1_ask_price',
        'japanese_name': '気配1売値段',
        'description': '板情報の1番目売気配価格',
        'data_type': 'VARCHAR(20)'
    },
    28: {
        'name': 'depth1_ask_yield',
        'japanese_name': '気配1売利回り',
        'description': '板情報の1番目売気配利回り',
        'data_type': 'VARCHAR(20)'
    }
}

def get_create_table_sql():
    """正式な列名でのCREATE TABLE文を生成"""
    
    sql_parts = [
        "-- JSDAデータの正式列名による整形済み国債データテーブル",
        "",
        "-- 既存テーブル削除",
        "DROP TABLE IF EXISTS bond_data;",
        "",
        "-- テーブル作成",
        "CREATE TABLE bond_data (",
        "    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,",
        ""
    ]
    
    # 各列の定義を追加
    for col_idx in range(29):
        col_info = JSDA_COLUMNS[col_idx]
        col_name = col_info['name']
        data_type = col_info['data_type']
        japanese_name = col_info['japanese_name']
        description = col_info['description']
        
        # NULL制約の設定
        null_constraint = ""
        if col_idx in [0, 1, 2, 3, 4]:  # 基本情報は NOT NULL
            null_constraint = " NOT NULL"
        
        sql_parts.append(f"    -- {japanese_name}（{description}）")
        sql_parts.append(f"    {col_name} {data_type}{null_constraint},")
        sql_parts.append("")
    
    # 追加の分析用列
    sql_parts.extend([
        "    -- 分析用追加列",
        "    bond_category VARCHAR(50),              -- 債券カテゴリ",
        "    bond_category_name VARCHAR(100),        -- 債券カテゴリ名",
        "    years_to_maturity DECIMAL(8,2),         -- 残存年数",
        "",
        "    -- メタデータ", 
        "    data_source VARCHAR(20) DEFAULT 'JSDA',",
        "    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),",
        "    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),",
        "",
        "    -- 制約",
        "    UNIQUE(trade_date, bond_code)",
        ");",
        "",
        "-- インデックス作成",
        "CREATE INDEX idx_clean_bond_trade_date ON bond_data(trade_date);",
        "CREATE INDEX idx_clean_bond_category ON bond_data(bond_category);",
        "CREATE INDEX idx_clean_bond_yield ON bond_data(reference_yield);",
        "CREATE INDEX idx_clean_bond_maturity ON bond_data(years_to_maturity);",
        "",
        "-- RLS設定",
        "ALTER TABLE bond_data ENABLE ROW LEVEL SECURITY;",
        "CREATE POLICY \"Allow public read access on bond_data\" ON bond_data FOR SELECT USING (true);",
        "CREATE POLICY \"Allow public insert on bond_data\" ON bond_data FOR INSERT WITH CHECK (true);",
        "CREATE POLICY \"Allow public update on bond_data\" ON bond_data FOR UPDATE USING (true);",
        "CREATE POLICY \"Allow public delete on bond_data\" ON bond_data FOR DELETE USING (true);"
    ])
    
    return "\n".join(sql_parts)

def print_column_mapping():
    """列マッピング情報を表示"""
    print("=== JSDAデータ列マッピング（正式版）===")
    print(f"{'列番号':>3} | {'列名':>20} | {'日本語名':>15} | {'説明'}")
    print("-" * 80)
    
    for col_idx in range(29):
        col_info = JSDA_COLUMNS[col_idx]
        print(f"{col_idx:>3} | {col_info['name']:>20} | {col_info['japanese_name']:>15} | {col_info['description']}")

if __name__ == "__main__":
    print_column_mapping()
    print("\n" + "="*80)
    print("CREATE TABLE SQL:")
    print(get_create_table_sql())