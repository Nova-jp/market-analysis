#!/usr/bin/env python3
"""
対内対外証券投資（週次）データ収集・インポートスクリプト

Source: https://www.mof.go.jp/policy/international_policy/reference/itn_transactions_in_securities/week.csv
Usage:
    python scripts/collect_mof_international_transactions.py
"""
import os
import sys
import io
import csv
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Load .env file
load_dotenv()

# データソースURL
DATA_URL = "https://www.mof.go.jp/policy/international_policy/reference/itn_transactions_in_securities/week.csv"

# データベース接続情報
DB_HOST = os.getenv('DB_HOST') or os.getenv('CLOUD_SQL_HOST')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME') or os.getenv('CLOUD_SQL_DATABASE')
DB_USER = os.getenv('DB_USER') or os.getenv('CLOUD_SQL_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD') or os.getenv('CLOUD_SQL_PASSWORD')


def parse_date(date_str):
    """
    日付文字列をパースする
    例: "2005．1．2" -> "2005-01-02"
    """
    try:
        # 全角数字やドットを半角に変換
        clean_str = date_str.strip().replace("．", ".").replace(" ", "")
        
        # 年号が含まれていない場合は処理が複雑になるが、
        # このCSVは "YYYY.M.D" の形式のようだ
        
        # 年、月、日を抽出
        parts = clean_str.split('.')
        if len(parts) >= 3:
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            return datetime(year, month, day).date()
            
        # 形式が違う場合（年がない場合など）
        # 文脈から年を補完する必要があるかもしれないが、
        # CSVの仕様上、開始日には必ず年がついているように見える
        # "2005．1．2～ 1．8" の後半 "1.8" には年がない
        if len(parts) == 2:
            # 月、日のみの場合（使用しない予定だが一応）
            return None
            
        return None
    except Exception as e:
        print(f"日付パースエラー: {date_str} -> {e}")
        return None


def parse_period(period_str):
    """
    期間文字列をパースする
    例: "2005．1．2～ 1．8"
    """
    try:
        # "～" で分割
        # 環境によっては "～" が異なる文字コードになる可能性に注意
        # 財務省のCSVはShift-JIS
        
        if "～" in period_str:
            sep = "～"
        else:
            # 文字化け対策などで他の可能性も考慮する場合
            sep = "~" # 仮
            
        parts = period_str.split(sep)
        if len(parts) != 2:
            return None, None
            
        start_str = parts[0].strip()
        end_str = parts[1].strip()
        
        start_date = parse_date(start_str)
        
        # 終了日は年が省略されている場合がある
        # "2005．1．2" -> year=2005
        # "1．8" -> yearを補完
        
        # end_strのクリーニング
        clean_end = end_str.strip().replace("．", ".").replace(" ", "")
        end_parts = clean_end.split('.')
        
        if start_date:
            year = start_date.year
            if len(end_parts) == 2:
                # 年がない場合、開始日の年を使う
                # ただし、年末年始で年をまたぐ場合（12/28 - 1/4など）は注意
                month = int(end_parts[0])
                day = int(end_parts[1])
                
                # 年またぎ判定
                if start_date.month == 12 and month == 1:
                    year += 1
                
                end_date = datetime(year, month, day).date()
            elif len(end_parts) >= 3:
                # 年がある場合
                end_date = parse_date(end_str)
            else:
                end_date = None
        else:
            end_date = None
            
        return start_date, end_date

    except Exception as e:
        print(f"期間パースエラー: {period_str} -> {e}")
        return None, None


def parse_value(value_str):
    """
    数値文字列をパースする（億円単位）
    例: "1,689 " -> 1689
    空文字や "-" は None (NULL) にする
    """
    if not value_str:
        return None
    
    clean_str = value_str.strip().replace(",", "")
    if clean_str == "" or clean_str == "-":
        return None
        
    try:
        return int(clean_str)
    except ValueError:
        return None


def fetch_and_import_data():
    """CSVをダウンロードしてDBにインポート"""
    print("=" * 60)
    print("対内対外証券投資データ収集・インポート")
    print("=" * 60)

    # 1. CSVダウンロード
    print(f"ダウンロード中: {DATA_URL}")
    try:
        response = requests.get(DATA_URL)
        response.raise_for_status()
        
        # エンコーディング判定
        # 財務省CSVは通常 Shift_JIS (cp932)
        response.encoding = 'cp932' 
        content = response.text
        
        print("✅ ダウンロード完了")
        
    except Exception as e:
        print(f"❌ ダウンロードエラー: {e}")
        return False

    # 2. CSVパース
    data_rows = []
    
    # StringIOを使ってCSVとして読み込む
    f = io.StringIO(content)
    reader = csv.reader(f)
    
    # ヘッダー行を探す、またはスキップする
    # データは "2005．1．2～ 1．8" のような日付が入っている行から開始
    
    header_found = False
    row_count = 0
    
    for row in reader:
        if not row:
            continue
            
        # データ行の判定: 1列目に期間が入っているか
        # 形式: "YYYY．M．D～"
        first_col = row[0].strip()
        
        # 簡易的な判定: 数字と「.」と「～」が含まれているか
        if "．" in first_col and "～" in first_col:
            # パース処理
            try:
                # 期間
                start_date, end_date = parse_period(first_col)
                
                if not start_date or not end_date:
                    continue
                
                # 値の抽出（カラム位置は固定と仮定）
                # CSVのカラム構成（web_fetchの結果から推測）:
                # 0: 期間
                # -- 対外 (Assets) --
                # 1: 株式 取得
                # 2: 株式 処分
                # 3: 株式 ネット
                # 4: 中長期債 取得
                # 5: 中長期債 処分
                # 6: 中長期債 ネット
                # 7: 小計 ネット
                # 8: 短期債 取得
                # 9: 短期債 処分
                # 10: 短期債 ネット
                # 11: 合計 ネット (net only in CSV header?) -> No, "Total" col 11
                # Wait, looking at CSV header again:
                # 1. Equity: Acq(1), Disp(2), Net(3)
                # 2. Long-term: Acq(4), Disp(5), Net(6)
                # 3. Subtotal: Net(7) ?
                # 4. Short-term: Acq(8), Disp(9), Net(10), Net(11)?
                # Let's re-examine the web_fetch output carefully.
                
                # Header structure from web_fetch:
                # Row 14: Acq, Disp, Net, Acq, Disp, Net, Net, Acq, Disp, Net, Net, Acq, Disp, Net, Acq, Disp, Net, Net, Acq, Disp, Net, Net
                # This suggests repeating blocks.
                # Col 0: Period
                # Assets (Residents):
                # Equity: 1, 2, 3
                # Long-term: 4, 5, 6
                # Subtotal: 7 (Net)
                # Short-term: 8, 9, 10
                # Total: 11 (Net)
                
                # Liabilities (Non-Residents):
                # Equity: 12, 13, 14
                # Long-term: 15, 16, 17
                # Subtotal: 18 (Net)
                # Short-term: 19, 20, 21
                # Total: 22 (Net)
                
                # Let's map to DB columns
                record = {
                    'start_date': start_date,
                    'end_date': end_date,
                    
                    # Assets
                    'outward_equity_acquisition': parse_value(row[1]),
                    'outward_equity_disposition': parse_value(row[2]),
                    'outward_equity_net': parse_value(row[3]),
                    
                    'outward_long_term_acquisition': parse_value(row[4]),
                    'outward_long_term_disposition': parse_value(row[5]),
                    'outward_long_term_net': parse_value(row[6]),
                    
                    'outward_subtotal_net': parse_value(row[7]),
                    
                    'outward_short_term_acquisition': parse_value(row[8]),
                    'outward_short_term_disposition': parse_value(row[9]),
                    'outward_short_term_net': parse_value(row[10]),
                    
                    'outward_total_net': parse_value(row[11]),
                    
                    # Liabilities
                    'inward_equity_acquisition': parse_value(row[12]),
                    'inward_equity_disposition': parse_value(row[13]),
                    'inward_equity_net': parse_value(row[14]),
                    
                    'inward_long_term_acquisition': parse_value(row[15]),
                    'inward_long_term_disposition': parse_value(row[16]),
                    'inward_long_term_net': parse_value(row[17]),
                    
                    'inward_subtotal_net': parse_value(row[18]),
                    
                    'inward_short_term_acquisition': parse_value(row[19]),
                    'inward_short_term_disposition': parse_value(row[20]),
                    'inward_short_term_net': parse_value(row[21]),
                    
                    'inward_total_net': parse_value(row[22]),
                }
                
                data_rows.append(record)
                row_count += 1
                
            except IndexError:
                print(f"⚠️ 行の解析スキップ (列不足): {row[0]}")
                continue
            except Exception as e:
                print(f"⚠️ 行の解析エラー: {row[0]} -> {e}")
                continue

    print(f"解析完了: {len(data_rows)} 件のデータを取得")

    if not data_rows:
        print("❌ データが見つかりませんでした")
        return False

    # 3. データベースへの保存 (UPSERT)
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("\nデータベースへ保存中...")
        
        inserted_count = 0
        updated_count = 0
        
        # 一括挿入も可能だが、件数がそこまで多くない(数千件)ので
        # 確実性のためにexecutemanyまたはループで処理
        
        insert_sql = """
            INSERT INTO mof_international_transactions (
                start_date, end_date,
                outward_equity_acquisition, outward_equity_disposition, outward_equity_net,
                outward_long_term_acquisition, outward_long_term_disposition, outward_long_term_net,
                outward_subtotal_net,
                outward_short_term_acquisition, outward_short_term_disposition, outward_short_term_net,
                outward_total_net,
                inward_equity_acquisition, inward_equity_disposition, inward_equity_net,
                inward_long_term_acquisition, inward_long_term_disposition, inward_long_term_net,
                inward_subtotal_net,
                inward_short_term_acquisition, inward_short_term_disposition, inward_short_term_net,
                inward_total_net,
                updated_at
            ) VALUES (
                %(start_date)s, %(end_date)s,
                %(outward_equity_acquisition)s, %(outward_equity_disposition)s, %(outward_equity_net)s,
                %(outward_long_term_acquisition)s, %(outward_long_term_disposition)s, %(outward_long_term_net)s,
                %(outward_subtotal_net)s,
                %(outward_short_term_acquisition)s, %(outward_short_term_disposition)s, %(outward_short_term_net)s,
                %(outward_total_net)s,
                %(inward_equity_acquisition)s, %(inward_equity_disposition)s, %(inward_equity_net)s,
                %(inward_long_term_acquisition)s, %(inward_long_term_disposition)s, %(inward_long_term_net)s,
                %(inward_subtotal_net)s,
                %(inward_short_term_acquisition)s, %(inward_short_term_disposition)s, %(inward_short_term_net)s,
                %(inward_total_net)s,
                NOW()
            )
            ON CONFLICT (start_date) DO UPDATE SET
                end_date = EXCLUDED.end_date,
                outward_equity_acquisition = EXCLUDED.outward_equity_acquisition,
                outward_equity_disposition = EXCLUDED.outward_equity_disposition,
                outward_equity_net = EXCLUDED.outward_equity_net,
                outward_long_term_acquisition = EXCLUDED.outward_long_term_acquisition,
                outward_long_term_disposition = EXCLUDED.outward_long_term_disposition,
                outward_long_term_net = EXCLUDED.outward_long_term_net,
                outward_subtotal_net = EXCLUDED.outward_subtotal_net,
                outward_short_term_acquisition = EXCLUDED.outward_short_term_acquisition,
                outward_short_term_disposition = EXCLUDED.outward_short_term_disposition,
                outward_short_term_net = EXCLUDED.outward_short_term_net,
                outward_total_net = EXCLUDED.outward_total_net,
                inward_equity_acquisition = EXCLUDED.inward_equity_acquisition,
                inward_equity_disposition = EXCLUDED.inward_equity_disposition,
                inward_equity_net = EXCLUDED.inward_equity_net,
                inward_long_term_acquisition = EXCLUDED.inward_long_term_acquisition,
                inward_long_term_disposition = EXCLUDED.inward_long_term_disposition,
                inward_long_term_net = EXCLUDED.inward_long_term_net,
                inward_subtotal_net = EXCLUDED.inward_subtotal_net,
                inward_short_term_acquisition = EXCLUDED.inward_short_term_acquisition,
                inward_short_term_disposition = EXCLUDED.inward_short_term_disposition,
                inward_short_term_net = EXCLUDED.inward_short_term_net,
                inward_total_net = EXCLUDED.inward_total_net,
                updated_at = NOW();
        """
        
        cursor.executemany(insert_sql, data_rows)
        
        print(f"✅ {len(data_rows)} 件のデータを処理しました")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("完了！")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"\n❌ データベースエラー: {e}")
        return False


if __name__ == '__main__':
    success = fetch_and_import_data()
    sys.exit(0 if success else 1)
