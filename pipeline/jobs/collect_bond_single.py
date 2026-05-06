#!/usr/bin/env python3
"""
1日分データ収集スクリプト
指定した日付の債券データをJSDAから取得してデータベースに保存
"""

import sys
import os
import argparse
from datetime import date

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pipeline.fetchers.jsda.processor import BondDataProcessor
from core.db.sync_client import DatabaseManager

def calculate_market_amount_for_record(calculator, bond_code: str, trade_date: str):
    """
    1レコード分のmarket_amountを計算

    Args:
        calculator: MarketAmountCalculator インスタンス
        bond_code: 銘柄コード
        trade_date: 取引日 (YYYY-MM-DD)

    Returns:
        market_amount (億円), 計算不可時はNone
    """
    try:
        auction_history = calculator.get_auction_history(bond_code)
        boj_history = calculator.get_boj_holdings_history(bond_code)

        # 累積発行額
        cumulative = calculator.calculate_cumulative_issuance(
            auction_history, trade_date
        )
        if cumulative is None:
            return None  # 発行前

        # 日銀保有額
        boj_holding = calculator.get_latest_boj_holding(boj_history, trade_date)

        # 市中残存額
        if boj_holding is not None:
            return cumulative - boj_holding
        else:
            return cumulative

    except Exception as e:
        # print(f"⚠️ market_amount計算エラー ({bond_code}, {trade_date}): {e}")
        return None

def collect_single_day_data(target_date_str, debug=False):
    """
    指定日付のデータを収集してデータベースに保存
    
    Args:
        target_date_str (str): 対象日付 (YYYY-MM-DD形式)
        debug (bool): デバッグモード（保存失敗時に1件ずつ詳細表示）
        
    Returns:
        bool: 成功時True、失敗時False
    """
    try:
        target_date = date.fromisoformat(target_date_str)
    except ValueError:
        print(f"❌ 無効な日付形式: {target_date_str}")
        print("   正しい形式: YYYY-MM-DD (例: 2025-04-21)")
        return False
    
    print(f"🎯 1日分データ収集")
    print("=" * 40)
    print(f"📅 収集対象日付: {target_date}")
    print(f"🔗 データソース: JSDA")
    print(f"🛠️  デバッグモード: {'ON' if debug else 'OFF'}")
    print("-" * 40)
    
    # 初期化
    processor = BondDataProcessor()
    db_manager = DatabaseManager()
    
    try:
        # 1. CSVデータ取得
        print("📡 JSDAからCSVデータを取得中...")
        # build_csv_urlは表示用URLのため、download_data_for_date内で適切に処理される
        url, filename, _ = processor.build_csv_url(target_date)
        print(f"   Target: {filename}")
        
        # download_data_for_dateを使用して取得（年またぎ対応）
        raw_df = processor.download_data_for_date(target_date)
        
        if raw_df is None:
            print("❌ CSVデータの取得に失敗しました")
            return False
        
        if raw_df.empty:
            print("📭 データが空でした（休日の可能性があります）")
            return True
        
        print(f"✅ CSV取得成功: {len(raw_df):,}行")
        
        # 2. データ処理
        print("🔄 データ処理中...")
        processed_df = processor.process_raw_data(raw_df)
        
        if processed_df.empty:
            print("📭 処理後のデータが空でした")
            return True
        
        print(f"✅ データ処理成功: {len(processed_df):,}行")
        
        # 3. trade_dateの検証と補完
        
        # HTML情報に基づく正確な取引日の特定（2026/1/5問題対応）
        actual_trade_date = None
        try:
            actual_trade_date = processor.determine_trade_date_from_html(target_date)
        except Exception as e:
            print(f"⚠️  HTML日付特定失敗: {e}")

        if actual_trade_date:
            print(f"📅 HTML情報により取引日を特定: {target_date_str} -> {actual_trade_date}")
            processed_df['trade_date'] = actual_trade_date.isoformat()
            # market_amount計算用にも補正後の日付を使用する
            final_trade_date_str = actual_trade_date.isoformat()
        else:
            if 'trade_date' in processed_df.columns:
                csv_dates = processed_df['trade_date'].unique()
                if len(csv_dates) > 0 and csv_dates[0] != target_date_str:
                    print(f"⚠️  日付不一致: CSV内={csv_dates[0]}, 指定={target_date_str}")
                    processed_df['trade_date'] = target_date_str
            else:
                processed_df['trade_date'] = target_date_str
            final_trade_date_str = target_date_str
        
        # 4. market_amount計算（今回はスキップ）
        # print("🔢 市中残存額を計算中...")
        # from data.utils.market_amount_calculator import MarketAmountCalculator
        # calculator = MarketAmountCalculator()
        
        # batch_data = processed_df.to_dict('records')
        
        # total_records = len(batch_data)
        # for i, record in enumerate(batch_data):
            # if i % 100 == 0:
                # print(f"   進捗: {i}/{total_records}...")
            
            # bond_code = record.get('bond_code')
            # if bond_code:
                # market_amount = calculate_market_amount_for_record(
                    # calculator, bond_code, final_trade_date_str
                # )
                # record['market_amount'] = market_amount

        # 計算スキップ時はそのまま辞書化
        batch_data = processed_df.to_dict('records')

        # 5. データベース保存
        print("💾 データベースに保存中...")
        saved_count = db_manager.batch_insert_data(batch_data)
        
        if saved_count == 0 and len(batch_data) > 0:
            print("⚠️  バッチ保存失敗。")
            
            if debug:
                print("🛠️  デバッグモード: 1件ずつ試行して詳細エラーを表示します。")
                success_count = 0
                for i, record in enumerate(batch_data):
                    try:
                        # 1件用リストにして保存
                        if db_manager.batch_insert_data([record]) > 0:
                            success_count += 1
                        else:
                            print(f"❌ 保存失敗 (Index {i}):")
                            print(record)
                            break # 最初のエラーで見つかれば十分
                    except Exception as e:
                        print(f"❌ エラー (Index {i}): {e}")
                        print(record)
                        break
                
                if success_count > 0:
                    print(f"⚠️  一部のデータのみ保存されました ({success_count}/{len(batch_data)})")
                    return True # 部分的成功
            else:
                print("💡 詳細なエラーを確認するには --debug オプションを使用してください。")
                
            return False

        if saved_count > 0:
            print(f"🎉 保存成功: {saved_count:,}件")
            
            # 保存結果の詳細表示
            print("\n📊 保存データ詳細:")
            # 銘柄種別の内訳
            issue_type_1 = sum(1 for record in batch_data if record.get('issue_type') == 1)
            issue_type_2 = sum(1 for record in batch_data if record.get('issue_type') == 2)
            print(f"   国庫短期証券 (issue_type=1): {issue_type_1:,}件")
            print(f"   中期国債 (issue_type=2): {issue_type_2:,}件")
            
            return True
        else:
            print("❌ データベース保存に失敗しました")
            return False
            
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        return False

def main():
    """メイン実行"""
    parser = argparse.ArgumentParser(description='1日分データ収集スクリプト')
    parser.add_argument('date', type=str, help='対象日付 (YYYY-MM-DD)')
    parser.add_argument('--debug', action='store_true', help='デバッグモード有効化')
    
    args = parser.parse_args()
    
    success = collect_single_day_data(args.date, debug=args.debug)
    
    if success:
        print("\n✅ 処理完了")
        return 0
    else:
        print("\n❌ 処理失敗")
        return 1

if __name__ == "__main__":
    sys.exit(main())
