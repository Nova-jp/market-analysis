import sys
import os
from datetime import date
import logging

# パスを通す
sys.path.append(os.getcwd())

# ログ設定
logging.basicConfig(level=logging.INFO)

def test_boj():
    print("\n=== Testing BOJ Holdings Collector ===")
    try:
        from data.collectors.boj.holdings_collector import BOJHoldingsCollector
        
        collector = BOJHoldingsCollector(delay_seconds=1.0)
        
        # 2025年のリスト取得
        print("Fetching file list for 2025...")
        links = collector.get_file_links_for_year(2025)
        print(f"Found {len(links)} files for 2025.")
        
        target_link = None
        if links:
            target_link = links[0]
        else:
            print("No files found for 2025. Trying 2024...")
            links = collector.get_file_links_for_year(2024)
            print(f"Found {len(links)} files for 2024.")
            if links:
                target_link = links[0]

        if target_link:
            print(f"Testing download and parse for: {target_link['url']}")
            try:
                records = collector.download_and_parse(target_link)
                print(f"Successfully parsed {len(records)} records.")
                if records:
                    print("\n--- BOJ Holdings Samples (First 5) ---")
                    for r in records[:5]:
                        print(r)
            except Exception as e:
                print(f"Error parsing BOJ data: {e}")
        else:
            print("No files found to test.")

    except Exception as e:
        print(f"BOJ Collector Error: {e}")

def test_mof():
    print("\n=== Testing MOF Auction Collector ===")
    try:
        from data.collectors.mof.bond_auction_web_collector import BondAuctionWebCollector
        
        collector = BondAuctionWebCollector()
        
        # 2024年12月のカレンダーから入札日を探す
        year = 2024
        month = 12
        print(f"Fetching calendar for {year}-{month} to find auction dates...")
        html = collector.fetch_calendar_page(year, month)
        
        if html:
            # 1日から順に探す
            found_date = None
            for day in range(1, 20):
                try:
                    d = date(year, month, day)
                    schedules = collector.extract_auction_schedule(html, d)
                    if schedules:
                        print(f"Found auction on {d}: {[s['bond_name'] for s in schedules]}")
                        found_date = d
                        # 最初の見つかった日でテスト終了
                        break
                except ValueError:
                    continue # 日付が存在しない場合など
            
            if found_date:
                print(f"Testing data collection for {found_date}...")
                results = collector.collect_auction_data(found_date)
                print(f"Collected {len(results)} results.")
                for r in results:
                    print("\n--- MOF Auction Full Result ---")
                    import pprint
                    pprint.pprint(r)
            else:
                print(f"No auction found in {year}-{month}.")
        else:
            print("Failed to fetch calendar.")

    except Exception as e:
        print(f"MOF Collector Error: {e}")

if __name__ == "__main__":
    test_boj()
    test_mof()
