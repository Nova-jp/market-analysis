import sys
import time
from data.collectors.jsda.bond_trade_volume_collector import BondTradeVolumeCollector

def main():
    collector = BondTradeVolumeCollector()
    
    # 未取得の2019年から2018年までを対象とする
    # (2020-2024は取得済み)
    years_to_collect = [2019, 2018]
    
    success_years = []
    failed_years = []
    
    print(f"Starting historical collection for: {years_to_collect}")
    
    for i, year in enumerate(years_to_collect):
        if i > 0:
            print(f"Waiting 30 seconds before next request to respect JSDA server...")
            time.sleep(30)
            
        print("\n" + "="*40)
        print(f"Processing Year: {year}")
        print("="*40)
        
        try:
            success = collector.fetch_and_process_year(year)
            if success:
                success_years.append(year)
            else:
                failed_years.append(year)
        except Exception as e:
            print(f"Unhandled error for year {year}: {e}")
            failed_years.append(year)
            
    print("\n" + "="*40)
    print("Historical Collection Summary")
    print("="*40)
    print(f"Success: {success_years}")
    print(f"Failed: {failed_years}")
    
    if failed_years:
        sys.exit(1)

if __name__ == "__main__":
    main()

