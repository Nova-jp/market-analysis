from data.collectors.jsda.bond_trade_volume_collector import BondTradeVolumeCollector
import sys

def main():
    collector = BondTradeVolumeCollector()
    year = 2024
    
    print(f"Starting collection for year {year}...")
    success = collector.fetch_and_process_year(year)
    
    if success:
        print("Done.")
    else:
        print("Failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
