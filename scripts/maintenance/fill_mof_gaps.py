import sys
import os
from datetime import date, timedelta
import logging

# パスを通す
sys.path.append(os.getcwd())

from data.collectors.mof.bond_auction_web_collector import BondAuctionWebCollector
from data.utils.database_manager import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fill_mof_gaps():
    collector = BondAuctionWebCollector()
    db_manager = DatabaseManager()
    
    start_date = date(2025, 10, 1)
    end_date = date(2026, 1, 10)
    
    current_date = start_date
    total_saved = 0
    
    # 毎日チェックするのは非効率なので、カレンダーを取得して入札がある日だけ処理
    # 月ごとに処理
    months = []
    d = start_date
    while d <= end_date:
        if (d.year, d.month) not in months:
            months.append((d.year, d.month))
        d += timedelta(days=20) # 大まかに進める
    
    for year, month in months:
        logger.info(f"Checking MOF calendar for {year}-{month}")
        html = collector.fetch_calendar_page(year, month)
        if not html:
            continue
            
        # その月の全日をチェック
        for day in range(1, 32):
            try:
                target_date = date(year, month, day)
                if target_date < start_date or target_date > end_date:
                    continue
                
                # 入札予定があるか確認
                schedules = collector.extract_auction_schedule(html, target_date)
                if schedules:
                    logger.info(f"Found auction on {target_date}: {[s['bond_name'] for s in schedules]}")
                    results = collector.collect_auction_data(target_date)
                    if results:
                        # DB保存用に日付を文字列に変換
                        for r in results:
                            if 'auction_date' in r: r['auction_date'] = r['auction_date'].isoformat()
                            if 'issue_date' in r: r['issue_date'] = r['issue_date'].isoformat()
                            if 'maturity_date' in r: r['maturity_date'] = r['maturity_date'].isoformat()
                            # 不要なキーを削除（DBカラムにないもの）
                            if 'source_url' in r: del r['source_url']
                        
                        saved = db_manager.batch_insert_data(results, table_name='bond_auction')
                        total_saved += saved
                        logger.info(f"  Saved {saved} records to bond_auction")
            except ValueError:
                continue
                
    logger.info(f"MOF gap fill complete. Total saved: {total_saved}")

if __name__ == "__main__":
    fill_mof_gaps()
