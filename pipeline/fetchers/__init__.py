# pipeline/fetchers — データ収集モジュール
#
# 構造:
#   pipeline/fetchers/
#   ├── jsda/   - JSDA (日本証券業協会) 国債市場データ
#   ├── mof/    - MOF (財務省) 入札データ
#   ├── boj/    - BOJ (日本銀行) 保有データ
#   ├── jscc/   - JSCC/JPX IRS/OIS データ
#   └── macro/  - マクロ経済指標（Yahoo Finance / FRED）
#
# 推奨インポートパス:
#   from pipeline.fetchers.jsda import JSDADataCollector
#   from pipeline.fetchers.mof import AuctionCalendarCollector, DailyAuctionCollector
#   from pipeline.fetchers.boj import BOJHoldingsCollector
