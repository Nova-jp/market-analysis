# データ収集モジュール
#
# 構造:
#   data/collectors/
#   ├── jsda/   - JSDA (日本証券業協会) データ
#   ├── mof/    - MOF (財務省) 入札データ
#   └── boj/    - BOJ (日銀) 保有データ
#
# 推奨インポートパス:
#   from data.collectors.jsda import JSDADataCollector, HistoricalBondCollector
#   from data.collectors.mof import LiquiditySupplyCollector, TDBCollector, DailyAuctionCollector
#   from data.collectors.boj import BOJHoldingsCollector

# 後方互換性: サブモジュールからの再エクスポート
from .jsda import JSDADataCollector, HistoricalBondCollector
from .mof import (
    LiquiditySupplyCollector,
    TDBCollector,
    BondAuctionCollector,
    BondAuctionWebCollector,
    AuctionCalendarCollector,
    DailyAuctionCollector,
)
from .boj import BOJHoldingsCollector

__all__ = [
    # JSDA
    'JSDADataCollector',
    'HistoricalBondCollector',
    # MOF
    'LiquiditySupplyCollector',
    'TDBCollector',
    'BondAuctionCollector',
    'BondAuctionWebCollector',
    'AuctionCalendarCollector',
    'DailyAuctionCollector',
    # BOJ
    'BOJHoldingsCollector',
]
