-- bond_dataテーブルにmarket_amountカラムを追加
-- 市中残存額 = 累積発行額 - 日銀保有額（億円単位）

-- カラム追加
ALTER TABLE bond_data
ADD COLUMN IF NOT EXISTS market_amount BIGINT;

-- コメント追加
COMMENT ON COLUMN bond_data.market_amount IS '市中残存額（億円）= 累積発行額 - 日銀保有額';

-- インデックス作成（市中残存額でのフィルタリング用）
CREATE INDEX IF NOT EXISTS idx_bond_data_market_amount ON bond_data(market_amount);
