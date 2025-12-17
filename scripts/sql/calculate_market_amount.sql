-- ======================================================================
-- 市中残存額（market_amount）一括計算SQL
-- ======================================================================
--
-- 実行方法:
-- 1. Supabase Dashboard → SQL Editor
-- 2. このSQLをコピー＆ペースト
-- 3. Run ボタンをクリック
--
-- 処理時間見込み: 3-5分
-- ======================================================================

-- Step 1: 一時テーブル作成（発行額累積）
CREATE TEMP TABLE temp_cumulative_issuance AS
SELECT
    bd.bond_code,
    bd.trade_date,
    COALESCE(SUM(ba.total_amount), 0) AS cumulative_issuance
FROM bond_data bd
LEFT JOIN bond_auction ba
    ON bd.bond_code = ba.bond_code
    AND ba.auction_date <= bd.trade_date
GROUP BY bd.bond_code, bd.trade_date;

-- Step 2: 一時テーブル作成（日銀保有額）
CREATE TEMP TABLE temp_boj_holdings AS
SELECT DISTINCT ON (bd.bond_code, bd.trade_date)
    bd.bond_code,
    bd.trade_date,
    COALESCE(bh.face_value, 0) AS boj_holding
FROM bond_data bd
LEFT JOIN boj_holdings bh
    ON bd.bond_code = bh.bond_code
    AND bh.data_date <= bd.trade_date
ORDER BY bd.bond_code, bd.trade_date, bh.data_date DESC;

-- Step 3: market_amount計算・UPDATE
UPDATE bond_data
SET market_amount = (
    tci.cumulative_issuance - tbh.boj_holding
)
FROM temp_cumulative_issuance tci
JOIN temp_boj_holdings tbh
    ON tci.bond_code = tbh.bond_code
    AND tci.trade_date = tbh.trade_date
WHERE bond_data.bond_code = tci.bond_code
  AND bond_data.trade_date = tci.trade_date;

-- Step 4: クリーンアップ
DROP TABLE temp_cumulative_issuance;
DROP TABLE temp_boj_holdings;

-- Step 5: 結果確認
SELECT
    COUNT(*) AS total_records,
    COUNT(market_amount) AS calculated_records,
    MIN(market_amount) AS min_market_amount,
    MAX(market_amount) AS max_market_amount,
    AVG(market_amount) AS avg_market_amount
FROM bond_data;
