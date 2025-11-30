-- ============================================================
-- bond_auction テーブルに total_amount 列を追加
-- ============================================================
-- 目的: allocated_amount + type1_noncompetitive + type2_noncompetitive の合計を格納
-- 作成日: 2025-11-25
-- ============================================================

-- Step 1: 列の追加
ALTER TABLE bond_auction
ADD COLUMN IF NOT EXISTS total_amount NUMERIC(10, 2);

-- Step 2: 既存データの total_amount を計算して更新
-- NULL値は0として扱う
UPDATE bond_auction
SET total_amount =
    COALESCE(allocated_amount, 0) +
    COALESCE(type1_noncompetitive, 0) +
    COALESCE(type2_noncompetitive, 0);

-- Step 3: インデックス作成（検索性能向上）
CREATE INDEX IF NOT EXISTS idx_bond_auction_total_amount
ON bond_auction(total_amount);

-- Step 4: 列コメント追加
COMMENT ON COLUMN bond_auction.total_amount IS
    '総発行額（単位: 億円）= allocated_amount + type1_noncompetitive + type2_noncompetitive（NULL値は0として扱う）';

-- ============================================================
-- 確認クエリ
-- ============================================================

DO $$
DECLARE
    total_rows INT;
    non_null_rows INT;
BEGIN
    SELECT COUNT(*) INTO total_rows FROM bond_auction;
    SELECT COUNT(*) INTO non_null_rows FROM bond_auction WHERE total_amount IS NOT NULL;

    RAISE NOTICE 'total_amount 列追加完了';
    RAISE NOTICE '総レコード数: %', total_rows;
    RAISE NOTICE 'total_amount が設定されたレコード数: %', non_null_rows;
END $$;

-- ============================================================
-- サンプルデータ確認（最新10件）
-- ============================================================

SELECT
    bond_code,
    auction_date,
    allocated_amount,
    type1_noncompetitive,
    type2_noncompetitive,
    total_amount,
    CASE
        WHEN total_amount = COALESCE(allocated_amount, 0) + COALESCE(type1_noncompetitive, 0) + COALESCE(type2_noncompetitive, 0)
        THEN 'OK'
        ELSE 'NG'
    END AS validation
FROM bond_auction
ORDER BY auction_date DESC
LIMIT 10;
