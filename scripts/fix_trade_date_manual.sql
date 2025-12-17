-- ======================================================================
-- trade_date 1営業日ずれ修正（手動実行版）
--
-- 実行手順:
-- 1. Supabaseダッシュボード → SQL Editor
-- 2. 以下のSQLを順番に実行
-- 3. 処理時間: 5-10分
-- ======================================================================

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Step 1: ユニーク制約を一時削除
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ALTER TABLE bond_data DROP CONSTRAINT IF EXISTS clean_bond_data_trade_date_bond_code_key;

-- 実行後、成功メッセージを確認してから次へ

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Step 2: インデックスを一時削除（UPDATE高速化）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DROP INDEX IF EXISTS idx_bond_data_trade_date;
DROP INDEX IF EXISTS idx_bond_data_bond_code;

-- 実行後、成功メッセージを確認してから次へ

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Step 3: trade_date 一括UPDATE
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--
-- 警告: このステップはPython側で実行します（jpholiday計算が必要）
--
-- 以下のコマンドを実行:
--
--   source venv/bin/activate && python scripts/fix_trade_date_direct_update.py
--
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Python実行完了後、以下のStep 4, 5を実行してください

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Step 4: ユニーク制約を再作成
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ALTER TABLE bond_data
ADD CONSTRAINT clean_bond_data_trade_date_bond_code_key
UNIQUE (trade_date, bond_code);

-- 実行後、成功メッセージを確認してから次へ

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Step 5: インデックスを再作成
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE INDEX idx_bond_data_trade_date ON bond_data(trade_date);
CREATE INDEX idx_bond_data_bond_code ON bond_data(bond_code);

-- 実行後、成功メッセージを確認してから次へ

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Step 6: 検証
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- 土日データがないことを確認
SELECT
    DATE_PART('dow', trade_date) as day_of_week,
    COUNT(*) as count
FROM bond_data
GROUP BY day_of_week
ORDER BY day_of_week;
-- 期待: 0(日曜), 6(土曜) が0件

-- 総レコード数確認
SELECT COUNT(*) as total_records FROM bond_data;

-- サンプルデータ確認
SELECT bond_code, trade_date
FROM bond_data
WHERE bond_code = '001380045'
ORDER BY trade_date
LIMIT 10;
