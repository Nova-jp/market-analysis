-- ======================================================================
-- trade_date 1営業日ずれ修正（安全版）
--
-- 戦略: ユニーク制約を一時的に無効化して一括UPDATE
--
-- 実行方法:
-- 1. Supabaseダッシュボード → SQL Editor
-- 2. このSQLを貼り付けて実行
-- 3. 実行時間: 5-10分
-- ======================================================================

-- Step 1: ユニーク制約を削除
ALTER TABLE bond_data DROP CONSTRAINT IF EXISTS clean_bond_data_trade_date_bond_code_key;

-- Step 2: インデックスも削除（高速化）
DROP INDEX IF EXISTS idx_bond_data_trade_date;
DROP INDEX IF EXISTS idx_bond_data_bond_code;

-- Step 3: 一括UPDATE（全レコードを1営業日前にシフト）
-- 注意: この処理は jpholiday の結果をハードコードしたマッピングテーブルを使用します
-- Python側で生成したマッピングに基づいて更新

-- このSQLは直接実行せず、Pythonスクリプト経由で実行します
-- 理由: jpholiday計算はPythonでしか実行できないため

-- 以下のコメントアウトされたクエリは参考用です:
/*
WITH date_mapping AS (
    SELECT
        '2021-01-20'::date as old_date,
        '2021-01-19'::date as new_date
    UNION ALL
    SELECT '2021-01-21'::date, '2021-01-20'::date
    -- ... (Pythonで生成)
)
UPDATE bond_data bd
SET trade_date = dm.new_date,
    updated_at = NOW()
FROM date_mapping dm
WHERE bd.trade_date = dm.old_date;
*/

-- Step 4: 制約を再作成
ALTER TABLE bond_data
ADD CONSTRAINT clean_bond_data_trade_date_bond_code_key
UNIQUE (trade_date, bond_code);

-- Step 5: インデックスを再作成
CREATE INDEX idx_bond_data_trade_date ON bond_data(trade_date);
CREATE INDEX idx_bond_data_bond_code ON bond_data(bond_code);

-- Step 6: 検証
SELECT
    DATE_PART('dow', trade_date) as day_of_week,
    COUNT(*) as count
FROM bond_data
GROUP BY day_of_week
ORDER BY day_of_week;
-- 期待: 0(日曜), 6(土曜) が0件

-- ======================================================================
-- 実行後の確認クエリ
-- ======================================================================

-- 総レコード数確認
SELECT COUNT(*) as total_records FROM bond_data;

-- サンプルデータ確認
SELECT bond_code, trade_date, market_amount
FROM bond_data
WHERE bond_code = '001380045'
ORDER BY trade_date
LIMIT 10;
