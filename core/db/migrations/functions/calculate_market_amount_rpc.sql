-- ======================================================================
-- market_amount 計算用 PostgreSQL RPC関数
-- Supabase PRO版の高性能を活用した最速計算
-- ======================================================================

-- 既存の関数を削除（存在する場合）
DROP FUNCTION IF EXISTS calculate_market_amount_fast();

-- 高速計算用RPC関数の作成
CREATE OR REPLACE FUNCTION calculate_market_amount_fast()
RETURNS TABLE(
    updated_count bigint,
    execution_time_seconds numeric
)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time timestamp;
    end_time timestamp;
    affected_rows bigint;
BEGIN
    start_time := clock_timestamp();

    -- Step 1: 一時テーブル作成（発行額累積）
    -- インデックスを活用して高速化
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

    -- インデックス作成で JOIN を高速化
    CREATE INDEX idx_temp_cumulative ON temp_cumulative_issuance(bond_code, trade_date);

    -- Step 2: 一時テーブル作成（日銀保有額）
    -- DISTINCT ON を使って最新の保有額のみ取得
    CREATE TEMP TABLE temp_boj_holdings AS
    SELECT DISTINCT ON (bd.bond_code, bd.trade_date)
        bd.bond_code,
        bd.trade_date,
        COALESCE(bh.face_value, 0) AS boj_holding
    FROM bond_data bd
    LEFT JOIN boj_holdings bh
        ON bd.bond_code = bh.bond_code
        AND bh.data_date <= bd.trade_date
    ORDER BY bd.bond_code, bd.trade_date, bh.data_date DESC NULLS LAST;

    -- インデックス作成
    CREATE INDEX idx_temp_boj ON temp_boj_holdings(bond_code, trade_date);

    -- Step 3: market_amount を一括UPDATE
    WITH calculated_values AS (
        SELECT
            tci.bond_code,
            tci.trade_date,
            (tci.cumulative_issuance - tbh.boj_holding) AS market_amount
        FROM temp_cumulative_issuance tci
        INNER JOIN temp_boj_holdings tbh
            ON tci.bond_code = tbh.bond_code
            AND tci.trade_date = tbh.trade_date
    )
    UPDATE bond_data bd
    SET market_amount = cv.market_amount
    FROM calculated_values cv
    WHERE bd.bond_code = cv.bond_code
      AND bd.trade_date = cv.trade_date;

    -- 更新件数を取得
    GET DIAGNOSTICS affected_rows = ROW_COUNT;

    -- 一時テーブル削除
    DROP TABLE IF EXISTS temp_cumulative_issuance;
    DROP TABLE IF EXISTS temp_boj_holdings;

    end_time := clock_timestamp();

    -- 結果を返す
    RETURN QUERY SELECT
        affected_rows,
        EXTRACT(EPOCH FROM (end_time - start_time))::numeric;
END;
$$;

-- 実行権限を付与
GRANT EXECUTE ON FUNCTION calculate_market_amount_fast() TO anon, authenticated, service_role;

-- 使用方法:
-- SELECT * FROM calculate_market_amount_fast();
