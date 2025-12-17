-- ======================================================================
-- market_amount 超高速計算用 PostgreSQL RPC関数
-- Supabase PRO版専用（60秒タイムアウト対応）
-- ======================================================================

-- 既存の関数を削除（存在する場合）
DROP FUNCTION IF EXISTS calculate_market_amount_ultra_fast();

-- 超高速計算用RPC関数の作成
CREATE OR REPLACE FUNCTION calculate_market_amount_ultra_fast()
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

    -- market_amount を一括UPDATE（サブクエリで直接計算）
    UPDATE bond_data bd
    SET market_amount = (
        -- 累積発行額
        COALESCE((
            SELECT SUM(ba.total_amount)
            FROM bond_auction ba
            WHERE ba.bond_code = bd.bond_code
              AND ba.auction_date <= bd.trade_date
        ), 0)
        -
        -- 日銀保有額（最新）
        COALESCE((
            SELECT bh.face_value
            FROM boj_holdings bh
            WHERE bh.bond_code = bd.bond_code
              AND bh.data_date <= bd.trade_date
            ORDER BY bh.data_date DESC
            LIMIT 1
        ), 0)
    )
    WHERE bd.market_amount IS NULL;

    -- 更新件数を取得
    GET DIAGNOSTICS affected_rows = ROW_COUNT;

    end_time := clock_timestamp();

    -- 結果を返す
    RETURN QUERY SELECT
        affected_rows,
        EXTRACT(EPOCH FROM (end_time - start_time))::numeric;
END;
$$;

-- 実行権限を付与
GRANT EXECUTE ON FUNCTION calculate_market_amount_ultra_fast() TO anon, authenticated, service_role;

-- 使用方法:
-- SELECT * FROM calculate_market_amount_ultra_fast();
