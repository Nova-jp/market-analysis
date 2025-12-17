-- ======================================================================
-- market_amount バッチ計算用 PostgreSQL RPC関数
-- 日付範囲を指定して計算（タイムアウト対策）
-- ======================================================================

-- 既存の関数を削除（存在する場合）
DROP FUNCTION IF EXISTS calculate_market_amount_batch(date, date);

-- バッチ計算用RPC関数の作成
CREATE OR REPLACE FUNCTION calculate_market_amount_batch(
    start_date date,
    end_date date
)
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

    -- 指定期間のmarket_amount を一括UPDATE
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
    WHERE bd.market_amount IS NULL
      AND bd.trade_date >= start_date
      AND bd.trade_date <= end_date;

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
GRANT EXECUTE ON FUNCTION calculate_market_amount_batch(date, date) TO anon, authenticated, service_role;

-- 使用例:
-- 1年分ずつ処理
-- SELECT * FROM calculate_market_amount_batch('2002-01-01', '2002-12-31');
-- SELECT * FROM calculate_market_amount_batch('2003-01-01', '2003-12-31');
