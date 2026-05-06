-- ======================================================================
-- market_amount 統一計算用 PostgreSQL RPC関数
-- 全粒度対応（年・月・半月・全データ）+ 後方互換性維持
-- ======================================================================

-- 既存の統一関数を削除（存在する場合）
DROP FUNCTION IF EXISTS calculate_market_amount_unified(date, date);

-- 統一RPC関数の作成（コア実装）
CREATE OR REPLACE FUNCTION calculate_market_amount_unified(
    start_date date DEFAULT NULL,
    end_date date DEFAULT NULL
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

    -- 日付範囲フィルタを動的に適用（NULL = 全件処理）
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
      AND (start_date IS NULL OR bd.trade_date >= start_date)
      AND (end_date IS NULL OR bd.trade_date <= end_date);

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
GRANT EXECUTE ON FUNCTION calculate_market_amount_unified(date, date)
  TO anon, authenticated, service_role;

-- ======================================================================
-- 後方互換性ラッパー関数（既存Pythonコードは変更不要）
-- ======================================================================

-- 年単位バッチ計算（既存関数のラッパー）
DROP FUNCTION IF EXISTS calculate_market_amount_batch(date, date);

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
BEGIN
    RETURN QUERY SELECT * FROM calculate_market_amount_unified(start_date, end_date);
END;
$$;

GRANT EXECUTE ON FUNCTION calculate_market_amount_batch(date, date)
  TO anon, authenticated, service_role;

-- 月単位バッチ計算（既存関数のラッパー）
DROP FUNCTION IF EXISTS calculate_market_amount_monthly(date, date);

CREATE OR REPLACE FUNCTION calculate_market_amount_monthly(
    start_date date,
    end_date date
)
RETURNS TABLE(
    updated_count bigint,
    execution_time_seconds numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY SELECT * FROM calculate_market_amount_unified(start_date, end_date);
END;
$$;

GRANT EXECUTE ON FUNCTION calculate_market_amount_monthly(date, date)
  TO anon, authenticated, service_role;

-- 半月単位バッチ計算（既存関数のラッパー）
DROP FUNCTION IF EXISTS calculate_market_amount_biweekly(date, date);

CREATE OR REPLACE FUNCTION calculate_market_amount_biweekly(
    start_date date,
    end_date date
)
RETURNS TABLE(
    updated_count bigint,
    execution_time_seconds numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY SELECT * FROM calculate_market_amount_unified(start_date, end_date);
END;
$$;

GRANT EXECUTE ON FUNCTION calculate_market_amount_biweekly(date, date)
  TO anon, authenticated, service_role;

-- 全データ計算（既存関数のラッパー）
DROP FUNCTION IF EXISTS calculate_market_amount_ultra_fast();

CREATE OR REPLACE FUNCTION calculate_market_amount_ultra_fast()
RETURNS TABLE(
    updated_count bigint,
    execution_time_seconds numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY SELECT * FROM calculate_market_amount_unified(NULL, NULL);
END;
$$;

GRANT EXECUTE ON FUNCTION calculate_market_amount_ultra_fast()
  TO anon, authenticated, service_role;

-- ======================================================================
-- 使用例
-- ======================================================================
-- 年単位: SELECT * FROM calculate_market_amount_unified('2002-01-01', '2002-12-31');
-- 月単位: SELECT * FROM calculate_market_amount_unified('2002-08-01', '2002-08-31');
-- 半月単位: SELECT * FROM calculate_market_amount_unified('2002-08-01', '2002-08-15');
-- 全データ: SELECT * FROM calculate_market_amount_unified(NULL, NULL);
--
-- 既存の関数名もそのまま使用可能（互換性維持）:
-- SELECT * FROM calculate_market_amount_batch('2002-01-01', '2002-12-31');
-- SELECT * FROM calculate_market_amount_monthly('2002-08-01', '2002-08-31');
-- SELECT * FROM calculate_market_amount_biweekly('2002-08-01', '2002-08-15');
-- SELECT * FROM calculate_market_amount_ultra_fast();
