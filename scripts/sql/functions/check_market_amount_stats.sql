-- ======================================================================
-- market_amount 統計情報取得用 RPC関数
-- ======================================================================

-- 既存の関数を削除（存在する場合）
DROP FUNCTION IF EXISTS check_market_amount_stats();

-- 統計情報取得用RPC関数
CREATE OR REPLACE FUNCTION check_market_amount_stats()
RETURNS TABLE(
    total_records bigint,
    calculated_records bigint,
    null_records bigint,
    completion_percentage numeric,
    min_value bigint,
    max_value bigint,
    avg_value numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::bigint AS total_records,
        COUNT(market_amount)::bigint AS calculated_records,
        (COUNT(*) - COUNT(market_amount))::bigint AS null_records,
        ROUND(100.0 * COUNT(market_amount) / NULLIF(COUNT(*), 0), 2) AS completion_percentage,
        MIN(market_amount)::bigint AS min_value,
        MAX(market_amount)::bigint AS max_value,
        ROUND(AVG(market_amount), 0) AS avg_value
    FROM bond_data;
END;
$$;

-- 実行権限を付与
GRANT EXECUTE ON FUNCTION check_market_amount_stats() TO anon, authenticated, service_role;

-- 使用方法:
-- SELECT * FROM check_market_amount_stats();
