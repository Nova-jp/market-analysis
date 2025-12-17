-- ======================================================================
-- RPC関数: update_market_amounts
--
-- 目的: 市中残存額（market_amount）の一括UPDATE用サーバーサイド関数
--
-- 使用方法:
-- 1. Supabaseダッシュボード（https://yfravzuebsvkzjnabalj.supabase.co）にアクセス
-- 2. SQL Editorを開く
-- 3. このSQLを貼り付けて実行
-- 4. 成功後、scripts/calculate_market_amount_rpc.py を実行
--
-- 期待される処理速度:
-- - 従来: 50時間以上（個別UPDATE × 120,000件）
-- - RPC版: 5-10分（バッチRPC × 120回）
-- ======================================================================

CREATE OR REPLACE FUNCTION update_market_amounts(
    updates jsonb
) RETURNS int AS $$
DECLARE
    update_count int := 0;
    rows_affected int;
    item jsonb;
BEGIN
    -- JSONB配列の各要素に対してUPDATE実行
    FOR item IN SELECT * FROM jsonb_array_elements(updates)
    LOOP
        UPDATE bond_data
        SET market_amount = (item->>'market_amount')::bigint
        WHERE bond_code = item->>'bond_code'
          AND trade_date = (item->>'trade_date')::date;

        -- 更新件数をカウント
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        update_count := update_count + rows_affected;
    END LOOP;

    -- 総更新件数を返却
    RETURN update_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 権限設定（必要に応じて調整）
-- GRANT EXECUTE ON FUNCTION update_market_amounts(jsonb) TO authenticated;
-- GRANT EXECUTE ON FUNCTION update_market_amounts(jsonb) TO service_role;

-- ======================================================================
-- テスト実行（オプション）
-- ======================================================================
-- 以下のクエリで関数が正しく動作するか確認できます:
/*
SELECT update_market_amounts('[
    {"bond_code": "001380045", "trade_date": "2002-01-04", "market_amount": 10000}
]'::jsonb);

-- 結果を確認:
SELECT bond_code, trade_date, market_amount
FROM bond_data
WHERE bond_code = '001380045' AND trade_date = '2002-01-04';
*/
