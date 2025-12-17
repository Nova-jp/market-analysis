-- ======================================================================
-- RPC関数: update_trade_dates_batch
--
-- 目的: bond_dataテーブルのtrade_dateを一括UPDATE
--       （1営業日ずれ問題の修正用）
--
-- 使用方法:
-- 1. Supabaseダッシュボード（https://yfravzuebsvkzjnabalj.supabase.co）にアクセス
-- 2. SQL Editorを開く
-- 3. このSQLを貼り付けて実行
-- 4. 成功後、scripts/fix_trade_date_offset.py を実行
--
-- 処理速度:
-- - 約178万件のレコードを5-10分で処理
-- - バッチサイズ: 50日分の日付マッピング
-- ======================================================================

CREATE OR REPLACE FUNCTION update_trade_dates_batch(
    updates jsonb
) RETURNS jsonb AS $$
DECLARE
    item jsonb;
    total_updated int := 0;
    rows_affected int;
BEGIN
    -- JSONB配列の各要素に対してUPDATE実行
    -- 形式: [{"old_date": "2024-12-09", "new_date": "2024-12-06"}, ...]
    FOR item IN SELECT * FROM jsonb_array_elements(updates)
    LOOP
        UPDATE bond_data
        SET trade_date = (item->>'new_date')::date,
            updated_at = NOW()
        WHERE trade_date = (item->>'old_date')::date;

        -- 更新件数をカウント
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        total_updated := total_updated + rows_affected;
    END LOOP;

    -- 総更新件数をJSON形式で返却
    RETURN jsonb_build_object(
        'total_updated', total_updated,
        'batch_size', jsonb_array_length(updates)
    );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 権限設定（必要に応じて）
-- GRANT EXECUTE ON FUNCTION update_trade_dates_batch(jsonb) TO authenticated;
-- GRANT EXECUTE ON FUNCTION update_trade_dates_batch(jsonb) TO service_role;

-- ======================================================================
-- テスト実行（オプション）
-- ======================================================================
-- 以下のクエリで関数が正しく動作するか確認できます:
/*
SELECT update_trade_dates_batch('[
    {"old_date": "2024-12-09", "new_date": "2024-12-06"}
]'::jsonb);

-- 結果を確認:
SELECT COUNT(*)
FROM bond_data
WHERE trade_date = '2024-12-06'::date;
*/
