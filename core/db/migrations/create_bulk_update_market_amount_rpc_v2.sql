-- Bulk Update Market Amount RPC Function (Optimized Version)
-- Uses single UPDATE statement with JSONB unnesting for maximum performance
-- Expected to handle 10,000+ records without timeout

-- Drop existing function
DROP FUNCTION IF EXISTS bulk_update_market_amount(JSONB);

-- Create optimized function
CREATE OR REPLACE FUNCTION bulk_update_market_amount(
    update_data JSONB
)
RETURNS TABLE(
    updated_count INTEGER,
    skipped_count INTEGER,
    error_count INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_updated INTEGER := 0;
    v_input_count INTEGER := 0;
BEGIN
    -- Get input record count
    SELECT jsonb_array_length(update_data) INTO v_input_count;

    -- Perform bulk update using single UPDATE statement with JSONB unnesting
    -- This is much faster than looping through records
    WITH update_values AS (
        SELECT
            (elem->>'bond_code')::VARCHAR(50) AS bond_code,
            (elem->>'trade_date')::DATE AS trade_date,
            (elem->>'market_amount')::DECIMAL(15,2) AS market_amount
        FROM jsonb_array_elements(update_data) AS elem
    )
    UPDATE bond_data
    SET market_amount = uv.market_amount,
        updated_at = NOW()
    FROM update_values uv
    WHERE bond_data.bond_code = uv.bond_code
      AND bond_data.trade_date = uv.trade_date;

    -- Get number of rows updated
    GET DIAGNOSTICS v_updated = ROW_COUNT;

    -- Return summary
    -- updated_count: actual rows updated
    -- skipped_count: input records that didn't match any existing rows
    -- error_count: always 0 (errors would raise exception)
    RETURN QUERY SELECT
        v_updated,
        v_input_count - v_updated AS skipped,
        0 AS errors;

EXCEPTION
    WHEN OTHERS THEN
        -- On error, return error count
        RETURN QUERY SELECT 0, 0, v_input_count;
        RAISE WARNING 'bulk_update_market_amount failed: %', SQLERRM;
END;
$$;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION bulk_update_market_amount(JSONB) TO authenticated;

-- Test with empty array
SELECT * FROM bulk_update_market_amount('[]'::JSONB);
