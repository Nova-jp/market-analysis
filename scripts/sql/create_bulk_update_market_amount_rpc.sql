-- Bulk Update Market Amount RPC Function
-- Performs efficient bulk updates of market_amount for bond_data table
-- Uses JSONB array input to minimize round-trips and maximize performance

-- Drop existing function if it exists
DROP FUNCTION IF EXISTS bulk_update_market_amount(JSONB);

-- Create function
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
    updated INTEGER := 0;
    skipped INTEGER := 0;
    errors INTEGER := 0;
    record_data JSONB;
    v_bond_code VARCHAR(50);
    v_trade_date DATE;
    v_market_amount DECIMAL(15,2);
BEGIN
    -- Iterate through JSONB array
    FOR record_data IN SELECT * FROM jsonb_array_elements(update_data)
    LOOP
        BEGIN
            -- Extract values from JSONB
            v_bond_code := record_data->>'bond_code';
            v_trade_date := (record_data->>'trade_date')::DATE;
            v_market_amount := (record_data->>'market_amount')::DECIMAL(15,2);

            -- Attempt update
            UPDATE bond_data
            SET market_amount = v_market_amount,
                updated_at = NOW()
            WHERE bond_code = v_bond_code
              AND trade_date = v_trade_date;

            -- Check if row was updated
            IF FOUND THEN
                updated := updated + 1;
            ELSE
                skipped := skipped + 1;
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                errors := errors + 1;
                -- Log error but continue processing
                RAISE WARNING 'Error updating bond_code=% trade_date=%: %',
                    v_bond_code, v_trade_date, SQLERRM;
        END;
    END LOOP;

    -- Return summary statistics
    RETURN QUERY SELECT updated, skipped, errors;
END;
$$;

-- Grant execute permission to authenticated users
GRANT EXECUTE ON FUNCTION bulk_update_market_amount(JSONB) TO authenticated;

-- Test with empty array
SELECT * FROM bulk_update_market_amount('[]'::JSONB);
