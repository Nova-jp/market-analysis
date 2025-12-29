-- Get Unique Trade Dates RPC Function
-- Efficiently retrieves all unique trade dates from bond_data table

-- Drop existing function if it exists
DROP FUNCTION IF EXISTS get_unique_trade_dates();

-- Create function
CREATE OR REPLACE FUNCTION get_unique_trade_dates()
RETURNS TABLE(trade_date DATE)
LANGUAGE sql
STABLE
AS $$
    SELECT DISTINCT bond_data.trade_date
    FROM bond_data
    ORDER BY bond_data.trade_date ASC;
$$;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION get_unique_trade_dates() TO authenticated;
GRANT EXECUTE ON FUNCTION get_unique_trade_dates() TO anon;

-- Test
SELECT * FROM get_unique_trade_dates() LIMIT 10;
