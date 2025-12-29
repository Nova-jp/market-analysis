-- ========================================
-- Supabase RPC Function: Get All Bond Codes from bond_data
-- ========================================
-- Purpose: Retrieve all unique bond codes from bond_data table
--          Bypasses REST API pagination limits (which only returned 227 bonds)
--          Actual count: 3,442 unique bond codes
--
-- Usage from Python:
--   response = supabase.rpc('get_all_bond_codes_from_bond_data').execute()
--   bond_codes = [row['bond_code'] for row in response.data]
--
-- Expected Result: 3,442 bond codes
-- ========================================

-- Drop existing function if it exists
DROP FUNCTION IF EXISTS get_all_bond_codes_from_bond_data();

-- Create RPC function (optimized with GROUP BY instead of DISTINCT)
CREATE OR REPLACE FUNCTION get_all_bond_codes_from_bond_data()
RETURNS TABLE(bond_code TEXT)
LANGUAGE sql
SECURITY DEFINER
SET statement_timeout = '120s'
AS $$
  SELECT bd.bond_code::TEXT AS bond_code
  FROM bond_data bd
  GROUP BY bd.bond_code
  ORDER BY bd.bond_code;
$$;

-- Grant execute permission to authenticated users
GRANT EXECUTE ON FUNCTION get_all_bond_codes_from_bond_data() TO authenticated;

-- Grant execute permission to anon users (if needed for service role)
GRANT EXECUTE ON FUNCTION get_all_bond_codes_from_bond_data() TO anon;

-- Add comment
COMMENT ON FUNCTION get_all_bond_codes_from_bond_data() IS
'Returns all distinct bond codes from bond_data table. Used to retrieve complete list of 3,442 bonds for market_amount recalculation.';
