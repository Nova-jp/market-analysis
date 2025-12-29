#!/usr/bin/env python3
"""
Verify total_amount Column in bond_auction Table

Checks if the total_amount column exists in the bond_auction table
and verifies data availability for market_amount calculation.
"""

import os
import sys
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.utils.database_manager import DatabaseManager

def verify_total_amount_column():
    """
    Verify if total_amount column exists in bond_auction table
    and check data quality
    """
    db_manager = DatabaseManager()

    print("=" * 70)
    print("VERIFYING bond_auction.total_amount COLUMN")
    print("=" * 70)

    # Check 1: Query sample records from bond_auction
    print("\n1. Checking bond_auction table structure...")
    try:
        import requests

        # Get sample records to see available columns
        response = requests.get(
            f'{db_manager.supabase_url}/rest/v1/bond_auction',
            params={
                'select': '*',
                'limit': 5
            },
            headers=db_manager.headers,
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"   ✓ Found {len(data)} sample records")
                print(f"   ✓ Available columns: {', '.join(data[0].keys())}")

                # Check if total_amount exists
                if 'total_amount' in data[0]:
                    print("   ✅ total_amount column EXISTS")

                    # Check if it has values
                    sample_value = data[0].get('total_amount')
                    print(f"   ✓ Sample total_amount value: {sample_value}")
                else:
                    print("   ❌ total_amount column DOES NOT EXIST")
                    print("   → Need to use: allocated_amount + type1_noncompetitive + type2_noncompetitive")

                # Show sample record
                print("\n   Sample record:")
                for key, value in list(data[0].items())[:10]:
                    print(f"     {key}: {value}")
            else:
                print("   ⚠️ No records found in bond_auction table")
        else:
            print(f"   ❌ Error querying table: HTTP {response.status_code}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    # Check 2: Count records with total_amount data
    print("\n2. Checking data availability...")
    try:
        # Try to query with total_amount filter
        response = requests.get(
            f'{db_manager.supabase_url}/rest/v1/bond_auction',
            params={
                'select': 'bond_code,auction_date,total_amount',
                'total_amount': 'not.is.null',
                'limit': 10
            },
            headers=db_manager.headers,
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            print(f"   ✓ Found {len(data)} records with total_amount NOT NULL")
            if data:
                print("   ✅ total_amount column has data")
                print(f"   Sample values: {[r.get('total_amount') for r in data[:3]]}")
            else:
                print("   ⚠️ total_amount column exists but all values are NULL")
        elif response.status_code == 400:
            print("   ❌ Column likely does not exist (HTTP 400 Bad Request)")
        else:
            print(f"   ⚠️ Unexpected response: HTTP {response.status_code}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    # Check 3: Alternative calculation method
    print("\n3. Checking alternative calculation method...")
    try:
        response = requests.get(
            f'{db_manager.supabase_url}/rest/v1/bond_auction',
            params={
                'select': 'bond_code,auction_date,allocated_amount,type1_noncompetitive,type2_noncompetitive',
                'allocated_amount': 'not.is.null',
                'limit': 5
            },
            headers=db_manager.headers,
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            print(f"   ✓ Found {len(data)} records with allocated_amount")
            if data:
                # Calculate total_amount from components
                for record in data[:3]:
                    allocated = float(record.get('allocated_amount', 0))
                    type1 = float(record.get('type1_noncompetitive', 0) or 0)
                    type2 = float(record.get('type2_noncompetitive', 0) or 0)
                    calculated_total = allocated + type1 + type2
                    print(f"   {record['bond_code']} ({record['auction_date']}): {allocated} + {type1} + {type2} = {calculated_total}")

                print("   ✅ Alternative calculation method is viable")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    # Check 4: Compare with current market_amount calculation
    print("\n4. Comparing with current market_amount data...")
    try:
        # Get a sample bond with market_amount
        response = requests.get(
            f'{db_manager.supabase_url}/rest/v1/bond_data',
            params={
                'select': 'bond_code,trade_date,market_amount',
                'market_amount': 'not.is.null',
                'limit': 5
            },
            headers=db_manager.headers,
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"   ✓ Found {len(data)} records with market_amount")
                print(f"   Sample bond_code for testing: {data[0]['bond_code']}")
                print(f"   Sample market_amount: {data[0]['market_amount']}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    print("\n" + "=" * 70)
    print("VERIFICATION COMPLETE")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. If total_amount EXISTS: Use it directly in calculation")
    print("  2. If total_amount MISSING: Calculate from allocated_amount + type1 + type2")
    print("  3. Proceed with bond-by-bond processor implementation")
    print("=" * 70)

if __name__ == '__main__':
    verify_total_amount_column()
