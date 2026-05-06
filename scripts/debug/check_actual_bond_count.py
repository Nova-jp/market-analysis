#!/usr/bin/env python3
"""
Check actual bond count in bond_data table
"""

import os
import sys
import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

def check_bond_count():
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

    headers = {
        'apikey': supabase_key,
        'Authorization': f'Bearer {supabase_key}',
        'Content-Type': 'application/json'
    }

    print("=" * 70)
    print("CHECKING ACTUAL BOND COUNT")
    print("=" * 70)

    # Method 1: Get all bond codes by paging through all records
    print("\n1. Fetching all unique bond codes from bond_data...")
    bond_codes = set()
    offset = 0
    limit = 1000

    while True:
        response = requests.get(
            f'{supabase_url}/rest/v1/bond_data',
            params={
                'select': 'bond_code',
                'order': 'bond_code.asc',
                'offset': offset,
                'limit': limit
            },
            headers=headers,
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            if not data:
                break

            for record in data:
                bond_codes.add(record['bond_code'])

            print(f"   Fetched offset {offset}, total unique so far: {len(bond_codes)}")
            offset += limit

            # Safety check
            if offset > 2000000:  # Max 2M records
                print("   ⚠️ Safety limit reached")
                break
        else:
            print(f"   ❌ Error: HTTP {response.status_code}")
            break

    print(f"\n✓ Total unique bond codes in bond_data: {len(bond_codes)}")

    # Method 2: Check bond_auction data
    print("\n2. Checking bond_auction table...")
    auction_bonds = set()
    offset = 0

    while True:
        response = requests.get(
            f'{supabase_url}/rest/v1/bond_auction',
            params={
                'select': 'bond_code',
                'total_amount': 'not.is.null',
                'order': 'bond_code.asc',
                'offset': offset,
                'limit': limit
            },
            headers=headers,
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            if not data:
                break

            for record in data:
                auction_bonds.add(record['bond_code'])

            print(f"   Fetched offset {offset}, total unique so far: {len(auction_bonds)}")
            offset += limit

            if offset > 100000:
                print("   ⚠️ Safety limit reached")
                break
        else:
            print(f"   ❌ Error: HTTP {response.status_code}")
            break

    print(f"\n✓ Total unique bond codes in bond_auction: {len(auction_bonds)}")

    # Compare
    print("\n3. Comparison:")
    print(f"   bond_data bonds: {len(bond_codes)}")
    print(f"   bond_auction bonds: {len(auction_bonds)}")

    bonds_without_auction = bond_codes - auction_bonds
    print(f"   Bonds in bond_data but NOT in bond_auction: {len(bonds_without_auction)}")

    if bonds_without_auction:
        print(f"   Sample bonds without auction data: {list(bonds_without_auction)[:10]}")

    bonds_with_auction = bond_codes & auction_bonds
    print(f"   Bonds with auction data (can calculate): {len(bonds_with_auction)}")

    # Save to file
    print("\n4. Saving bond codes to file...")
    with open('/tmp/all_bond_codes.txt', 'w') as f:
        for code in sorted(bond_codes):
            f.write(f"{code}\n")

    with open('/tmp/bonds_with_auction.txt', 'w') as f:
        for code in sorted(bonds_with_auction):
            f.write(f"{code}\n")

    print("   ✓ Saved to /tmp/all_bond_codes.txt")
    print("   ✓ Saved to /tmp/bonds_with_auction.txt")

    print("\n" + "=" * 70)

if __name__ == '__main__':
    check_bond_count()
