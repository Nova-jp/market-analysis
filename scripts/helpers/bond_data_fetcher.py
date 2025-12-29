#!/usr/bin/env python3
"""
Bond Data Fetcher

Fetches bond-related data from Supabase database for market_amount calculation.
Provides functions to retrieve auction data, BOJ holdings, and trade dates for individual bonds.
"""

import requests
from typing import List, Dict, Any
from datetime import datetime


class BondDataFetcher:
    """
    Fetcher for bond-related data from Supabase database
    """

    def __init__(self, supabase_url: str, supabase_key: str):
        """
        Initialize fetcher with Supabase credentials

        Args:
            supabase_url: Supabase project URL
            supabase_key: Supabase API key (Service Role Key)
        """
        self.supabase_url = supabase_url
        self.headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json'
        }

    def fetch_auctions(self, bond_code: str) -> List[Dict[str, Any]]:
        """
        Fetch all auction records for a specific bond

        Args:
            bond_code: 9-digit bond code

        Returns:
            List of auction records sorted by auction_date ascending
            Each record contains: auction_date, total_amount, bond_code
        """
        try:
            response = requests.get(
                f'{self.supabase_url}/rest/v1/bond_auction',
                params={
                    'select': 'auction_date,total_amount,bond_code',
                    'bond_code': f'eq.{bond_code}',
                    'total_amount': 'not.is.null',
                    'order': 'auction_date.asc',
                    'limit': 1000
                },
                headers=self.headers,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                return [
                    {
                        'auction_date': record['auction_date'],
                        'total_amount': float(record['total_amount']),
                        'bond_code': record['bond_code']
                    }
                    for record in data
                ]
            else:
                print(f"  ⚠️ Failed to fetch auctions for {bond_code}: HTTP {response.status_code}")
                return []

        except Exception as e:
            print(f"  ❌ Error fetching auctions for {bond_code}: {e}")
            return []

    def fetch_boj_holdings(self, bond_code: str) -> List[Dict[str, Any]]:
        """
        Fetch all BOJ holdings records for a specific bond

        Args:
            bond_code: 9-digit bond code

        Returns:
            List of BOJ holdings records sorted by data_date ascending
            Each record contains: data_date, face_value, bond_code
        """
        try:
            response = requests.get(
                f'{self.supabase_url}/rest/v1/boj_holdings',
                params={
                    'select': 'data_date,face_value,bond_code',
                    'bond_code': f'eq.{bond_code}',
                    'face_value': 'not.is.null',
                    'order': 'data_date.asc',
                    'limit': 10000
                },
                headers=self.headers,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                return [
                    {
                        'data_date': record['data_date'],
                        'face_value': float(record['face_value']),
                        'bond_code': record['bond_code']
                    }
                    for record in data
                ]
            else:
                print(f"  ⚠️ Failed to fetch BOJ holdings for {bond_code}: HTTP {response.status_code}")
                return []

        except Exception as e:
            print(f"  ❌ Error fetching BOJ holdings for {bond_code}: {e}")
            return []

    def fetch_trade_dates(self, bond_code: str) -> List[str]:
        """
        Fetch all trade dates for a specific bond

        Args:
            bond_code: 9-digit bond code

        Returns:
            List of trade dates (YYYY-MM-DD format) sorted ascending
        """
        try:
            response = requests.get(
                f'{self.supabase_url}/rest/v1/bond_data',
                params={
                    'select': 'trade_date',
                    'bond_code': f'eq.{bond_code}',
                    'order': 'trade_date.asc',
                    'limit': 10000
                },
                headers=self.headers,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                return [record['trade_date'] for record in data]
            else:
                print(f"  ⚠️ Failed to fetch trade dates for {bond_code}: HTTP {response.status_code}")
                return []

        except Exception as e:
            print(f"  ❌ Error fetching trade dates for {bond_code}: {e}")
            return []

    def get_all_bond_codes(self) -> List[str]:
        """
        Fetch ALL unique bond codes from bond_data table in small batches

        Uses cursor-based pagination to retrieve bond codes in manageable batches.
        Fetches approximately 20 unique bonds at a time to avoid memory/timeout issues.
        Expected count: 3,442 unique bond codes

        Returns:
            List of bond codes sorted (should be 3,442 bonds)
        """
        print("  Fetching bond codes in small batches...")
        all_bond_codes = set()  # Use set for automatic deduplication
        last_bond_code = ''
        batch_size = 100  # Fetch 100 records at a time (gives ~20 unique bonds per batch)
        max_iterations = 10000  # Safety limit to prevent infinite loops
        iteration = 0

        while iteration < max_iterations:
            try:
                # Build request params with cursor-based pagination
                params = {
                    'select': 'bond_code',
                    'order': 'bond_code.asc',
                    'limit': batch_size
                }

                # Add cursor condition if not first iteration
                if last_bond_code:
                    params['bond_code'] = f'gt.{last_bond_code}'

                # Fetch batch
                response = requests.get(
                    f'{self.supabase_url}/rest/v1/bond_data',
                    params=params,
                    headers=self.headers,
                    timeout=30
                )

                if response.status_code == 200:
                    data = response.json()

                    # If no data returned, we've fetched everything
                    if not data:
                        print(f"  ✓ Completed fetching all bond codes")
                        break

                    # Add bond codes to set (automatically removes duplicates)
                    for record in data:
                        all_bond_codes.add(record['bond_code'])

                    # Update cursor to last bond code in this batch
                    last_bond_code = data[-1]['bond_code']

                    # Progress update every 10 iterations
                    if iteration % 10 == 0:
                        print(f"     Retrieved {len(all_bond_codes)} unique bonds (iteration {iteration})...")

                    iteration += 1

                else:
                    print(f"  ❌ HTTP {response.status_code} at iteration {iteration}")
                    break

            except Exception as e:
                print(f"  ❌ Error at iteration {iteration}: {e}")
                break

        # Convert set to sorted list
        result = sorted(list(all_bond_codes))
        print(f"  ✓ Retrieved {len(result)} unique bond codes in {iteration} iterations")

        # Verify expected count
        if len(result) != 3442:
            print(f"  ⚠️ WARNING: Expected 3,442 bonds but got {len(result)}")

        return result
