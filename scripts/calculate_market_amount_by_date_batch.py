#!/usr/bin/env python3
"""
Market Amount Calculation - 15-Day Batch Processor

Calculates market_amount for all bonds using date-based batching:
1. Fetches all unique trade dates
2. Splits dates into 15-day batches
3. For each batch, calculates market_amount for all bonds with trades in that period
4. Uses PostgreSQL RPC function for efficient bulk updates

Expected performance: ~250 RPC calls for 3,750 days (vs 206,520 individual PATCH requests)
Speedup: 800x faster
"""

import os
import sys
import argparse
import requests
from typing import List, Dict, Any, Set
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.helpers.bond_data_fetcher import BondDataFetcher
from scripts.helpers.cumulative_calculator import calculate_cumulative_by_date, expand_cumulative_to_all_dates
from scripts.helpers.forward_fill import forward_fill_boj_holdings

load_dotenv()


class MarketAmountDateBatchProcessor:
    """
    Processes market_amount calculation in 15-day batches

    Algorithm:
    1. Get all unique trade dates (sorted)
    2. Split into 15-day batches
    3. For each batch:
       a. Get all bonds traded in that period
       b. For each bond:
          - Fetch all auctions (full history)
          - Fetch all BOJ holdings (full history)
          - Calculate market_amount for dates in current batch
       c. Bulk update via RPC function
    """

    def __init__(self, batch_days: int = 15):
        """
        Initialize processor

        Args:
            batch_days: Number of days per batch (default: 15)
        """
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment")

        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }

        self.fetcher = BondDataFetcher(self.supabase_url, self.supabase_key)
        self.batch_days = batch_days

        # Statistics
        self.stats = {
            'batches_processed': 0,
            'total_records_updated': 0,
            'total_records_skipped': 0,
            'total_errors': 0,
            'bonds_processed': 0
        }

    def get_all_trade_dates(self) -> List[str]:
        """
        Fetch all unique trade dates from bond_data table
        Uses year-by-year approach to avoid large offsets and timeouts

        Returns:
            List of trade dates (YYYY-MM-DD) sorted ascending
        """
        print("\n📅 Fetching all unique trade dates (year-by-year)...")

        all_dates = set()

        # Fetch dates year by year from 2002 to 2025
        for year in range(2002, 2026):
            print(f"  Processing year {year}...")
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"

            offset = 0
            batch_size = 5000

            try:
                while True:
                    response = requests.get(
                        f'{self.supabase_url}/rest/v1/bond_data',
                        params={
                            'select': 'trade_date',
                            'trade_date': f'gte.{start_date}',
                            'trade_date': f'lte.{end_date}',
                            'order': 'trade_date.asc',
                            'limit': batch_size,
                            'offset': offset
                        },
                        headers=self.headers,
                        timeout=60
                    )

                    if response.status_code != 200:
                        print(f"    ⚠️ HTTP {response.status_code} for year {year}")
                        break

                    data = response.json()

                    # If no data returned, we've fetched everything for this year
                    if not data:
                        break

                    # Add dates to set (automatically removes duplicates)
                    before_count = len(all_dates)
                    for record in data:
                        all_dates.add(record['trade_date'])

                    # If we got less than batch_size records, we're done with this year
                    if len(data) < batch_size:
                        break

                    offset += batch_size

            except Exception as e:
                print(f"    ❌ Error fetching year {year}: {e}")
                continue

            print(f"    → {len(all_dates)} total dates collected so far")

        # Convert set to sorted list
        dates = sorted(list(all_dates))
        print(f"✓ Found {len(dates)} unique trade dates across all years")
        if dates:
            print(f"  Date range: {dates[0]} to {dates[-1]}")
        return dates

    def create_date_batches(self, all_dates: List[str]) -> List[List[str]]:
        """
        Split dates into batches of specified size

        Args:
            all_dates: List of all trade dates (sorted)

        Returns:
            List of date batches, each containing up to batch_days dates
        """
        batches = []
        for i in range(0, len(all_dates), self.batch_days):
            batch = all_dates[i:i + self.batch_days]
            batches.append(batch)

        print(f"\n📦 Created {len(batches)} batches ({self.batch_days} days each)")
        return batches

    def get_bonds_in_date_range(self, start_date: str, end_date: str) -> List[str]:
        """
        Get all unique bond codes that have trades in the specified date range

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            List of unique bond codes
        """
        try:
            response = requests.get(
                f'{self.supabase_url}/rest/v1/bond_data',
                params={
                    'select': 'bond_code',
                    'trade_date': f'gte.{start_date}',
                    'trade_date': f'lte.{end_date}',
                    'order': 'bond_code.asc',
                    'limit': 100000
                },
                headers=self.headers,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                bonds = sorted(list(set(record['bond_code'] for record in data)))
                return bonds
            else:
                print(f"  ⚠️ Failed to fetch bonds for date range: HTTP {response.status_code}")
                return []

        except Exception as e:
            print(f"  ❌ Error fetching bonds: {e}")
            return []

    def calculate_market_amount_for_bond_dates(
        self,
        bond_code: str,
        target_dates: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Calculate market_amount for a specific bond for given dates

        Args:
            bond_code: 9-digit bond code
            target_dates: List of trade dates to calculate for

        Returns:
            List of update records: [{'bond_code': ..., 'trade_date': ..., 'market_amount': ...}]
        """
        try:
            # 1. Fetch auction data (full history)
            auctions = self.fetcher.fetch_auctions(bond_code)
            if not auctions:
                return []

            # 2. Fetch BOJ holdings (full history)
            boj_holdings = self.fetcher.fetch_boj_holdings(bond_code)

            # 3. Calculate cumulative issuance
            cumulative_by_auction_date = calculate_cumulative_by_date(auctions)
            cumulative_by_trade_date = expand_cumulative_to_all_dates(
                cumulative_by_auction_date,
                target_dates
            )

            # 4. Forward-fill BOJ holdings
            boj_by_trade_date = forward_fill_boj_holdings(boj_holdings, target_dates)

            # 5. Calculate market_amount for each target date
            updates = []
            for trade_date in target_dates:
                cumulative = cumulative_by_trade_date.get(trade_date, 0.0)
                boj = boj_by_trade_date.get(trade_date, 0.0)
                market_amount = round(cumulative - boj, 2)

                updates.append({
                    'bond_code': bond_code,
                    'trade_date': trade_date,
                    'market_amount': market_amount
                })

            return updates

        except Exception as e:
            print(f"    ⚠️ Error calculating for {bond_code}: {e}")
            return []

    def bulk_update_via_rpc(self, updates: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Bulk update market_amount using PostgreSQL RPC function
        Chunks large updates into smaller pieces to avoid timeouts

        Args:
            updates: List of update records

        Returns:
            Dictionary with update statistics:
            {
                'updated_count': int,
                'skipped_count': int,
                'error_count': int
            }
        """
        chunk_size = 500  # Process 500 records per RPC call to avoid timeouts
        total_updated = 0
        total_skipped = 0
        total_errors = 0

        # Split updates into chunks
        for i in range(0, len(updates), chunk_size):
            chunk = updates[i:i + chunk_size]

            try:
                # Call RPC function for this chunk
                response = requests.post(
                    f'{self.supabase_url}/rest/v1/rpc/bulk_update_market_amount',
                    json={'update_data': chunk},
                    headers=self.headers,
                    timeout=120  # 2 minutes timeout
                )

                if response.status_code == 200:
                    result = response.json()
                    if result and len(result) > 0:
                        total_updated += result[0].get('updated_count', 0)
                        total_skipped += result[0].get('skipped_count', 0)
                        total_errors += result[0].get('error_count', 0)
                else:
                    print(f"    ⚠️ RPC call failed for chunk {i}-{i+len(chunk)}: HTTP {response.status_code}")
                    print(f"       Response: {response.text[:200]}")
                    total_skipped += len(chunk)

            except Exception as e:
                print(f"    ❌ RPC error for chunk {i}-{i+len(chunk)}: {e}")
                total_skipped += len(chunk)

        return {
            'updated_count': total_updated,
            'skipped_count': total_skipped,
            'error_count': total_errors
        }

    def process_date_batch(self, batch_dates: List[str], batch_num: int, total_batches: int) -> bool:
        """
        Process a single date batch

        Args:
            batch_dates: List of dates in this batch
            batch_num: Current batch number (1-indexed)
            total_batches: Total number of batches

        Returns:
            True if successful, False otherwise
        """
        start_date = batch_dates[0]
        end_date = batch_dates[-1]

        print(f"\n[Batch {batch_num}/{total_batches}] Processing {start_date} to {end_date} ({len(batch_dates)} days)")

        # Get all bonds that have trades in this date range
        bonds = self.get_bonds_in_date_range(start_date, end_date)
        print(f"  Found {len(bonds)} bonds with trades in this period")

        if not bonds:
            print(f"  ⚠️ No bonds found for this date range, skipping")
            return True

        # Calculate market_amount for all bonds in this batch
        all_updates = []

        for bond_code in tqdm(bonds, desc=f"  Calculating batch {batch_num}", unit="bond", leave=False):
            updates = self.calculate_market_amount_for_bond_dates(bond_code, batch_dates)
            all_updates.extend(updates)
            self.stats['bonds_processed'] += 1

        print(f"  Calculated {len(all_updates)} records")

        # Bulk update via RPC
        if all_updates:
            result = self.bulk_update_via_rpc(all_updates)
            self.stats['total_records_updated'] += result['updated_count']
            self.stats['total_records_skipped'] += result['skipped_count']
            self.stats['total_errors'] += result['error_count']

            print(f"  ✓ Updated: {result['updated_count']}, Skipped: {result['skipped_count']}, Errors: {result['error_count']}")

        self.stats['batches_processed'] += 1
        return True

    def process_all_batches(self, dry_run: bool = True):
        """
        Process all date batches

        Args:
            dry_run: If True, calculate but don't update database
        """
        print("\n" + "=" * 70)
        print("MARKET AMOUNT CALCULATION - 15-DAY BATCH PROCESSOR")
        print("=" * 70)
        print(f"Mode: {'DRY RUN' if dry_run else 'PRODUCTION'}")
        print(f"Batch size: {self.batch_days} days")

        # Get all trade dates
        all_dates = self.get_all_trade_dates()
        if not all_dates:
            print("❌ No trade dates found, exiting")
            return

        # Create date batches
        batches = self.create_date_batches(all_dates)

        # Process each batch
        print(f"\n{'=' * 70}")
        print("PROCESSING BATCHES")
        print(f"{'=' * 70}")

        for i, batch_dates in enumerate(batches, 1):
            if dry_run and i > 2:
                print(f"\n[DRY RUN] Stopping after 2 batches for testing")
                break

            success = self.process_date_batch(batch_dates, i, len(batches))
            if not success:
                print(f"❌ Batch {i} failed, stopping")
                break

        # Print final statistics
        self.print_summary()

    def print_summary(self):
        """Print final statistics"""
        print("\n" + "=" * 70)
        print("PROCESSING SUMMARY")
        print("=" * 70)
        print(f"Batches processed: {self.stats['batches_processed']}")
        print(f"Bonds processed: {self.stats['bonds_processed']}")
        print(f"Records updated: {self.stats['total_records_updated']}")
        print(f"Records skipped: {self.stats['total_records_skipped']}")
        print(f"Errors: {self.stats['total_errors']}")
        print("=" * 70)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Calculate market_amount for all bonds using 15-day batch processing'
    )
    parser.add_argument('--mode', choices=['dry-run', 'production'], default='dry-run',
                       help='Execution mode: dry-run (2 batches only) or production (all batches)')
    parser.add_argument('--batch-days', type=int, default=15,
                       help='Number of days per batch (default: 15)')
    parser.add_argument('--force', action='store_true',
                       help='Skip confirmation prompt in production mode')

    args = parser.parse_args()

    dry_run = args.mode == 'dry-run'

    print(f"\n{'🧪 DRY RUN MODE' if dry_run else '🚀 PRODUCTION MODE'}")
    if dry_run:
        print("Will process first 2 batches only for testing")
    else:
        print("⚠️ WARNING: This will update ALL records in the database")
        if not args.force:
            confirm = input("Type 'yes' to confirm: ")
            if confirm.lower() != 'yes':
                print("Aborted")
                return
        else:
            print("--force flag detected, proceeding without confirmation")

    processor = MarketAmountDateBatchProcessor(batch_days=args.batch_days)
    processor.process_all_batches(dry_run=dry_run)


if __name__ == '__main__':
    main()
