#!/usr/bin/env python3
"""
Bond-by-Bond Market Amount Calculator

Recalculates market_amount for all bonds using bond-by-bond approach.
This fixes gaps/jumps caused by date-range batch processing.

Formula: market_amount = cumulative_issuance - BOJ_holdings (with forward-fill)

Usage:
    # Dry run (no updates)
    python scripts/calculate_market_amount_by_bond.py --dry-run

    # Single bond test
    python scripts/calculate_market_amount_by_bond.py --bond-code 003720067

    # Production mode (updates database)
    python scripts/calculate_market_amount_by_bond.py --mode production
"""

import os
import sys
import argparse
import requests
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv
from tqdm import tqdm

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.helpers.bond_data_fetcher import BondDataFetcher
from scripts.helpers.cumulative_calculator import calculate_cumulative_by_date, expand_cumulative_to_all_dates
from scripts.helpers.forward_fill import forward_fill_boj_holdings, get_forward_fill_statistics
from scripts.helpers.bulk_updater import BulkMarketAmountUpdater

load_dotenv()


class MarketAmountCalculator:
    """
    Calculator for bond-by-bond market_amount recalculation
    """

    def __init__(self, dry_run: bool = True):
        """
        Initialize calculator

        Args:
            dry_run: If True, don't actually update database (default: True)
        """
        self.dry_run = dry_run
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment")

        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'  # Don't return updated records
        }

        self.fetcher = BondDataFetcher(self.supabase_url, self.supabase_key)
        self.bulk_updater = BulkMarketAmountUpdater(self.supabase_url, self.supabase_key)

        # Statistics
        self.stats = {
            'bonds_processed': 0,
            'bonds_skipped': 0,
            'records_updated': 0,
            'errors': 0
        }

    def calculate_market_amount_for_bond(self, bond_code: str) -> Dict[str, Any]:
        """
        Calculate market_amount for all dates for a single bond

        Args:
            bond_code: 9-digit bond code

        Returns:
            Dictionary with results:
            - success: bool
            - bond_code: str
            - records_updated: int
            - error: str (if failed)
        """
        try:
            print(f"\n{'='*70}")
            print(f"Processing bond: {bond_code}")
            print(f"{'='*70}")

            # 1. Fetch all auction data and calculate cumulative issuance
            print("  1. Fetching auction data...")
            auctions = self.fetcher.fetch_auctions(bond_code)
            if not auctions:
                print(f"  ⚠️ No auction data found for {bond_code}")
                return {'success': False, 'bond_code': bond_code, 'error': 'No auction data'}

            print(f"     ✓ Found {len(auctions)} auction records")

            # 2. Fetch BOJ holdings
            print("  2. Fetching BOJ holdings...")
            boj_holdings = self.fetcher.fetch_boj_holdings(bond_code)
            print(f"     ✓ Found {len(boj_holdings)} BOJ holdings records")

            # 3. Get all trade dates for this bond
            print("  3. Fetching trade dates...")
            trade_dates = self.fetcher.fetch_trade_dates(bond_code)
            if not trade_dates:
                print(f"  ⚠️ No trade dates found for {bond_code}")
                return {'success': False, 'bond_code': bond_code, 'error': 'No trade dates'}

            print(f"     ✓ Found {len(trade_dates)} trade dates")
            print(f"     Date range: {trade_dates[0]} to {trade_dates[-1]}")

            # 4. Calculate cumulative issuance for all dates
            print("  4. Calculating cumulative issuance...")
            cumulative_by_auction_date = calculate_cumulative_by_date(auctions)
            cumulative_by_trade_date = expand_cumulative_to_all_dates(
                cumulative_by_auction_date,
                trade_dates
            )
            print(f"     ✓ Cumulative range: {min(cumulative_by_trade_date.values()):.2f} to {max(cumulative_by_trade_date.values()):.2f}")

            # 5. Forward-fill BOJ holdings to match all trade dates
            print("  5. Forward-filling BOJ holdings...")
            boj_by_trade_date = forward_fill_boj_holdings(boj_holdings, trade_dates)
            boj_stats = get_forward_fill_statistics(boj_by_trade_date)
            print(f"     ✓ BOJ holdings range: {boj_stats['min_value']:.2f} to {boj_stats['max_value']:.2f}")
            print(f"     ✓ Unique BOJ values: {boj_stats['unique_values']} (updates)")

            # 6. Calculate market_amount for each date
            print("  6. Calculating market_amount...")
            updates = []
            for trade_date in trade_dates:
                cumulative = cumulative_by_trade_date.get(trade_date, 0.0)
                boj = boj_by_trade_date.get(trade_date, 0.0)
                market_amount = cumulative - boj

                updates.append({
                    'bond_code': bond_code,
                    'trade_date': trade_date,
                    'market_amount': round(market_amount, 2)
                })

            print(f"     ✓ Calculated {len(updates)} market_amount values")

            # Show sample calculations
            if updates:
                sample = updates[0]
                print(f"     Sample (first date):")
                print(f"       Date: {sample['trade_date']}")
                print(f"       Cumulative: {cumulative_by_trade_date[sample['trade_date']]:.2f}")
                print(f"       BOJ: {boj_by_trade_date[sample['trade_date']]:.2f}")
                print(f"       Market Amount: {sample['market_amount']:.2f}")

            # 7. Batch update bond_data (if not dry run)
            if self.dry_run:
                print("  7. [DRY RUN] Skipping database update")
                return {
                    'success': True,
                    'bond_code': bond_code,
                    'records_updated': len(updates),
                    'dry_run': True
                }
            else:
                print("  7. Updating database...")
                updated_count = self._batch_update_market_amount(updates)
                print(f"     ✓ Updated {updated_count} records")
                return {
                    'success': True,
                    'bond_code': bond_code,
                    'records_updated': updated_count
                }

        except Exception as e:
            print(f"  ❌ Error processing {bond_code}: {e}")
            return {'success': False, 'bond_code': bond_code, 'error': str(e)}

    def _batch_update_market_amount(self, updates: List[Dict[str, Any]], batch_size: int = 500) -> int:
        """
        Batch update market_amount in bond_data table using bulk UPSERT

        Args:
            updates: List of update dictionaries with bond_code, trade_date, market_amount
            batch_size: Number of records to update per batch

        Returns:
            Number of records successfully updated

        Note:
            Now uses BulkMarketAmountUpdater with UPSERT for efficient bulk updates.
            Replaces 500 individual PATCH requests with 1 bulk UPSERT request.
            Expected speedup: 50x faster (25 seconds → 0.5 seconds per 500 records)
        """
        try:
            return self.bulk_updater.bulk_update_via_upsert(updates, batch_size)
        except Exception as e:
            print(f"       ❌ Bulk update error: {e}")
            return 0

    def process_all_bonds(self, bond_codes: List[str] = None):
        """
        Process market_amount calculation for all bonds

        Args:
            bond_codes: Optional list of bond codes to process.
                       If None, processes all bonds in database.
        """
        start_time = datetime.now()

        print("\n" + "=" * 70)
        print("BOND-BY-BOND MARKET AMOUNT RECALCULATION")
        print("=" * 70)
        print(f"Mode: {'DRY RUN (no updates)' if self.dry_run else 'PRODUCTION (will update DB)'}")
        print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        # Get bond codes if not provided
        if bond_codes is None:
            print("\nFetching all bond codes...")
            bond_codes = self.fetcher.get_all_bond_codes()
            print(f"✓ Found {len(bond_codes)} bonds")

        total_bonds = len(bond_codes)

        # Process each bond with progress bar
        with tqdm(total=total_bonds, desc="Processing bonds", unit="bond") as pbar:
            for bond_code in bond_codes:
                result = self.calculate_market_amount_for_bond(bond_code)

                if result['success']:
                    self.stats['bonds_processed'] += 1
                    self.stats['records_updated'] += result.get('records_updated', 0)
                else:
                    self.stats['bonds_skipped'] += 1
                    self.stats['errors'] += 1

                # Update progress bar
                pbar.update(1)
                pbar.set_postfix({
                    'bond': bond_code[:9],
                    'records': result.get('records_updated', 0),
                    'errors': self.stats['errors']
                })

        # Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "=" * 70)
        print("PROCESSING COMPLETE")
        print("=" * 70)
        print(f"Total bonds: {total_bonds}")
        print(f"Bonds processed: {self.stats['bonds_processed']}")
        print(f"Bonds skipped: {self.stats['bonds_skipped']}")
        print(f"Records updated: {self.stats['records_updated']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        if self.stats['bonds_processed'] > 0:
            print(f"Average time per bond: {duration/self.stats['bonds_processed']:.2f} seconds")
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description='Bond-by-bond market_amount calculator')
    parser.add_argument('--dry-run', action='store_true', default=False,
                       help='Dry run mode (no database updates)')
    parser.add_argument('--mode', choices=['dry-run', 'production'], default='dry-run',
                       help='Execution mode: dry-run or production (default: dry-run)')
    parser.add_argument('--bond-code', type=str,
                       help='Process single bond code (9 digits)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of bonds to process (for testing)')
    parser.add_argument('--bond-list-file', type=str,
                       help='File containing list of bond codes (one per line)')
    parser.add_argument('--verify', action='store_true',
                       help='Run sample verification after processing (validates 20 random bonds)')

    args = parser.parse_args()

    # Determine dry_run mode
    # If --mode production is specified, set dry_run to False (unless --dry-run is explicitly set)
    if args.mode == 'production' and not args.dry_run:
        dry_run = False
    else:
        dry_run = True

    # Create calculator
    calculator = MarketAmountCalculator(dry_run=dry_run)

    # Process single bond or all bonds
    if args.bond_code:
        # Single bond mode
        result = calculator.calculate_market_amount_for_bond(args.bond_code)
        if result['success']:
            print(f"\n✅ Successfully processed {args.bond_code}")
            print(f"   Records: {result.get('records_updated', 0)}")
        else:
            print(f"\n❌ Failed to process {args.bond_code}: {result.get('error')}")
    else:
        # All bonds mode
        bond_codes = None

        # Load from file if specified
        if args.bond_list_file:
            print(f"\n📄 Loading bond codes from file: {args.bond_list_file}")
            try:
                with open(args.bond_list_file, 'r') as f:
                    bond_codes = [line.strip() for line in f if line.strip()]
                print(f"✓ Loaded {len(bond_codes)} bond codes from file")
            except Exception as e:
                print(f"❌ Error reading file: {e}")
                return
        elif args.limit:
            # Get limited number of bonds
            fetcher = BondDataFetcher(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
            all_codes = fetcher.get_all_bond_codes()
            bond_codes = all_codes[:args.limit]
            print(f"\n⚠️ LIMIT MODE: Processing only first {args.limit} bonds")

        calculator.process_all_bonds(bond_codes)

        # Run sample verification if requested
        if args.verify and not dry_run:
            print("\n" + "=" * 70)
            print("STARTING SAMPLE VERIFICATION")
            print("=" * 70)
            from scripts.helpers.market_amount_validator import MarketAmountValidator
            validator = MarketAmountValidator()
            results = validator.validate_sample(sample_size=20)


if __name__ == '__main__':
    main()
