#!/usr/bin/env python3
"""
Market Amount Validator

Validates market_amount calculation accuracy by recalculating samples
and comparing with database values.
"""

import os
import sys
import random
import requests
from typing import List, Dict, Any
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.helpers.bond_data_fetcher import BondDataFetcher
from scripts.helpers.cumulative_calculator import calculate_cumulative_by_date, expand_cumulative_to_all_dates
from scripts.helpers.forward_fill import forward_fill_boj_holdings

load_dotenv()


class MarketAmountValidator:
    """
    Validates market_amount calculation accuracy

    Performs sample verification by:
    1. Selecting random bonds
    2. Recalculating market_amount for each date
    3. Comparing with database values
    4. Reporting mismatches
    """

    def __init__(self):
        """Initialize validator"""
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment")

        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json'
        }

        self.fetcher = BondDataFetcher(self.supabase_url, self.supabase_key)

    def validate_sample(self, sample_size: int = 20) -> Dict[str, Any]:
        """
        Validate market_amount for random sample of bonds

        Args:
            sample_size: Number of bonds to validate (default: 20)

        Returns:
            Dictionary with validation results:
            {
                'total_checked': int,
                'total_records_checked': int,
                'matches': int,
                'mismatches': int,
                'mismatch_details': List[Dict],
                'bonds_checked': List[str]
            }
        """
        print("\n" + "=" * 70)
        print("MARKET AMOUNT VALIDATION - SAMPLE VERIFICATION")
        print("=" * 70)

        # Get all bond codes
        print(f"\nFetching bond codes...")
        all_bond_codes = self.fetcher.get_all_bond_codes()
        print(f"✓ Total bonds available: {len(all_bond_codes)}")

        # Select random sample
        if len(all_bond_codes) < sample_size:
            sample_size = len(all_bond_codes)
            print(f"⚠️ Only {sample_size} bonds available, using all")

        sample_bonds = random.sample(all_bond_codes, sample_size)
        print(f"✓ Selected {sample_size} random bonds for validation")

        # Validation results
        results = {
            'total_checked': sample_size,
            'total_records_checked': 0,
            'matches': 0,
            'mismatches': 0,
            'mismatch_details': [],
            'bonds_checked': sample_bonds
        }

        # Validate each bond
        print(f"\nValidating {sample_size} bonds...")
        for idx, bond_code in enumerate(sample_bonds, 1):
            print(f"\n[{idx}/{sample_size}] Validating bond {bond_code}...")

            bond_result = self._validate_single_bond(bond_code)

            results['total_records_checked'] += bond_result['records_checked']

            if bond_result['all_match']:
                results['matches'] += 1
                print(f"  ✅ All {bond_result['records_checked']} records match")
            else:
                results['mismatches'] += 1
                results['mismatch_details'].append({
                    'bond_code': bond_code,
                    'records_checked': bond_result['records_checked'],
                    'mismatched_records': bond_result['mismatch_count'],
                    'sample_mismatches': bond_result['mismatches'][:5]  # First 5 mismatches
                })
                print(f"  ❌ {bond_result['mismatch_count']}/{bond_result['records_checked']} records mismatch")

        # Print summary
        self._print_validation_summary(results)

        return results

    def _validate_single_bond(self, bond_code: str) -> Dict[str, Any]:
        """
        Validate market_amount for a single bond

        Args:
            bond_code: 9-digit bond code

        Returns:
            Dictionary with validation result for this bond
        """
        try:
            # 1. Fetch auction data and calculate cumulative issuance
            auctions = self.fetcher.fetch_auctions(bond_code)
            if not auctions:
                return {
                    'all_match': False,
                    'records_checked': 0,
                    'mismatch_count': 0,
                    'mismatches': [],
                    'error': 'No auction data'
                }

            # 2. Fetch BOJ holdings
            boj_holdings = self.fetcher.fetch_boj_holdings(bond_code)

            # 3. Get all trade dates
            trade_dates = self.fetcher.fetch_trade_dates(bond_code)
            if not trade_dates:
                return {
                    'all_match': False,
                    'records_checked': 0,
                    'mismatch_count': 0,
                    'mismatches': [],
                    'error': 'No trade dates'
                }

            # 4. Calculate cumulative issuance
            cumulative_by_auction_date = calculate_cumulative_by_date(auctions)
            cumulative_by_trade_date = expand_cumulative_to_all_dates(
                cumulative_by_auction_date,
                trade_dates
            )

            # 5. Forward-fill BOJ holdings
            boj_by_trade_date = forward_fill_boj_holdings(boj_holdings, trade_dates)

            # 6. Calculate expected market_amount for each date
            expected_values = {}
            for trade_date in trade_dates:
                cumulative = cumulative_by_trade_date.get(trade_date, 0.0)
                boj = boj_by_trade_date.get(trade_date, 0.0)
                expected_values[trade_date] = round(cumulative - boj, 2)

            # 7. Fetch actual market_amount from database
            actual_values = self._fetch_market_amounts_from_db(bond_code, trade_dates)

            # 8. Compare expected vs actual
            mismatches = []
            for trade_date in trade_dates:
                expected = expected_values.get(trade_date)
                actual = actual_values.get(trade_date)

                if actual is None:
                    mismatches.append({
                        'trade_date': trade_date,
                        'expected': expected,
                        'actual': None,
                        'difference': None,
                        'error': 'Missing in database'
                    })
                else:
                    diff = abs(expected - actual)
                    # Allow small rounding errors (< 0.01)
                    if diff >= 0.01:
                        mismatches.append({
                            'trade_date': trade_date,
                            'expected': expected,
                            'actual': actual,
                            'difference': diff
                        })

            return {
                'all_match': len(mismatches) == 0,
                'records_checked': len(trade_dates),
                'mismatch_count': len(mismatches),
                'mismatches': mismatches
            }

        except Exception as e:
            return {
                'all_match': False,
                'records_checked': 0,
                'mismatch_count': 0,
                'mismatches': [],
                'error': str(e)
            }

    def _fetch_market_amounts_from_db(self, bond_code: str, trade_dates: List[str]) -> Dict[str, float]:
        """
        Fetch actual market_amount values from database

        Args:
            bond_code: 9-digit bond code
            trade_dates: List of trade dates to fetch

        Returns:
            Dictionary mapping trade_date to market_amount
        """
        try:
            # Fetch bond_data for this bond
            response = requests.get(
                f'{self.supabase_url}/rest/v1/bond_data',
                params={
                    'bond_code': f'eq.{bond_code}',
                    'select': 'trade_date,market_amount'
                },
                headers=self.headers,
                timeout=30
            )

            if response.status_code != 200:
                return {}

            data = response.json()

            # Build dictionary
            result = {}
            for record in data:
                trade_date = record['trade_date']
                market_amount = record.get('market_amount')
                if market_amount is not None:
                    result[trade_date] = float(market_amount)

            return result

        except Exception as e:
            print(f"    ⚠️ Error fetching market_amounts from DB: {e}")
            return {}

    def _print_validation_summary(self, results: Dict[str, Any]):
        """Print validation summary report"""
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        print(f"Total bonds checked: {results['total_checked']}")
        print(f"Total records checked: {results['total_records_checked']}")
        print(f"Bonds with all matching: {results['matches']} ✅")
        print(f"Bonds with mismatches: {results['mismatches']} ❌")

        if results['mismatches'] > 0:
            print(f"\n{'-' * 70}")
            print("MISMATCH DETAILS")
            print(f"{'-' * 70}")
            for detail in results['mismatch_details']:
                print(f"\nBond: {detail['bond_code']}")
                print(f"  Total records: {detail['records_checked']}")
                print(f"  Mismatched: {detail['mismatched_records']}")
                print(f"  Sample mismatches:")
                for mm in detail['sample_mismatches']:
                    print(f"    Date: {mm['trade_date']}")
                    print(f"      Expected: {mm.get('expected', 'N/A')}")
                    print(f"      Actual: {mm.get('actual', 'N/A')}")
                    print(f"      Difference: {mm.get('difference', 'N/A')}")

        print("\n" + "=" * 70)

        if results['mismatches'] == 0:
            print("✅ VALIDATION PASSED - All samples match!")
        else:
            mismatch_rate = (results['mismatches'] / results['total_checked']) * 100
            print(f"⚠️ VALIDATION ISSUES - {mismatch_rate:.1f}% of bonds have mismatches")

        print("=" * 70)


def main():
    """Main entry point for standalone validation"""
    import argparse

    parser = argparse.ArgumentParser(description='Market Amount Sample Validator')
    parser.add_argument('--sample-size', type=int, default=20,
                       help='Number of bonds to validate (default: 20)')

    args = parser.parse_args()

    validator = MarketAmountValidator()
    results = validator.validate_sample(sample_size=args.sample_size)

    # Exit with error code if mismatches found
    if results['mismatches'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
