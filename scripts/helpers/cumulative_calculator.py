#!/usr/bin/env python3
"""
Cumulative Calculator

Calculates cumulative issuance amounts for bonds based on auction data.
Provides functions to compute cumulative sums by date.
"""

from typing import List, Dict, Any
from datetime import datetime
from collections import defaultdict


def calculate_cumulative_by_date(auctions: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Calculate cumulative issuance amount for each auction date

    Args:
        auctions: List of auction records with auction_date and total_amount
                 Must be sorted by auction_date ascending

    Returns:
        Dictionary mapping auction_date (str) to cumulative amount (float)

    Example:
        auctions = [
            {'auction_date': '2023-01-10', 'total_amount': 10000},
            {'auction_date': '2023-02-10', 'total_amount': 12000},
            {'auction_date': '2023-03-10', 'total_amount': 11000}
        ]
        Result: {
            '2023-01-10': 10000.0,
            '2023-02-10': 22000.0,
            '2023-03-10': 33000.0
        }
    """
    cumulative_by_date = {}
    cumulative_sum = 0.0

    for auction in auctions:
        auction_date = auction['auction_date']
        total_amount = float(auction['total_amount'])

        cumulative_sum += total_amount
        cumulative_by_date[auction_date] = cumulative_sum

    return cumulative_by_date


def get_cumulative_for_trade_date(
    trade_date: str,
    cumulative_by_date: Dict[str, float]
) -> float:
    """
    Get cumulative issuance amount as of a specific trade date

    Args:
        trade_date: Trade date in YYYY-MM-DD format
        cumulative_by_date: Dictionary from calculate_cumulative_by_date()

    Returns:
        Cumulative amount as of trade_date (0.0 if no auctions before this date)

    Logic:
        Returns the cumulative sum of all auctions with auction_date <= trade_date
    """
    cumulative = 0.0

    # Find the latest auction date that is <= trade_date
    for auction_date, amount in cumulative_by_date.items():
        if auction_date <= trade_date:
            cumulative = amount
        else:
            # Since cumulative_by_date should be ordered, we can break here
            break

    return cumulative


def expand_cumulative_to_all_dates(
    cumulative_by_date: Dict[str, float],
    all_trade_dates: List[str]
) -> Dict[str, float]:
    """
    Expand cumulative issuance data to cover all trade dates

    Args:
        cumulative_by_date: Dictionary from calculate_cumulative_by_date()
        all_trade_dates: List of all trade dates (sorted ascending)

    Returns:
        Dictionary mapping each trade_date to cumulative issuance amount
        Uses forward-fill logic: each trade date gets the cumulative amount
        from the most recent auction date on or before that trade date

    Example:
        cumulative_by_date = {
            '2023-01-10': 10000.0,
            '2023-02-10': 22000.0
        }
        all_trade_dates = ['2023-01-05', '2023-01-10', '2023-01-15', '2023-02-10', '2023-02-15']

        Result: {
            '2023-01-05': 0.0,      # Before first auction
            '2023-01-10': 10000.0,  # On auction date
            '2023-01-15': 10000.0,  # After auction, forward-fill
            '2023-02-10': 22000.0,  # On second auction date
            '2023-02-15': 22000.0   # After auction, forward-fill
        }
    """
    result = {}
    current_cumulative = 0.0

    # Sort auction dates for efficient lookup
    sorted_auction_dates = sorted(cumulative_by_date.keys())
    auction_idx = 0

    for trade_date in all_trade_dates:
        # Update current_cumulative if we've passed any auction dates
        while auction_idx < len(sorted_auction_dates) and sorted_auction_dates[auction_idx] <= trade_date:
            current_cumulative = cumulative_by_date[sorted_auction_dates[auction_idx]]
            auction_idx += 1

        result[trade_date] = current_cumulative

    return result
