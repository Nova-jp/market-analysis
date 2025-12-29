#!/usr/bin/env python3
"""
Forward Fill Utility

Implements forward-fill logic for sparse BOJ holdings data.
BOJ data is typically updated monthly, but we need values for all trade dates.
"""

from typing import List, Dict, Any


def forward_fill_boj_holdings(
    boj_holdings: List[Dict[str, Any]],
    all_trade_dates: List[str]
) -> Dict[str, float]:
    """
    Forward-fill BOJ holdings data to cover all trade dates

    Args:
        boj_holdings: List of BOJ holdings records with data_date and face_value
                     Must be sorted by data_date ascending
        all_trade_dates: List of all trade dates (sorted ascending)

    Returns:
        Dictionary mapping each trade_date to BOJ holdings (face_value)
        Uses forward-fill logic: each trade date gets the most recent
        BOJ holdings value on or before that date

    Logic:
        - Before first BOJ data date: 0.0 (no holdings yet)
        - On BOJ data date: actual face_value
        - Between BOJ updates: forward-fill (use last known value)

    Example:
        boj_holdings = [
            {'data_date': '2023-01-15', 'face_value': 5000},
            {'data_date': '2023-02-15', 'face_value': 6000}
        ]
        all_trade_dates = ['2023-01-10', '2023-01-15', '2023-01-20', '2023-02-10', '2023-02-15', '2023-02-20']

        Result: {
            '2023-01-10': 0.0,      # Before first BOJ data
            '2023-01-15': 5000.0,   # On BOJ data date
            '2023-01-20': 5000.0,   # Forward-fill
            '2023-02-10': 5000.0,   # Forward-fill
            '2023-02-15': 6000.0,   # On BOJ data date
            '2023-02-20': 6000.0    # Forward-fill
        }
    """
    result = {}
    current_holdings = 0.0  # Default to 0 before any BOJ data

    # Convert BOJ holdings to dictionary for efficient lookup
    boj_by_date = {record['data_date']: float(record['face_value']) for record in boj_holdings}

    # Sort BOJ data dates
    sorted_boj_dates = sorted(boj_by_date.keys())
    boj_idx = 0

    for trade_date in all_trade_dates:
        # Update current_holdings if we've passed any BOJ data dates
        while boj_idx < len(sorted_boj_dates) and sorted_boj_dates[boj_idx] <= trade_date:
            current_holdings = boj_by_date[sorted_boj_dates[boj_idx]]
            boj_idx += 1

        result[trade_date] = current_holdings

    return result


def forward_fill_generic(
    data_points: List[Dict[str, Any]],
    all_dates: List[str],
    date_key: str = 'data_date',
    value_key: str = 'value',
    default_value: float = 0.0
) -> Dict[str, float]:
    """
    Generic forward-fill function for any time series data

    Args:
        data_points: List of data records with date and value
        all_dates: List of all dates to fill (sorted ascending)
        date_key: Key name for date field in data_points
        value_key: Key name for value field in data_points
        default_value: Default value before first data point

    Returns:
        Dictionary mapping each date to forward-filled value
    """
    result = {}
    current_value = default_value

    # Convert to dictionary
    data_by_date = {record[date_key]: float(record[value_key]) for record in data_points}

    # Sort data dates
    sorted_data_dates = sorted(data_by_date.keys())
    data_idx = 0

    for date in all_dates:
        # Update current_value if we've passed any data dates
        while data_idx < len(sorted_data_dates) and sorted_data_dates[data_idx] <= date:
            current_value = data_by_date[sorted_data_dates[data_idx]]
            data_idx += 1

        result[date] = current_value

    return result


def validate_forward_fill_result(
    result: Dict[str, float],
    expected_dates: List[str]
) -> bool:
    """
    Validate that forward-fill result covers all expected dates

    Args:
        result: Forward-fill result dictionary
        expected_dates: List of dates that should be covered

    Returns:
        True if all expected dates are present, False otherwise
    """
    result_dates = set(result.keys())
    expected_set = set(expected_dates)

    missing_dates = expected_set - result_dates
    extra_dates = result_dates - expected_set

    if missing_dates:
        print(f"  ⚠️ Missing dates in forward-fill result: {len(missing_dates)} dates")
        return False

    if extra_dates:
        print(f"  ⚠️ Extra dates in forward-fill result: {len(extra_dates)} dates")
        return False

    return True


def get_forward_fill_statistics(result: Dict[str, float]) -> Dict[str, Any]:
    """
    Get statistics about forward-filled data

    Args:
        result: Forward-fill result dictionary

    Returns:
        Dictionary with statistics:
        - min_value: Minimum value
        - max_value: Maximum value
        - unique_values: Number of unique values (indicates update frequency)
        - dates_count: Total number of dates
    """
    values = list(result.values())

    return {
        'min_value': min(values) if values else 0,
        'max_value': max(values) if values else 0,
        'unique_values': len(set(values)),
        'dates_count': len(result),
        'zero_dates': sum(1 for v in values if v == 0),
        'non_zero_dates': sum(1 for v in values if v != 0)
    }
