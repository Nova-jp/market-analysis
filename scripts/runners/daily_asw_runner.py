#!/usr/bin/env python3
"""
Daily ASW Calculation Runner
Checks if bond_data and irs_data are available for a given date and calculates ASW.
"""
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import logging

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from app.core.config import settings
from scripts.calculate_historical_asw import process_date_worker

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(project_root / "logs/daily_asw_runner.log")
    ]
)
logger = logging.getLogger("daily_asw_runner")

def check_data_availability(engine, target_date):
    """Checks if required data exists for the target date."""
    with engine.connect() as conn:
        # Check bond_data
        bond_count = conn.execute(
            text("SELECT count(*) FROM bond_data WHERE trade_date = :date"),
            {"date": target_date}
        ).scalar()
        
        # Check irs_data (OIS)
        ois_count = conn.execute(
            text("SELECT count(*) FROM irs_data WHERE trade_date = :date AND product_type = 'OIS'"),
            {"date": target_date}
        ).scalar()
        
        return bond_count > 0, ois_count >= 4

def main():
    parser = argparse.ArgumentParser(description="Run Daily ASW Calculation")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--force", action="store_true", help="Force recalculation if already exists")
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = datetime.now().date()

    engine = create_engine(settings.database_url)
    
    logger.info(f"Checking data availability for {target_date}...")
    
    has_bond, has_ois = check_data_availability(engine, target_date)
    
    if not has_bond or not has_ois:
        missing = []
        if not has_bond: missing.append("bond_data")
        if not has_ois: missing.append("irs_data (OIS)")
        logger.warning(f"Data not ready for {target_date}. Missing: {', '.join(missing)}. Skipping.")
        sys.exit(0)

    # Check if already calculated
    if not args.force:
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT count(*) FROM \"ASW_data\" WHERE trade_date = :date"),
                {"date": target_date}
            ).scalar()
            if exists > 0:
                logger.info(f"ASW data already exists for {target_date}. Use --force to recalculate. Skipping.")
                sys.exit(0)

    logger.info(f"Starting ASW calculation for {target_date}...")
    result = process_date_worker(target_date)
    logger.info(f"Result: {result}")

if __name__ == "__main__":
    main()
