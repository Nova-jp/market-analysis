import sys
import os
import argparse
from pathlib import Path
from datetime import datetime, date
import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import logging
import json
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from app.core.config import settings
from analysis.finance.quantlib_helper import QuantLibHelper

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Use a single engine for the main process
_engine = None

def get_db_engine():
    global _engine
    if _engine is None:
        # Create engine with a small pool for individual worker processes
        _engine = create_engine(settings.database_url, pool_size=5, max_overflow=10)
    return _engine

def get_pending_dates(engine, force=False):
    """
    Get list of dates that exist in bond_data but not in ASW_data (unless force=True)
    """
    with engine.connect() as conn:
        if force:
            query = text("SELECT DISTINCT trade_date FROM bond_data ORDER BY trade_date DESC")
            result = conn.execute(query)
            return [row[0] for row in result]
        else:
            # Only dates not in ASW_data
            query = text("""
                SELECT DISTINCT trade_date 
                FROM bond_data 
                WHERE trade_date NOT IN (SELECT DISTINCT trade_date FROM "ASW_data")
                ORDER BY trade_date DESC
            """)
            result = conn.execute(query)
            return [row[0] for row in result]

def fetch_ois_data(conn, trade_date):
    """
    Fetch OIS data for a specific date.
    """
    query = text("""
        SELECT tenor, rate 
        FROM irs_data 
        WHERE trade_date = :date AND product_type = 'OIS'
    """)
    result = conn.execute(query, {"date": trade_date})
    return [{'tenor': row[0], 'rate': float(row[1])} for row in result]

def fetch_bond_data(conn, trade_date):
    """
    Fetch bond data for a specific date.
    """
    query = text("""
        SELECT bond_code, due_date, ave_compound_yield
        FROM bond_data
        WHERE trade_date = :date AND ave_compound_yield IS NOT NULL
    """)
    result = conn.execute(query, {"date": trade_date})
    return [dict(row._mapping) for row in result]

def save_batch(conn, data):
    """
    Save calculated ASW data to ASW_data table.
    """
    if not data:
        return

    stmt = text("""
        INSERT INTO "ASW_data" (
            trade_date, bond_code, 
            asw_act365_pa, asw_act365_sa, calculation_log
        ) VALUES (
            :trade_date, :bond_code, 
            :asw_act365_pa, :asw_act365_sa, :calculation_log
        )
        ON CONFLICT (trade_date, bond_code) DO UPDATE SET
            asw_act365_pa = EXCLUDED.asw_act365_pa,
            asw_act365_sa = EXCLUDED.asw_act365_sa,
            calculation_log = EXCLUDED.calculation_log,
            updated_at = CURRENT_TIMESTAMP
    """)
    
    conn.execute(stmt, data)
    conn.commit()

def process_date_worker(trade_date):
    """
    Worker function to process a single date.
    """
    engine = create_engine(settings.database_url)
    date_str = trade_date.strftime("%Y-%m-%d")
    
    try:
        with engine.connect() as conn:
            ois_data = fetch_ois_data(conn, trade_date)
            if len(ois_data) < 4:
                return f"{date_str}: Insufficient OIS data"

            bond_data = fetch_bond_data(conn, trade_date)
            if not bond_data:
                return f"{date_str}: No bond data"

            # Initialize QuantLib Helper
            ql_helper = QuantLibHelper(date_str)
            ql_helper.build_ois_curve(ois_data)

            results = []
            for bond in bond_data:
                bond_yield = float(bond['ave_compound_yield'])
                maturity_date = bond['due_date']
                maturity_str = maturity_date.strftime("%Y-%m-%d")
                
                try:
                    mms_pa = ql_helper.calculate_mms(maturity_str, 'Annual', 'Act365')
                    mms_sa = ql_helper.calculate_mms(maturity_str, 'Semiannual', 'Act365')
                    
                    if mms_pa is None or mms_sa is None:
                        continue

                    asw_pa = round(bond_yield - mms_pa, 4)
                    asw_sa = round(bond_yield - mms_sa, 4)
                    
                    results.append({
                        "trade_date": trade_date,
                        "bond_code": bond['bond_code'],
                        "asw_act365_pa": asw_pa,
                        "asw_act365_sa": asw_sa,
                        "calculation_log": json.dumps({"curve_points": len(ois_data)})
                    })
                except:
                    continue

            if results:
                save_batch(conn, results)
                return f"{date_str}: Success ({len(results)} bonds)"
            
            return f"{date_str}: No results"
            
    except Exception as e:
        return f"{date_str}: Error - {str(e)}"
    finally:
        engine.dispose()

def main():
    parser = argparse.ArgumentParser(description="Calculate Historical ASW (Parallel)")
    parser.add_argument("--force", action="store_true", help="Recalculate even if exists")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of days to process")
    parser.add_argument("--date", type=str, help="Process specific date (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=multiprocessing.cpu_count(), help="Number of parallel workers")
    args = parser.parse_args()

    engine = get_db_engine()

    if args.date:
        dates = [datetime.strptime(args.date, "%Y-%m-%d").date()]
    else:
        dates = get_pending_dates(engine, force=args.force)

    if args.limit:
        dates = dates[:args.limit]

    logger.info(f"Found {len(dates)} dates to process using {args.workers} workers.")

    if len(dates) == 1:
        # Sequential processing for single date or debugging
        res = process_date_worker(dates[0])
        logger.info(res)
    else:
        # Parallel processing
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            list(tqdm(executor.map(process_date_worker, dates), total=len(dates)))

if __name__ == "__main__":
    main()
