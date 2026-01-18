#!/usr/bin/env python3
"""
Migrate data from irs_raw to irs_data
"""
import sys
import os
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data.utils.database_manager import DatabaseManager

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def migrate_data():
    logger.info("Starting migration from irs_raw to irs_data...")
    db = DatabaseManager()

    # 1. Fetch data from irs_raw
    logger.info("Fetching data from irs_raw...")
    # Explicitly select columns to ensure order
    query = "SELECT trade_date, rate_type, tenor, rate FROM irs_raw"
    raw_data_tuples = db.execute_query(query)

    if not raw_data_tuples:
        logger.info("No data found in irs_raw.")
        return

    logger.info(f"Fetched {len(raw_data_tuples)} records from irs_raw.")

    # 2. Transform data
    transformed_data = []
    for row in raw_data_tuples:
        trade_date, rate_type, tenor, rate = row
        
        # Map rate_type to product_type
        # TONA -> OIS
        product_type = 'OIS' if rate_type == 'TONA' else rate_type

        transformed_row = {
            'trade_date': trade_date,
            'product_type': product_type,
            'tenor': tenor,
            'rate': float(rate) if rate is not None else None,
            'unit': '%'  # Default to %
        }
        
        if transformed_row['rate'] is not None:
            transformed_data.append(transformed_row)

    # 3. Insert into irs_data
    logger.info(f"Inserting {len(transformed_data)} records into irs_data...")
    
    # Use batch_insert_data which handles batching and "ON CONFLICT DO NOTHING"
    inserted_count = db.batch_insert_data(
        data_list=transformed_data,
        table_name='irs_data',
        batch_size=500
    )

    logger.info(f"Migration completed. Total records processed: {len(transformed_data)}")
    # Note: batch_insert_data returns len(data_list) even if some were skipped due to conflicts,
    # because it executes the batch. It doesn't report exact inserted count distinct from skipped.

if __name__ == "__main__":
    migrate_data()
