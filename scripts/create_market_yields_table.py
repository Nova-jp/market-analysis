from data.utils.database_manager import DatabaseManager
import logging

def create_market_yields_table():
    db = DatabaseManager()
    logger = logging.getLogger(__name__)
    
    # Market Yields Table (Constant Maturity Yields for US, JP, EU etc.)
    sql = """
    CREATE TABLE IF NOT EXISTS market_yields (
        trade_date DATE NOT NULL,
        region VARCHAR(10) NOT NULL, -- 'US', 'JP', 'EU'
        instrument_type VARCHAR(20) NOT NULL, -- 'sovereign', 'swap', 'policy'
        tenor VARCHAR(10) NOT NULL, -- '10Y', '2Y', '1M', 'ON' (Overnight)
        yield_value DECIMAL(10, 4),
        source VARCHAR(50),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, region, instrument_type, tenor)
    );
    """
    
    try:
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Creating market_yields_table...")
                cur.execute(sql)
            conn.commit()
        logger.info("Table 'market_yields' created successfully.")
    except Exception as e:
        logger.error(f"Error creating table: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_market_yields_table()
