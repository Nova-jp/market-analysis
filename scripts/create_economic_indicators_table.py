from data.utils.database_manager import DatabaseManager
import logging

def create_economic_indicators_table():
    db = DatabaseManager()
    logger = logging.getLogger(__name__)
    
    # Economic Indicators Table (CPI, GDP, Unemployment, etc.)
    sql = """
    CREATE TABLE IF NOT EXISTS economic_indicators (
        release_date DATE NOT NULL,
        indicator_code VARCHAR(50) NOT NULL,
        country VARCHAR(50),
        name VARCHAR(100),
        value DECIMAL(18, 6),
        frequency VARCHAR(20), -- 'monthly', 'quarterly', etc.
        source VARCHAR(50), -- 'FRED', 'OECD', etc.
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (release_date, indicator_code)
    );
    """
    
    try:
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Dropping existing economic_indicators table...")
                cur.execute("DROP TABLE IF EXISTS economic_indicators")

                logger.info("Creating economic_indicators table...")
                cur.execute(sql)
            conn.commit()
        logger.info("Table 'economic_indicators' created successfully.")
    except Exception as e:
        logger.error(f"Error creating table: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_economic_indicators_table()
