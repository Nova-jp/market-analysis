from data.utils.database_manager import DatabaseManager
import logging

def create_tables():
    db = DatabaseManager()
    logger = logging.getLogger(__name__)
    
    # Stock Prices Table
    create_stock_prices_sql = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        trade_date DATE NOT NULL,
        ticker VARCHAR(20) NOT NULL,
        name VARCHAR(100),
        open_price DECIMAL(10, 2),
        high_price DECIMAL(10, 2),
        low_price DECIMAL(10, 2),
        close_price DECIMAL(10, 2),
        volume BIGINT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, ticker)
    );
    """
    
    # Exchange Rates Table
    create_exchange_rates_sql = """
    CREATE TABLE IF NOT EXISTS exchange_rates (
        trade_date DATE NOT NULL,
        currency_pair VARCHAR(10) NOT NULL,
        open_price DECIMAL(10, 4),
        high_price DECIMAL(10, 4),
        low_price DECIMAL(10, 4),
        close_price DECIMAL(10, 4),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, currency_pair)
    );
    """
    
    try:
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Creating stock_prices table...")
                cur.execute(create_stock_prices_sql)
                
                logger.info("Creating exchange_rates table...")
                cur.execute(create_exchange_rates_sql)
            conn.commit()
        logger.info("Tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_tables()
