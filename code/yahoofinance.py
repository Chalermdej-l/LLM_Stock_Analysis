import json
import logging
from typing import Dict
from dotenv import load_dotenv
import os
import sys
from helper.yahoo_processor import StockData
from helper.sql_processor import CloudSQLDatabase

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_environment_variables() -> Dict[str, str]:
    """Load and return environment variables."""
    load_dotenv('./.env')
    return {
        'sql_database': os.getenv('SQL_DATABASE'),
        'sql_user': os.getenv('SQL_USER'),
        'sql_password': os.getenv('SQL_PASSWORD'),
        'sql_port': os.getenv('SQL_PORT'),
        'sql_host': os.getenv('SQL_HOST')
    }

def main(stock_symbol_list: list):
    """
    Main function to load environment variables, initialize SQL helper and Finviz scraper, and update the database.
    """
    try:
        # Load environment variables
        env_vars = load_environment_variables()
        logger.info("Load credential")
        # Initialize CloudSQLDatabase
        sql_helper = CloudSQLDatabase(
            env_vars['sql_user'],
            env_vars['sql_password'],
            env_vars['sql_host'],
            env_vars['sql_port'],
            env_vars['sql_database'],
            big_flag=True
        )

        # Set to track created tables
        created_tables = set()
        
        for stock_symbol in stock_symbol_list:
            # Fetch and update stock data
            stock_data = StockData(stock_symbol)
            all_data = stock_data.fetch_all_data()
            logger.info('Process data for {}'.format(stock_symbol))

            for key, df in all_data.items():
                if not df.empty:
                    table_name = 'yahoofinance_' + key
                    if table_name not in created_tables:
                        sql_helper.create_table(table_name, df.dtypes)
                        created_tables.add(table_name)
                    sql_helper.update_table_schema(table_name, df)
                    sql_helper.insert_data(table_name, df)
                    
            logger.info(f"Tables for {stock_symbol} updated successfully")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python script.py <stock_symbol_1> [<stock_symbol_2> ...]")
        sys.exit(1)
    
    stock_symbols = sys.argv[1:]
    main(stock_symbols)
