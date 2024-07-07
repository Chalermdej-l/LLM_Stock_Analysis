import json
import logging
from typing import Dict

from dotenv import load_dotenv
import os
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

def main():
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
        
            # Example usage
        ticker = 'MSFT'
        stock_data = StockData(ticker)
        all_data = stock_data.fetch_all_data()

        for key, df in all_data.items():

            if not(df.empty):
                
                # Table name for SQL database
                table_name = 'yahoofinance_' + key
                # Create table and insert data
                sql_helper.create_table(table_name, df.dtypes)
                sql_helper.insert_data(table_name, df)
                
        logger.info("All tables updated successfully")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()