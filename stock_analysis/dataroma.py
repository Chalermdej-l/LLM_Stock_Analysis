import logging
import pandas as pd

from stock_analysis.settings import SQL_VARS, load_env
from stock_analysis.helper.sql_processor import CloudSQLDatabase
from stock_analysis.helper.dataroma_processor import DataromaScraper

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_table(sql_helper: CloudSQLDatabase, table_name: str, df: pd.DataFrame) -> None:
    """Create or update a table with the given DataFrame."""
    try:
        sql_helper.create_table(table_name, df.dtypes)
        sql_helper.insert_data(table_name, df)
        logger.info(f"Successfully updated table: {table_name}")
    except Exception as e:
        logger.error(f"Error updating table {table_name}: {str(e)}")

def main():
    try:
        env_vars = load_env(SQL_VARS)
        sql_helper = CloudSQLDatabase(
            env_vars['SQL_USER'],
            env_vars['SQL_PASSWORD'],
            env_vars['SQL_HOST'],
            env_vars['SQL_PORT'],
            env_vars['SQL_DATABASE'],
            logger=logger
        )
        
        scraper = DataromaScraper()
        
        # Scrape data
        path_url = '/m/ins/ins.php?t=w&po=1&am=10000&sym=&o=fd&d=d&L=1'
        df_insider_buy = scraper.scrape_insider_buy_data(path_url)
        df_insider_buy_home, df_bigbets, df_low, df_insider_super = scraper.scrape_home_data()
        
        # Define tables to update
        tables_to_update = [
            ('dataroma_screen_insider', df_insider_buy_home),
            ('dataroma_insider_buy', df_insider_buy),
            ('dataroma_bigbets', df_bigbets),
            ('dataroma_low', df_low),
            ('dataroma_insider_super', df_insider_super)
        ]
        
        # Update tables
        for table_name, df in tables_to_update:
            update_table(sql_helper, table_name, df)
        
        logger.info("All tables updated successfully")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()