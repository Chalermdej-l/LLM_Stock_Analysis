import logging
import os
from typing import Dict

from dotenv import load_dotenv

from helper.sql_processor import CloudSQLDatabase
from helper.finviz_processor import FinvizScraper

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_environment_variables() -> Dict[str, str]:
    """
    Load and return required environment variables.
    
    Returns:
        env_vars (Dict[str, str]): A dictionary containing the required environment variables.
    
    Raises:
        ValueError: If any required environment variables are missing.
    """
    load_dotenv('./.env')
    required_vars = ['SQL_DATABASE', 'SQL_USER', 'SQL_PASSWORD', 'SQL_PORT', 'SQL_HOST']
    env_vars = {var: os.getenv(var) for var in required_vars}
    
    missing_vars = [var for var, value in env_vars.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return env_vars

def main():
    """
    Main function to load environment variables, initialize SQL helper and Finviz scraper, and update the database.
    """
    try:
        # Load environment variables
        env_vars = load_environment_variables()
        
        # Initialize CloudSQLDatabase
        sql_helper = CloudSQLDatabase(
            env_vars['SQL_USER'],
            env_vars['SQL_PASSWORD'],
            env_vars['SQL_HOST'],
            env_vars['SQL_PORT'],
            env_vars['SQL_DATABASE']
        )
        
        # Finviz scraper URL
        url = ('https://finviz.com/screener.ashx?v=151&f=cap_microover,fa_curratio_o2,'
               'fa_eps5years_o5,fa_opermargin_o10,fa_roe_pos,fa_sales5years_o5,geo_usa,'
               'sh_insiderown_o10,sh_insidertrans_neg,sh_outstanding_o1,sh_price_o4,'
               'ta_highlow52w_b30h&ft=4&o=change')
        
        # Initialize FinvizScraper and fetch data
        scraper = FinvizScraper(url)
        scraper.fetch_data()
        
        # Table name for SQL database
        table_name = 'finviz_screen'
        
        # Create table and insert data
        sql_helper.create_table(table_name, scraper.df.dtypes)
        sql_helper.insert_data(table_name, scraper.df)
        
        logger.info("All tables updated successfully")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
