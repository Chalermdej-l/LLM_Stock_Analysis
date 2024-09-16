import logging
from typing import Dict
import os

from dotenv import load_dotenv

from helper.sql_processor import CloudSQLDatabase
from helper.magic_processor import MagicFormulaInvesting

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_environment_variables() -> Dict[str, str]:
    """
    Load and return required environment variables from the .env file.

    Raises:
        ValueError: If any required environment variables are missing.

    Returns:
        Dict[str, str]: A dictionary of environment variables.
    """
    load_dotenv('./.env')
    required_vars = ['SQL_DATABASE', 'SQL_USER', 'SQL_PASSWORD', 'SQL_PORT', 'SQL_HOST', 'MAGIC_USER', 'MAGIC_PW']
    env_vars = {var: os.getenv(var) for var in required_vars}
    
    missing_vars = [var for var, value in env_vars.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return env_vars

def main():
    """
    Main function to execute the logic of loading environment variables,
    fetching stock screening data, and inserting it into a Cloud SQL database.
    """
    try:
        # Load environment variables
        env_vars = load_environment_variables()

        # Initialize SQL helper
        sql_helper = CloudSQLDatabase(
            env_vars['SQL_USER'],
            env_vars['SQL_PASSWORD'],
            env_vars['SQL_HOST'],
            env_vars['SQL_PORT'],
            env_vars['SQL_DATABASE'],
            logger=logger
        )

        # Fetch stock screening data using MagicFormulaInvesting
        email = env_vars['MAGIC_USER']
        password = env_vars['MAGIC_PW']
        mfi = MagicFormulaInvesting(email, password)
        stock_df = mfi.get_stock_screening()

        # Define the table name
        table_name = 'magic_screen'

        # Create table and insert data
        try:
            sql_helper.create_table(table_name, stock_df.dtypes)
            sql_helper.insert_data(table_name, stock_df)
        except Exception as e:
            logger.error(f"Error inserting data into SQL: {str(e)}")
            raise
        finally:
            sql_helper.close_connection()
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
