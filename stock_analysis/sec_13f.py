import json
import logging
from typing import Dict

import pandas as pd
from stock_analysis.settings import SQL_VARS, load_env
from stock_analysis.helper.sec_processor import SecProcessor
from stock_analysis.helper.sql_processor import CloudSQLDatabase

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_cik_list(file_path: str) -> Dict:
    """Load CIK list from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"CIK list file not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in CIK list file: {file_path}")
        raise

def process_sec_data(cik_list: Dict) -> pd.DataFrame:
    """Process SEC data using SecProcessor."""
    processor = SecProcessor(cik_list)
    df = processor.process_all_funds()
    
    # Convert other numeric columns as needed
    numeric_columns = ['value', 'voting_sole', 'voting_shared', 'voting_none', 'prn_amt']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    df = df.drop_duplicates()
    return df

def insert_data_to_sql(df: pd.DataFrame, env_vars: Dict[str, str]) -> None:
    """Insert data into SQL database."""
    sql_helper = CloudSQLDatabase(
        env_vars['SQL_USER'],
        env_vars['SQL_PASSWORD'],
        env_vars['SQL_HOST'],
        env_vars['SQL_PORT'],
        env_vars['SQL_DATABASE'],
        big_flag=True,
        logger=logger
    )
    table_name = 'sec_13f'
    try:
        logger.info("Inserting data into SQL table %s", table_name)
        
        sql_helper.create_table(table_name, df.dtypes)
        sql_helper.insert_data(table_name, df)
        
    except Exception as e:
        logger.error(f"Error inserting data into SQL: {str(e)}")
        raise
    finally:
        sql_helper.close_connection()


def main():
    try:
        env_vars = load_env(SQL_VARS)
        cik_list = load_cik_list('./data/CIK_LIST.json')
        df_sec = process_sec_data(cik_list)
        insert_data_to_sql(df_sec, env_vars)
        logger.info("Data processing and insertion completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()