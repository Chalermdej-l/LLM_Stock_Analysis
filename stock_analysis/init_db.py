import json
import logging
from typing import Dict

from sqlalchemy import create_engine, text, inspect
from stock_analysis.settings import SQL_VARS, build_db_url, load_env

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_tables(engine, table_queries: Dict[str, str]):
    """
    Create tables in the database from the provided table creation queries.
    Skips tables that already exist.
    """
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    try:
        with engine.connect() as conn:
            for table_name, query in table_queries.items():
                if table_name in existing_tables:
                    logger.info(f"Table '{table_name}' already exists. Skipping.")
                    continue  # Skip to the next table

                logger.info(f"Creating table: {table_name}")
                try:
                    conn.execute(text(query.replace("'",'"')))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error creating table '{table_name}': {e}", exc_info=True)


            logger.info("Table creation process completed.")
    except Exception as e:
        logger.error(f"A general database error occurred: {e}", exc_info=True)



def main():
    """Main function."""
    try:
        env_vars = load_env(SQL_VARS + ['MODEL', 'API_KEY', 'PROJECT_ID', 'REGION_NAME', 'DATABASE_NAME'])
        engine = create_engine(build_db_url(env_vars))

        with open('./data/DB_INIT.json', 'r') as f:
            table_creation_queries = json.load(f)

        create_tables(engine, table_creation_queries)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()