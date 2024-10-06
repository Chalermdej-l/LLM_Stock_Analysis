import logging
from typing import Dict
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_environment_variables() -> Dict[str, str]:
    """Load and validate required environment variables."""
    load_dotenv('./.env')
    required_vars = ['SQL_DATABASE', 'SQL_USER', 'SQL_PASSWORD', 'SQL_PORT', 'SQL_HOST', 'MODEL', 'API_KEY', 'PROJECT_ID', 'REGION_NAME', 'DATABASE_NAME']
    env_vars = {var: os.getenv(var) for var in required_vars}
    env_vars['DB_URL'] = ':'.join([env_vars['PROJECT_ID'],env_vars['REGION_NAME'],env_vars['DATABASE_NAME']])
    
    missing_vars = [var for var, value in env_vars.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return env_vars

def save_to_file(filename: str, content: str) -> None:
    """Save content to a text file."""
    file_path = os.path.join('data', filename)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w+') as f:
        f.write(content)

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
        env_vars = load_environment_variables()

        db_url = f"postgresql+psycopg2://{env_vars['SQL_USER']}:{env_vars['SQL_PASSWORD']}@{env_vars['SQL_HOST']}:{env_vars['SQL_PORT']}/{env_vars['SQL_DATABASE']}"
        engine = create_engine(db_url)

        with open('./data/DB_INIT.json', 'r') as f:
            table_creation_queries = json.load(f)

        create_tables(engine, table_creation_queries)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()