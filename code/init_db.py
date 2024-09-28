import logging
from typing import Dict
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from helper.sql_processor import CloudSQLDatabase
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_environment_variables() -> Dict[str, str]:
    """
    Load and validate required environment variables.
    """
    load_dotenv('./.env')
    required_vars = ['SQL_DATABASE', 'SQL_USER', 'SQL_PASSWORD', 'SQL_PORT', 'SQL_HOST', 'MODEL', 'API_KEY']
    env_vars = {var: os.getenv(var) for var in required_vars}
    
    missing_vars = [var for var, value in env_vars.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return env_vars

def save_to_file(filename: str, content: str) -> None:
    """
    Save content to a text file.
    """
    file_path = os.path.join('data', filename)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w+') as f:
        f.write(content)

def create_tables(engine, table_queries: Dict[str, str]):
    """
    Create tables in the database from the provided table creation queries.
    """
    try:
        with engine.connect() as conn:
            for table_name, query in table_queries.items():
                logger.info(f"Creating table: {table_name}")
                conn.execute(text(query.replace("'",'"')))
                conn.commit()
            logger.info("All tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}", exc_info=True)

def main():
    """
    Main function to execute the logic of loading environment variables,
    fetching stock screening data, inserting it into a Cloud SQL database,
    and processing reports using LLMProcessor.
    """
    try:
        # Load environment variables
        env_vars = load_environment_variables()

        # Initialize SQL helper (This is just a placeholder, as the actual class isn't defined here)
        sql_helper = CloudSQLDatabase(
            env_vars['SQL_USER'],
            env_vars['SQL_PASSWORD'],
            env_vars['SQL_HOST'],
            env_vars['SQL_PORT'],
            env_vars['SQL_DATABASE'],
            logger=logger
        )

        # Construct the database URL for SQLAlchemy
        db_url = f"postgresql+psycopg2://{env_vars['SQL_USER']}:{env_vars['SQL_PASSWORD']}@{env_vars['SQL_HOST']}:{env_vars['SQL_PORT']}/{env_vars['SQL_DATABASE']}"

        # Connect to the PostgreSQL database using SQLAlchemy
        engine = create_engine(db_url)

        with open('./data/DB_INIT.json', 'r') as f:
            table_creation_queries = json.load(f)

        # Create the tables using the queries
        create_tables(engine, table_creation_queries)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
