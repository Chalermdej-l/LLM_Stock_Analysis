import logging

from stock_analysis.settings import SQL_VARS, load_env
from stock_analysis.constants import FINVIZ_SCREENER_URL, FINVIZ_SCREEN_TABLE
from stock_analysis.helper.sql_processor import CloudSQLDatabase
from stock_analysis.helper.finviz_processor import FinvizScraper

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    """
    Main function to load environment variables, initialize SQL helper and Finviz scraper, and update the database.
    """
    try:
        # Load environment variables
        env_vars = load_env(SQL_VARS)

        # Initialize CloudSQLDatabase
        sql_helper = CloudSQLDatabase(
            env_vars["SQL_USER"],
            env_vars["SQL_PASSWORD"],
            env_vars["SQL_HOST"],
            env_vars["SQL_PORT"],
            env_vars["SQL_DATABASE"],
            logger=logger,
        )

        # Initialize FinvizScraper and fetch data
        scraper = FinvizScraper(FINVIZ_SCREENER_URL)
        scraper.fetch_data()

        # Table name for SQL database
        table_name = FINVIZ_SCREEN_TABLE

        # Create table and insert data
        sql_helper.create_table(table_name, scraper.df.dtypes)
        sql_helper.insert_data(table_name, scraper.df)

        logger.info("All tables updated successfully")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
