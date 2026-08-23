import logging

from stock_analysis.settings import SQL_VARS, load_env
from stock_analysis.helper.sql_processor import CloudSQLDatabase
from stock_analysis.helper.magic_processor import MagicFormulaInvesting

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    """
    Main function to execute the logic of loading environment variables,
    fetching stock screening data, and inserting it into a Cloud SQL database.
    """
    try:
        # Load environment variables
        env_vars = load_env(SQL_VARS + ["MAGIC_USER", "MAGIC_PW"])

        # Initialize SQL helper
        sql_helper = CloudSQLDatabase(
            env_vars["SQL_USER"],
            env_vars["SQL_PASSWORD"],
            env_vars["SQL_HOST"],
            env_vars["SQL_PORT"],
            env_vars["SQL_DATABASE"],
            logger=logger,
        )

        # Fetch stock screening data using MagicFormulaInvesting
        email = env_vars["MAGIC_USER"]
        password = env_vars["MAGIC_PW"]
        mfi = MagicFormulaInvesting(email, password)
        stock_df = mfi.get_stock_screening()

        # Define the table name
        table_name = "magic_screen"

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
