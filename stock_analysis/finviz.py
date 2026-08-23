import logging

from stock_analysis.settings import SQL_VARS, load_env
from stock_analysis.helper.sql_processor import CloudSQLDatabase
from stock_analysis.helper.finviz_processor import FinvizScraper

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
            env_vars['SQL_USER'],
            env_vars['SQL_PASSWORD'],
            env_vars['SQL_HOST'],
            env_vars['SQL_PORT'],
            env_vars['SQL_DATABASE'],
            logger=logger
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
