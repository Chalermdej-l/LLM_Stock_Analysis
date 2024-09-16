import concurrent.futures
import pandas as pd
from helper.yahoo_processor import StockData
from helper.sql_processor import CloudSQLDatabase
class StockDetail:
    def __init__(self, stock_symbol_list: list, logger, env_vars, max_workers=5):
        """
        Initialize StockDetail class with a list of stock symbols, logger, and SQL helper.
        :param stock_symbol_list: List of stock symbols to fetch data for.
        :param logger: Logger for logging information and errors.
        :param sql_helper: SQL helper object for database operations.
        :param max_workers: Maximum number of threads to run in parallel (default is 5).
        """
        self.stock_symbol_list = stock_symbol_list
        self.logger = logger
        self.sql_helper = CloudSQLDatabase(
            env_vars['SQL_USER'],
            env_vars['SQL_PASSWORD'],
            env_vars['SQL_HOST'],
            env_vars['SQL_PORT'],
            env_vars['SQL_DATABASE'],
            big_flag=True,
            logger=logger
        )
        self.all_data_by_table = {}  # Dictionary to hold data for each table (keyed by table_name)
        self.max_workers = max_workers

    def process_yahoo_finance_pipeline(self):
        """
        Fetch Yahoo Finance data for all stock symbols concurrently and batch insert into the database.
        """
        try:
            created_tables = set()

            # Step 1: Fetch and store all data concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit fetch task for each stock symbol
                future_to_symbol = {executor.submit(self._fetch_stock_data, stock_symbol): stock_symbol for stock_symbol in self.stock_symbol_list}

                for future in concurrent.futures.as_completed(future_to_symbol):
                    stock_symbol = future_to_symbol[future]
                    try:
                        stock_symbol_data = future.result()  # Fetch result for each stock symbol
                        self.logger.info(f'Data fetched for {stock_symbol}')
                        self._collect_data(stock_symbol_data)  # Collect the data after fetching
                    except Exception as e:
                        self.logger.error(f"Error fetching data for {stock_symbol}: {e}")

            # Step 2: Insert all collected data in one batch per table
            for table_name, combined_data in self.all_data_by_table.items():
                if table_name not in created_tables:
                    self.sql_helper.create_table(table_name, combined_data.dtypes)
                    created_tables.add(table_name)
                self.sql_helper.update_table_schema(table_name, combined_data)
                self.sql_helper.insert_data(table_name, combined_data)

            self.logger.info("All stock data processed and tables updated successfully")

        except Exception as e:
            self.logger.error(f"An error occurred in Yahoo Finance pipeline: {str(e)}")

    def _fetch_stock_data(self, stock_symbol):
        """
        Fetch data for a single stock symbol.
        :param stock_symbol: Stock symbol to fetch data for.
        :return: Dictionary of fetched data.
        """
        stock_data = StockData(stock_symbol, self.logger)
        return stock_data.fetch_all_data()

    # def _collect_data(self, stock_symbol_data: dict):
    #     """
    #     Collect and combine data from individual stock symbols into self.all_data_by_table.
    #     :param stock_symbol_data: Dictionary containing the fetched data for a stock symbol.
    #     """
    #     for key, df in stock_symbol_data.items():
    #         if not df.empty:
    #             table_name = 'yahoofinance_' + key
    #             if table_name not in self.all_data_by_table:
    #                 # Initialize with the first dataframe
    #                 self.all_data_by_table[table_name] = df
    #             else:
    #                 # Append the new data to the existing dataframe for that table
    #                 self.all_data_by_table[table_name] = self.all_data_by_table[table_name].append(df, ignore_index=True)


    def _collect_data(self, stock_symbol_data: dict):
        """
        Collect and combine data from individual stock symbols into self.all_data_by_table.
        :param stock_symbol_data: Dictionary containing the fetched data for a stock symbol.
        """
        for key, df in stock_symbol_data.items():
            if not df.empty:
                table_name = 'yahoofinance_' + key
                if table_name not in self.all_data_by_table:
                    # Initialize with the first dataframe
                    self.all_data_by_table[table_name] = df
                else:
                    # Concatenate the new data to the existing dataframe for that table
                    self.all_data_by_table[table_name] = pd.concat([self.all_data_by_table[table_name], df], ignore_index=True)
