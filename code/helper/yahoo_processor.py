import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import time

class StockData:
    def __init__(self, ticker, logger):
        self.ticker = ticker.strip()
        self.company = yf.Ticker(ticker)
        self.data_frames = {}
        self.current_date = datetime.today().strftime('%Y-%m-%d')
        self.logger = logger

    def _add_meta_data(self, df):
        df['date_insert'] = self.current_date
        df['ticker_name'] = self.ticker
        return df

    def _format_columns(self, df):
        df.columns = [col.lower().replace(' ', '_')[:59] for col in df.columns]
        return df

    def _is_empty(self, result):
        if isinstance(result, pd.DataFrame):
            return result.empty
        elif isinstance(result, dict):
            return not bool(result)  # True if dict is empty
        else:
            self.logger.error(f"Unexpected result type: {type(result)}")
            return True
        
    def _retry_operation(self, operation, max_retries=2, delay=3):
        for attempt in range(max_retries + 1):
            try:
                result = operation()
                if self._is_empty(result):
                    if attempt < max_retries:
                        self.logger.error(f"Attempt {attempt + 1} failed. Retrying in {delay} second(s)...")
                        time.sleep(delay)
                        self.company = yf.Ticker(self.ticker)
                    else:
                        self.logger.error(f"All attempts failed. Last error: {result}")
                        return pd.DataFrame()
                else:
                    return result
            except Exception as e:
                self.logger.error(f"Error during operation: {str(e)}")
                if attempt < max_retries:
                    self.logger.info(f"Retrying operation after {delay} seconds...")
                    time.sleep(delay)
                else:
                    self.logger.error(f"All attempts failed due to error: {str(e)}")
                    return pd.DataFrame()
        return pd.DataFrame()

    def fetch_info(self):
        return self._retry_operation(self.company.info.copy)

    def fetch_history(self, period="max", interval="1d"):
        def fetch_operation():
            try:
                history = self.company.history(period=period, interval=interval).reset_index()
                history.rename(columns={'index': 'date'}, inplace=True)
                history = self._format_columns(history)
                history['symbol'] = self.ticker
                history = self._add_meta_data(history)
                history = history.fillna(np.nan)
                return history
            except Exception as e:
                self.logger.error(f"Error fetching history: {str(e)}")
                return pd.DataFrame()

        history = self._retry_operation(fetch_operation)
        self.data_frames['history'] = history if history is not None else pd.DataFrame()
        return self.data_frames['history']

    def fetch_metadata(self):
        def fetch_operation():
            try:
                metadata = self.company.history_metadata
                keys_to_delete = ['currentTradingPeriod', 'dataGranularity', 'range', 'validRanges']
                for key in keys_to_delete:
                    if key in metadata:
                        del metadata[key]
                df_meta = pd.json_normalize(metadata)
                info = self.fetch_info()
                keys_to_add = ['website', 'industry', 'sector', 'fullTimeEmployees',
                               'auditRisk', 'boardRisk', 'compensationRisk',
                               'shareHolderRightsRisk', 'overallRisk', 'irWebsite']
                for key in keys_to_add:
                    df_meta[key] = info.get(key, None)
                df_meta = self._format_columns(df_meta)
                df_meta = self._add_meta_data(df_meta)
                df_meta = df_meta.fillna(np.nan)
                return df_meta
            except Exception as e:
                self.logger.error(f"Error fetching metadata: {str(e)}")
                return pd.DataFrame()

        metadata = self._retry_operation(fetch_operation)
        self.data_frames['metadata'] = metadata if metadata is not None else pd.DataFrame()
        return self.data_frames['metadata']

    def fetch_insider_roster_holders(self):
        def fetch_operation():
            try:
                df_insider = self.company.insider_roster_holders
                df_insider = self._format_columns(df_insider)
                df_insider['symbol'] = self.ticker
                df_insider = self._add_meta_data(df_insider)
                df_insider = df_insider.fillna(np.nan)
                return df_insider
            except Exception as e:
                self.logger.error(f"Error fetching insider roster holders: {str(e)}")
                return pd.DataFrame()

        insider_roster = self._retry_operation(fetch_operation)
        self.data_frames['insider_roster_holders'] = insider_roster if insider_roster is not None else pd.DataFrame()
        return self.data_frames['insider_roster_holders']

    def fetch_holders(self):
        def fetch_operation():
            try:
                df_holders = self.company.mutualfund_holders
                df_holders['type'] = 'mutual'
                df_institutional = self.company.institutional_holders
                df_institutional['type'] = 'institutional'
                df_holders = pd.concat([df_holders, df_institutional])
                df_holders = self._format_columns(df_holders)
                df_holders['symbol'] = self.ticker
                df_holders = self._add_meta_data(df_holders)
                df_holders = df_holders.fillna(np.nan)
                return df_holders
            except Exception as e:
                self.logger.error(f"Error fetching holders: {str(e)}")
                return pd.DataFrame()

        holders = self._retry_operation(fetch_operation)
        self.data_frames['holders'] = holders if holders is not None else pd.DataFrame()
        return self.data_frames['holders']

    def fetch_cashflow(self):
        def fetch_operation():
            try:
                df_cashflow = pd.concat([self.company.cashflow, self.company.quarterly_cash_flow], axis=1)
                df_cashflow = df_cashflow.T.astype(float).round(2).reset_index()
                df_cashflow.rename(columns={'index': 'date'}, inplace=True)
                df_cashflow = self._format_columns(df_cashflow)
                df_cashflow['symbol'] = self.ticker
                df_cashflow = self._add_meta_data(df_cashflow)
                df_cashflow = df_cashflow.fillna(np.nan)
                return df_cashflow
            except Exception as e:
                self.logger.error(f"Error fetching cashflow: {str(e)}")
                return pd.DataFrame()

        cashflow = self._retry_operation(fetch_operation)
        self.data_frames['cash_flow'] = cashflow if cashflow is not None else pd.DataFrame()
        return self.data_frames['cash_flow']

    def fetch_balance_sheet(self):
        def fetch_operation():
            try:
                df_balance_sheet = pd.concat([self.company.balance_sheet, self.company.quarterly_balance_sheet], axis=1)
                df_balance_sheet = df_balance_sheet.T.astype(float).round(2).reset_index()
                df_balance_sheet.rename(columns={'index': 'date'}, inplace=True)
                df_balance_sheet = self._format_columns(df_balance_sheet)
                df_balance_sheet['symbol'] = self.ticker
                df_balance_sheet = self._add_meta_data(df_balance_sheet)
                df_balance_sheet = df_balance_sheet.fillna(np.nan)
                return df_balance_sheet
            except Exception as e:
                self.logger.error(f"Error fetching balance sheet: {str(e)}")
                return pd.DataFrame()

        balance_sheet = self._retry_operation(fetch_operation)
        self.data_frames['balance_sheet'] = balance_sheet if balance_sheet is not None else pd.DataFrame()
        return self.data_frames['balance_sheet']

    def fetch_income_statement(self):
        def fetch_operation():
            try:
                df_income_stmt = pd.concat([self.company.income_stmt, self.company.quarterly_incomestmt], axis=1)
                df_income_stmt = df_income_stmt.T.astype(float).round(2).reset_index()
                df_income_stmt.rename(columns={'index': 'date'}, inplace=True)
                df_income_stmt = self._format_columns(df_income_stmt)
                df_income_stmt['symbol'] = self.ticker
                df_income_stmt = self._add_meta_data(df_income_stmt)
                df_income_stmt = df_income_stmt.fillna(np.nan)
                return df_income_stmt
            except Exception as e:
                self.logger.error(f"Error fetching income statement: {str(e)}")
                return pd.DataFrame()

        income_statement = self._retry_operation(fetch_operation)
        self.data_frames['income_statement'] = income_statement if income_statement is not None else pd.DataFrame()
        return self.data_frames['income_statement']

    def fetch_all_data(self):
        self.logger.info('Processing history data...')
        self.fetch_history()
        self.logger.info('Processing metadata...')
        self.fetch_metadata()
        self.logger.info('Processing insider roster and holders...')
        self.fetch_insider_roster_holders()
        self.logger.info('Processing holders...')
        self.fetch_holders()
        self.logger.info('Processing cash flow...')
        self.fetch_cashflow()
        self.logger.info('Processing balance sheet...')
        self.fetch_balance_sheet()
        self.logger.info('Processing income statement...')
        self.fetch_income_statement()

        time.sleep(1)
        return self.data_frames
