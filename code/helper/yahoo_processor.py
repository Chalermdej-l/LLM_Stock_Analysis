import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

class StockData:
    def __init__(self, ticker):
        self.ticker = ticker
        self.company = yf.Ticker(ticker)
        self.data_frames = {}
        self.current_date = datetime.today().strftime('%Y-%m-%d')

    def _add_current_date(self, df):
        df['date'] = self.current_date
        return df

    def _format_columns(self, df):
        df.columns = [col.lower().replace(' ', '_')[:59] for col in df.columns]
        return df

    def fetch_info(self):
        info = self.company.info
        return info

    def fetch_history(self, period="max", interval="1d"):
        try:
            history = self.company.history(period=period, interval=interval).reset_index()
            history.rename(columns={'index': 'date'}, inplace=True)
            history = self._format_columns(history)
            history['symbol'] = self.ticker
            history = self._add_current_date(history)
            history = history.fillna(np.nan)
            self.data_frames['history'] = history
            return history
        except Exception as e:
            print(f"Failed to fetch history: {e}")
            self.data_frames['history'] = pd.DataFrame()

    def fetch_metadata(self):
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
            df_meta = self._add_current_date(df_meta)
            df_meta = df_meta.fillna(np.nan)
            self.data_frames['metadata'] = df_meta
            return df_meta
        except Exception as e:
            print(f"Failed to fetch metadata: {e}")
            self.data_frames['metadata'] = pd.DataFrame()

    def fetch_insider_roster_holders(self):
        try:
            df_insider = self.company.insider_roster_holders
            df_insider = self._format_columns(df_insider)
            df_insider['symbol'] = self.ticker
            df_insider = self._add_current_date(df_insider)
            df_insider = df_insider.fillna(np.nan)
            self.data_frames['insider_roster_holders'] = df_insider
            return df_insider
        except Exception as e:
            print(f"Failed to fetch insider roster holders: {e}")
            self.data_frames['insider_roster_holders'] = pd.DataFrame()

    def fetch_holders(self):
        try:
            df_holders = self.company.mutualfund_holders
            df_holders['type'] = 'mutual'
            df_institutional = self.company.institutional_holders
            df_institutional['type'] = 'institutional'
            df_holders = pd.concat([df_holders, df_institutional])
            df_holders = self._format_columns(df_holders)
            df_holders['symbol'] = self.ticker
            df_holders = self._add_current_date(df_holders)
            df_holders = df_holders.fillna(np.nan)
            self.data_frames['holders'] = df_holders
            return df_holders
        except Exception as e:
            print(f"Failed to fetch holders: {e}")
            self.data_frames['holders'] = pd.DataFrame()

    def fetch_cashflow(self):
        try:
            df_cashflow = self.company.cashflow
            df_cashflow = df_cashflow.T.astype(float).round(2).reset_index()
            df_cashflow.rename(columns={'index': 'date'}, inplace=True)
            df_cashflow = self._format_columns(df_cashflow)
            df_cashflow['symbol'] = self.ticker
            df_cashflow = self._add_current_date(df_cashflow)
            df_cashflow = df_cashflow.fillna(np.nan)
            self.data_frames['cash_flow'] = df_cashflow
            return df_cashflow
        except Exception as e:
            print(f"Failed to fetch cashflow: {e}")
            self.data_frames['cash_flow'] = pd.DataFrame()

    def fetch_balance_sheet(self):
        try:
            df_balance_sheet = self.company.balance_sheet
            df_balance_sheet = df_balance_sheet.T.astype(float).round(2).reset_index()
            df_balance_sheet.rename(columns={'index': 'date'}, inplace=True)
            df_balance_sheet = self._format_columns(df_balance_sheet)
            df_balance_sheet['symbol'] = self.ticker
            df_balance_sheet = self._add_current_date(df_balance_sheet)
            df_balance_sheet = df_balance_sheet.fillna(np.nan)
            self.data_frames['balance_sheet'] = df_balance_sheet
            return df_balance_sheet
        except Exception as e:
            print(f"Failed to fetch balance sheet: {e}")
            self.data_frames['balance_sheet'] = pd.DataFrame()

    def fetch_income_statement(self):
        try:
            df_income_stmt = self.company.income_stmt
            df_income_stmt = df_income_stmt.T.astype(float).round(2).reset_index()
            df_income_stmt.rename(columns={'index': 'date'}, inplace=True)
            df_income_stmt = self._format_columns(df_income_stmt)
            df_income_stmt['symbol'] = self.ticker
            df_income_stmt = self._add_current_date(df_income_stmt)
            df_income_stmt = df_income_stmt.fillna(np.nan)
            self.data_frames['income_statement'] = df_income_stmt
            return df_income_stmt
        except Exception as e:
            print(f"Failed to fetch income statement: {e}")
            self.data_frames['income_statement'] = pd.DataFrame()

    def fetch_historical_data(self, period="max"):
        try:
            historical_data = yf.download(self.ticker, period=period).reset_index()
            historical_data.rename(columns={'index': 'date'}, inplace=True)
            historical_data = self._format_columns(historical_data)
            historical_data = self._add_current_date(historical_data)
            historical_data = historical_data.fillna(np.nan)
            self.data_frames['historical_data'] = historical_data
            return historical_data
        except Exception as e:
            print(f"Failed to fetch historical data: {e}")
            self.data_frames['historical_data'] = pd.DataFrame()

    def fetch_all_data(self):
        self.fetch_history()
        self.fetch_metadata()
        self.fetch_insider_roster_holders()
        self.fetch_holders()
        self.fetch_cashflow()
        self.fetch_balance_sheet()
        self.fetch_income_statement()
        self.fetch_historical_data()
        return self.data_frames
