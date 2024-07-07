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

    def fetch_info(self):
        info = self.company.info
        return info

    def fetch_history(self, period="max", interval="1d"):
        history = self.company.history(period=period, interval=interval).reset_index()
        history.rename(columns={'index':'date'},inplace=True)
        history.columns = [i.lower() for i in history.columns]
        history['symbol'] = self.ticker
        history = self._add_current_date(history)
        self.data_frames['history'] = history
        return history

    def fetch_metadata(self):
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
        df_meta.columns = [i.lower() for i in df_meta.columns]
        df_meta = self._add_current_date(df_meta)
        self.data_frames['metadata'] = df_meta
        return df_meta

    def fetch_insider_roster_holders(self):
        df_insider = self.company.insider_roster_holders
        df_insider.columns = [i.lower() for i in df_insider.columns]
        df_insider['symbol'] = self.ticker
        df_insider = self._add_current_date(df_insider)
        self.data_frames['insider_roster_holders'] = df_insider
        return df_insider

    def fetch_holders(self):
        df_holders = self.company.mutualfund_holders
        df_holders['type'] = 'mutual'
        df_institutional = self.company.institutional_holders
        df_institutional['type'] = 'institutional'
        df_holders = pd.concat([df_holders, df_institutional])
        df_holders.columns = [i.lower() for i in df_holders.columns]
        df_holders['symbol'] = self.ticker
        df_holders = self._add_current_date(df_holders)
        self.data_frames['holders'] = df_holders
        return df_holders

    def fetch_cashflow(self):
        df_cashflow = self.company.cashflow            
        df_cashflow = df_cashflow.T.astype(float).round(2).reset_index()
        df_cashflow.rename(columns={'index':'date'},inplace=True)
        df_cashflow.columns = [i.lower() for i in df_cashflow.columns]
        df_cashflow['symbol'] = self.ticker
        df_cashflow = self._add_current_date(df_cashflow)
        self.data_frames['cash_flow'] = df_cashflow             
        return df_cashflow

    def fetch_balance_sheet(self):
        df_balance_sheet = self.company.balance_sheet
        df_balance_sheet = df_balance_sheet.T.astype(float).round(2).reset_index()
        df_balance_sheet.rename(columns={'index':'date'},inplace=True)
        df_balance_sheet.columns = [i.lower() for i in df_balance_sheet.columns]
        df_balance_sheet['symbol'] = self.ticker
        df_balance_sheet = self._add_current_date(df_balance_sheet)
        self.data_frames['balance_sheet'] = df_balance_sheet
        return df_balance_sheet

    def fetch_income_statement(self):
        df_income_stmt = self.company.income_stmt
        df_income_stmt = df_income_stmt.T.astype(float).round(2).reset_index()
        df_income_stmt.rename(columns={'index':'date'},inplace=True)
        df_income_stmt.columns = [i.lower() for i in df_income_stmt.columns]
        df_income_stmt['symbol'] = self.ticker
        df_income_stmt = self._add_current_date(df_income_stmt)
        self.data_frames['income_statement'] = df_income_stmt
        return df_income_stmt

    def fetch_historical_data(self, period="max"):
        historical_data = yf.download(self.ticker, period=period).reset_index()
        historical_data.rename(columns={'index':'date'},inplace=True)
        historical_data.columns = [i.lower() for i in historical_data.columns]
        historical_data = self._add_current_date(historical_data)
        self.data_frames['historical_data'] = historical_data
        return historical_data

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