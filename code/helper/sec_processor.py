import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
import logging
import numpy as np
import time
from requests.exceptions import RequestException
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime

pd.options.mode.chained_assignment = None

class SecProcessor:
    BASE_URL = "https://data.sec.gov/submissions/"
    HEADERS = {
        "User-Agent": "Test Project (Test_Project@test.com)"
    }
    RATE_LIMIT = 5  # requests per second
    RATE_LIMIT_PERIOD = 1  # second

    def __init__(self, cik_list, max_workers=5):
        self.cik_list = cik_list
        self.max_workers = max_workers
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)

    @sleep_and_retry
    @limits(calls=RATE_LIMIT, period=RATE_LIMIT_PERIOD)
    def get_13f_filings(self, cik):
        try:
            url = f"{self.BASE_URL}CIK{cik}.json"
            # self.logger.info(f"Fetching URL: {url}")
            response = requests.get(url, headers=self.HEADERS)
            response.raise_for_status()
            data = response.json()
            filings = data['filings']['recent']
            filings_df = pd.DataFrame(filings)
            form_13f_df = filings_df[filings_df['form'] == '13F-HR']
            return form_13f_df.iloc[:1]
        except RequestException as e:
            self.logger.error(f"RequestException for CIK {cik}: {e}")
            return pd.DataFrame()

    @sleep_and_retry
    @limits(calls=RATE_LIMIT, period=RATE_LIMIT_PERIOD)
    def get_13f_details(self, accession_number, cik, date_part):
        file_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number.replace('-', '')}/{accession_number}-index.htm"
        self.logger.info(f"Fetching URL: {file_url}")

        response = self._make_request(file_url)
        if response is None:
            return pd.DataFrame()

        xml_file_url = self._extract_xml_file_url(response.content, cik, accession_number)
        if xml_file_url is None:
            return pd.DataFrame()

        xml_response = self._make_request(xml_file_url)
        if xml_response is None:
            return pd.DataFrame()

        df = self._parse_xml_response(xml_response.content, date_part)
        if df is None:
            return pd.DataFrame()

        df = self._clean_dataframe(df, date_part)
        return df

    def _make_request(self, url):
        try:
            response = requests.get(url, headers=self.HEADERS)
            response.raise_for_status()
            return response
        except RequestException as e:
            self.logger.error(f"RequestException for URL {url}: {e}")
            return None

    def _extract_xml_file_url(self, html_content, cik, accession_number):
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', class_='tableFile')
        if not table:
            self.logger.error(f"Table not found for CIK {cik}, accession number {accession_number}")
            return None

        rows = table.find_all('tr')
        for row in rows:
            if 'information table' in row.text.lower():
                return 'https://www.sec.gov' + row.find('a')['href']

        self.logger.error(f"Information table link not found for CIK {cik}, accession number {accession_number}")
        return None

    def _parse_xml_response(self, xml_content, date_part):
        xml_soup = BeautifulSoup(xml_content, 'xml')
        tables = xml_soup.find_all('table', {'summary': 'Form 13F-NT Header Information'})
        if not tables:
            self.logger.error(f"XML tables not found in information table for date part {date_part}")
            return None

        df_list = pd.read_html(StringIO(str(tables)))
        if not df_list:
            self.logger.error(f"No DataFrames parsed from XML for date part {date_part}")
            return None

        df = df_list[0].iloc[3:]
        return df

    def _clean_dataframe(self, df, date_part):
        column = ['name_of_issuer', 'title_of_class', 'cusip', 'figi', 'value', 'prn_amt', 'prn',
                  'put_call', 'discretion', 'manager', 'voting_sole', 'voting_shared', 'voting_none']
        column_2 = ['name_of_issuer', 'title_of_class', 'cusip', 'value', 'prn_amt', 'prn',
                    'put_call', 'discretion', 'manager', 'voting_sole', 'voting_shared', 'voting_none']
        col_int = ['value', 'prn_amt', 'voting_sole', 'voting_shared', 'voting_none']

        col_length = len(df.columns)
        if col_length == 13:
            df.columns = column
        else:
            df.columns = column_2
            df['figi'] = np.nan
            df = df[column]

        df['trans_date'] = datetime.datetime.strptime(date_part, '%Y-%m-%d')
        df[col_int] = df[col_int].apply(pd.to_numeric, errors='coerce')

        for col in df.columns:
            df[col] = df[col].apply(lambda x: x if pd.isna(x) else str(x).replace('\n', ' '))

        df['value'] = df['value'].apply(lambda x: x if not isinstance(x, str) or ',' not in x else x.split(','))
        df = df.explode('value')
        df['value'] = df['value'].apply(lambda x: x.strip())

        df['date_insert'] = datetime.datetime.today().strftime('%Y-%m-%d')
        return df

    def fetch_fund_data(self, cik):
        form_13f_df = self.get_13f_filings(cik)
        if form_13f_df.empty:
            return []

        fund_data = []
        for _, row in form_13f_df.iterrows():
            details_df = self.get_13f_details(row['accessionNumber'], cik, row['filingDate'])
            if not details_df.empty:
                fund_data.append(details_df)
        time.sleep(1)
        return fund_data

    def process_all_funds(self):
        start_time = time.time()
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_cik = {executor.submit(self.fetch_fund_data, cik): cik for cik in self.cik_list}
            for future in as_completed(future_to_cik):
                cik = future_to_cik[future]
                try:
                    data = future.result()
                    results.extend(data)
                except Exception as exc:
                    self.logger.error(f'{cik} generated an exception: {exc}')

        if results:
            df_all = pd.concat(results, ignore_index=True)
            df_all.replace('None', pd.NA, inplace=True)
            self.logger.info('All data was collected.')
            return df_all
        else:
            self.logger.warning('No data was collected.')

        end_time = time.time()
        self.logger.info(f'Total execution time: {end_time - start_time:.2f} seconds')
