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
    BASE_URL = 'https://www.sec.gov'
    HEADERS = {
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/91.0.4472.124 Safari/537.36')
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
    def _rate_limited_request(self, url):
        try:
            response = requests.get(url, headers=self.HEADERS)
            response.raise_for_status()
            return response
        except RequestException as e:
            self.logger.error(f"Request failed for {url}: {str(e)}")
            return None

    def _parse_fund_name(self, soup):
        company_name = soup.find('span', {'class': 'companyName'})
        if company_name:
            return company_name.text.replace('(see all company filings)', '').strip()
        return "Unknown Fund"

    def _get_valid_urls(self, soup, date_part):
        links = soup.find_all('a', {'id': 'documentsbutton'})
        return [i['href'] for i in links if date_part in i['href']]

    def _parse_table(self, soup, date_part):
        tables = soup.find_all('table', {'summary': 'Form 13F-NT Header Information'})
        if not tables:
            return None

        column = ['name_of_issuer', 'title_of_class', 'cusip', 'figi', 'value', 'prn_amt', 'prn',
                'put_call', 'discretion', 'manager', 'voting_sole', 'voting_shared', 'voting_none']
        
        column_2 = ['name_of_issuer', 'title_of_class', 'cusip', 'value', 'prn_amt', 'prn',
                'put_call', 'discretion', 'manager', 'voting_sole', 'voting_shared', 'voting_none']
        
        col_int = ['value', 'prn_amt', 'voting_sole', 'voting_shared', 'voting_none']

        # Parse the HTML table into a DataFrame
        df_list = pd.read_html(StringIO(str(tables)))
        if not df_list or len(df_list) < 1:
            self.logger.warning("No tables found or table parsing failed.")
            return None
        
        df = df_list[0].iloc[3:]  # Skip header rows if necessary
        col_length = len(df.columns)

        if col_length == 13:
            df.columns = column
        else:
            df.columns = column_2
            df['figi'] = np.nan
            df = df[column]
        
        df['trans_date'] = datetime.datetime.strptime(date_part, '%Y-%m-%d')
        df[col_int] = df[col_int].apply(pd.to_numeric, errors='coerce')        

        # Data cleaning: Handle concatenated values
        for col in df.columns:
            df[col] = df[col].apply(lambda x: x if pd.isna(x) else str(x).replace('\n', ' '))

        # Ensure no concatenated rows
        def split_concat_rows(value):
            if isinstance(value, str) and value.isdigit() and len(value) > 3:
                return list(value)
            return [value]
        
        df = df.map(split_concat_rows).explode('value')
        df['date_insert'] = datetime.datetime.today().strftime('%Y-%m-%d')


        return df


    def fetch_fund_data(self, cik):
        self.logger.info(f'Fetching data for CIK {cik}')
        fund_data = []

        url = f'{self.BASE_URL}/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=13F-HR&owner=include&count=10&hidefilings=0'
        response_first = self._rate_limited_request(url)
        if not response_first:
            return fund_data

        soup = BeautifulSoup(response_first.content, 'html.parser')
        fund_name = self._parse_fund_name(soup)
        
        data_links = soup.find('a', {'id': 'documentsbutton'})
        if not data_links:
            self.logger.warning(f'No links found for CIK {cik}')
            return fund_data

        _link_part = data_links['href'].split('/')
        date_part = _link_part[-1].split('-')[1] + '-' + _link_part[-1].split('-')[2][:3]
        valid_urls = self._get_valid_urls(soup, date_part)

        date_report = soup.find('table',class_="tableFile2").find_all('td')[3].text

        for url in valid_urls:
            self.logger.info(f'Fetching data from {url}')
            response_first = self._rate_limited_request(self.BASE_URL + url)
            if not response_first:
                continue

            soup = BeautifulSoup(response_first.content, 'html.parser')
            _idpath = next((tag['href'] for tag in soup.find_all('a', href=True)
                           if (tag['href'].startswith('/'.join(url.split('/')[:-1]) + '/xslForm13F_')
                               and tag['href'].endswith('.xml') and 'primary_doc' not in tag['href'])), None)
            
            if not _idpath:
                self.logger.warning(f'No valid XML path found for {url}')
                continue

            response = self._rate_limited_request(f'{self.BASE_URL}{_idpath}')
            if not response:
                continue

            soup = BeautifulSoup(response.content, 'html.parser')
            df = self._parse_table(soup, date_report)
            
            if df is not None:
                df['fund_name'] = fund_name
                df['path_name'] = _idpath
                fund_data.append(df)
            else:
                self.logger.warning(f'No valid table found for {_idpath}')

        self.logger.info(f'Fetched data for CIK {cik}')
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
