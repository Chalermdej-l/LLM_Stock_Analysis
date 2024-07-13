import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime

pd.options.mode.chained_assignment = None 

class DataromaScraper:
    def __init__(self):
        self.base_url = 'https://www.dataroma.com'
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }

    def make_request(self, path_url):
        url = self.base_url + path_url
        result = requests.get(url, headers=self.header)
        return result

    def get_soup_page_list(self, soup):    
        div_element = soup.find('div', {'id': 'pages'})
        a_tags = div_element.find_all('a')

        hrefs = set()
        for tag in a_tags:
            hrefs.add(tag['href'])

        hrefs = sorted(list(hrefs))[1:]
        return hrefs

    def get_table_data(self, soup):
        tables = soup.find_all('table')[2]
        table_data = []

        # Skip column row
        for tr in tables.find_all('tr')[1:]:
            cols = tr.find_all('td')
            cols = [col.text.strip() for col in cols]
            table_data.append(cols)
        return table_data

    def scrape_insider_buy_data(self, path_url):
        result = self.make_request(path_url)
        soup = BeautifulSoup(result.text, 'html.parser')
        hrefs = self.get_soup_page_list(soup)
        table_data = self.get_table_data(soup)

        columns = ['filing', 'symbol', 'security', 'reporting_name', 'relationship', 'trans_date', 'purchase_sale', 'shares', 'price', 'amount', 'di']
        for href in hrefs:
            result = self.make_request(href)
            soup = BeautifulSoup(result.text, 'html.parser')
            table_data.extend(self.get_table_data(soup))

        df_insider_buy = pd.DataFrame(table_data, columns=columns)
        df_insider_buy = df_insider_buy.query("symbol != ''")

        df_insider_buy['relationship'] = df_insider_buy['relationship'].str.title() \
            .str.replace('Chief Accounting Officer', 'CAO') \
            .str.replace('Chief Financial Officer', 'CFO') \
            .str.replace('Chief Executive Officer', 'CEO') \
            .str.replace('Chief Operating Officer', 'COO') \
            .str.replace('See Remark', 'Other') \
            .str.replace('Chief Executive Officer', 'CEO')
        df_insider_buy['purchase_sale'] = df_insider_buy['purchase_sale'].str.replace('Purchase', 'Buy')
        df_insider_buy['amount'] = df_insider_buy['amount'].str.replace(',', '').astype('int')
        df_insider_buy['count'] = 1
        df_insider_buy['trans_date'] = pd.to_datetime(df_insider_buy['trans_date'], format='%d %b %Y')
        
        # Group by all relevant columns except 'amount' and 'shares'
        groupby_columns = ['symbol', 'relationship', 'trans_date', 'purchase_sale', 'price', 'security', 'reporting_name', 'di']
        df_insider_buy = df_insider_buy.groupby(groupby_columns).agg({
            'amount': 'sum',
            'shares': 'sum',
            'count': 'sum'
        }).reset_index()
        
        df_insider_buy['date_insert'] = datetime.datetime.today().strftime('%Y-%m-%d')
        return df_insider_buy

    def scrape_table(self, table):
        rows = []
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            row_data = [cell.get_text(strip=True) for cell in cells]
            rows.append(row_data)
        return rows

    def scrape_home_data(self):
        url = 'https://www.dataroma.com/m/home.php'
        result = requests.get(url, headers=self.header)
        soup = BeautifulSoup(result.text, 'html.parser')
        tables = soup.find_all('table')

        df_insider_buy_home = pd.DataFrame(self.scrape_table(tables[0]), columns=['date_filling', 'company', 'total_value', 'price'])
        df_insider_buy_home['ticker'] = df_insider_buy_home['company'].str.split('-').str[0].str.strip()
        df_insider_buy_home['company'] = df_insider_buy_home['company'].str.split('-').str[1].str.strip()

        df_insider_buy_home['total_value'] = df_insider_buy_home['total_value'].str.replace(',', '').astype('int')
        df_insider_buy_home['date_filling'] = df_insider_buy_home['date_filling'] + ' ' + datetime.date.today().strftime('%Y')
        df_insider_buy_home['date_filling'] = pd.to_datetime(df_insider_buy_home['date_filling'], format='%d %b %Y')
        # Group by all relevant columns
        groupby_columns = ['ticker', 'date_filling', 'company', 'price']
        df_insider_buy_home = df_insider_buy_home.groupby(groupby_columns).agg({
            'total_value': 'sum'
        }).reset_index()
        
        df_insider_buy_home['date_insert'] = datetime.datetime.today().strftime('%Y-%m-%d')
        df_insider_buy_home['total_value'] = df_insider_buy_home['total_value'].astype('int')

        df_bigbets = pd.DataFrame(self.scrape_table(tables[2]), columns=['company', 'percent_owned', 'count'])
        df_bigbets['ticker'] = df_bigbets['company'].str.split('-').str[0].str.strip()
        df_bigbets['company'] = df_bigbets['company'].str.split('-').str[1].str.strip()
        df_bigbets['date_insert'] = datetime.datetime.today().strftime('%Y-%m-%d')

        df_low = pd.DataFrame(self.scrape_table(tables[3]), columns=['company', 'percent_owned'])
        df_low['ticker'] = df_low['company'].str.split('-').str[0].str.strip()
        df_low['company'] = df_low['company'].str.split('-').str[1].str.strip()
        df_low['date_insert'] = datetime.datetime.today().strftime('%Y-%m-%d')

        df_insider_super = pd.DataFrame(self.scrape_table(tables[4]), columns=['company', 'count', 'total_amount'])
        df_insider_super['ticker'] = df_insider_super['company'].str.split('-').str[0].str.strip()
        df_insider_super['company'] = df_insider_super['company'].str.split('-').str[1].str.strip()
        df_insider_super['date_insert'] = datetime.datetime.today().strftime('%Y-%m-%d')
    
        return df_insider_buy_home, df_bigbets, df_low, df_insider_super