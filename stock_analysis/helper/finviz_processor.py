import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import datetime

class FinvizScraper:
    def __init__(self, url):
        self.url = url
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.142.86 Safari/537.36",
        }
        self.data = None
        self.df = None
    
    def fetch_data(self):
        response = requests.get(self.url, headers=self.header)
        if response.status_code == 200:
            soup = bs(response.content, 'html.parser')
            table = soup.find('table', class_='styled-table-new is-rounded is-tabular-nums w-full screener_table')
            if table:
                self.extract_data(table)
            else:
                print("Table not found in the HTML.")
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")

    def extract_data(self, table):
        td_tags = table.find_all('td')
        self.data = [td.get_text(strip=True) for td in td_tags]
        self.create_dataFrame()

    def create_dataFrame(self):
        # Assuming that 'data' contains a flat list of table cell values
        num_columns = 11  # Number of columns in the table
        rows = [self.data[i:i + num_columns] for i in range(0, len(self.data), num_columns)]
        
        columns = ['index', 'ticker', 'company', 'sector', 'industry', 'country', 'market_cap', 'pe', 'volume', 'price', 'change']
        self.df = pd.DataFrame(rows, columns=columns)
        self.df ['date_insert'] = datetime.datetime.today().strftime('%Y-%m-%d')
        self.df.drop(columns=['index'], inplace=True)