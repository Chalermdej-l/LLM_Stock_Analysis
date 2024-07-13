import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime

class MagicFormulaInvesting:
    def __init__(self, email, password):
        self.login_url = "https://www.magicformulainvesting.com/Account/LogOn"
        self.screening_url = "https://www.magicformulainvesting.com/Screening/StockScreening"
        self.credentials = {
            'Email': email,
            'Password': password
        }
        self.session = requests.Session()

    def login(self):
        # Get the login page to retrieve the token
        login_page = self.session.get(self.login_url)
        soup = BeautifulSoup(login_page.text, 'html.parser')

        # Find the __RequestVerificationToken value
        token = soup.find('input', {'name': '__RequestVerificationToken'}).get('value')

        # Add the token to your credentials
        self.credentials['__RequestVerificationToken'] = token

        # Perform the login
        response = self.session.post(self.login_url, data=self.credentials)

        # Check if login was successful
        if "Logout" in response.text:
            print("Login successful!")
            return True
        else:
            print("Login failed!")
            return False

    def get_stock_screening(self, minimum_market_cap='50', select_30=False):
        if not self.login():
            return None

        # Prepare the form data for screening stocks
        screening_data = {
            '__RequestVerificationToken': self.credentials['__RequestVerificationToken'],
            'MinimumMarketCap': minimum_market_cap,  # Example minimum market cap
            'Select30': 'true' if select_30 else 'false'  # 'false' to select 50 stocks
        }

        # Submit the form to get the stock screening results
        screening_response = self.session.post(self.screening_url, data=screening_data)
        soup = BeautifulSoup(screening_response.text, 'html.parser')

        # Find the table with stock data
        table = soup.findAll('tbody')[1].find_all('tr')

        table_list = []
        for row in table:
            columns = row.find_all('td')
            table_list.append({
                'company': columns[0].text.strip(),
                'ticker': columns[1].text.strip(),
                'market_cap': columns[2].text.strip()
            })
        
        df = pd.DataFrame(table_list)
        df['date_insert'] = datetime.datetime.today().strftime('%Y-%m-%d')

        return df
