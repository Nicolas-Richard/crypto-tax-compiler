#sys.path.insert(0, '../config_directoy')
import requests
from bs4 import BeautifulSoup
from main_file import Coinmarketcap
import requests
import json

class HTMLTableParser:
    def parse_url(self, url):
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
        return [(table, self.parse_html_table(table)) for table in soup.find_all('table')]

    def parse_html_table(self, table):
        n_columns = 0
        n_rows = 0
        column_names = []

        # Find number of rows and columns
        # we also find the column titles if we can
        for row in table.find_all('tr'):

            # Determine the number of rows in the table
            td_tags = row.find_all('td')
            if len(td_tags) > 0:
                n_rows += 1
                if n_columns == 0:
                    # Set the number of columns for our table
                    n_columns = len(td_tags)

            # Handle column names if we find them
            th_tags = row.find_all('th')
            if len(th_tags) > 0 and len(column_names) == 0:
                for th in th_tags:
                    column_names.append(th.get_text())

        # Safeguard on Column Titles
        if len(column_names) > 0 and len(column_names) != n_columns:
            raise Exception("Column titles do not match the number of columns")

        list_storage = []
        row_marker = 0
        for row in table.find_all('tr'):
            column_marker = 0
            columns = row.find_all('td')
            if row.parent.name!='thead':
                daily_values = []
                for column in columns:
                    daily_values.append(column.get_text())
                    column_marker += 1
                    if len(columns) > 0:
                        row_marker += 1
                daily_dict_values = dict(zip(column_names, daily_values))
                daily_dict_values['Date'] = data_transformers.unix_ts_to_date(daily_dict_values.get('Date'))
                list_storage.append(daily_dict_values)
        return list_storage

a = data_transformers.coinmarketcap_date_to_date()

url="https://coinmarketcap.com/currencies/bitcoin/historical-data/?start=20171012&end=20171018"
hp = HTMLTableParser()
table = hp.parse_url(url)[0][1]
print(table)


