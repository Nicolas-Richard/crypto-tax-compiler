import datetime
import json
from collections import namedtuple
from urllib.parse import urljoin

import psycopg2
import requests
import inspect
from bs4 import BeautifulSoup

import config_directory.config_file as config

# Constants
class Coinmarketcap:
    API_DOMAIN_NAME="https://api.coinmarketcap.com/"
    TABLE_DOMAIN_NAME="https://coinmarketcap.com/"
    MIN_FETCHING_DATE="20170101"


# Defining tuple to hold data from API
Daily_ticker_info = namedtuple('Daily_ticker_info','id, name, symbol, rank, price_usd, market_cap_usd, available_supply, total_supply, last_updated, volume_24h_usd')
Historical_ticker_info = namedtuple('Historical_ticker_info','date, open, high, low, close, volume, market_cap, id')

# Columns
fact_price_volume_stats_daily_columns=('id, name, symbol, rank, price_usd, market_cap_usd, available_supply, total_supply, last_updated, volume_24h_usd')
fact_price_volume_stats_daily_historical_columns=('date, open, high, low, close, volume, market cap, id')

# Connect to Postgres SQL
try:
    conn=psycopg2.connect(host=config.hostname, user=config.username, password=config.password, dbname=config.database)
    conn.autocommit = True
except:
    print("I am unable to connect to the database")

cur = conn.cursor()


# Functions fetching from API
class data_fetchers:
    # Fetching Data From API
    def api_json_fetcher(self, ticker, unwanted=[]):
        extra_part = "v1/ticker/"+ticker #https://api.coinmarketcap.com/v1/ticker/ethereum
        url = urljoin(Coinmarketcap.API_DOMAIN_NAME, extra_part)
        response = requests.get(url)

        # Modify json to make it tuple compliant
        response = json.loads(response.content, object_hook=data_transformers.modify_key)

        # Remove useless keys
        for unwanted_key in unwanted:
            for x in response:
                if unwanted_key in x:
                    del x[unwanted_key]

        response = response[0]

        # Transform timestamp to date
        response['last_updated']=data_transformers.unix_ts_to_date(response.get('last_updated'))

        return response

    def html_fetcher(self, ticker, start_date, end_date):
        extra_part = "currencies/" + ticker + "/historical-data/?start=" + start_date + "&end=" + end_date
        url = urljoin(Coinmarketcap.TABLE_DOMAIN_NAME, extra_part) #https://coinmarketcap.com/currencies/bitcoin/historical-data/
        response = requests.get(url)

        soup = BeautifulSoup(response.text, 'lxml')
        return [(table, data_transformers.parse_html_table(table)) for table in soup.find_all('table')]


# Functions transforming fetched data
class data_transformers:
    # To replace a given key in JSON filed
    def modify_key(obj):
        for key in obj.keys():
            new_key = key.replace("24h_volume_usd","volume_24h_usd")
            if new_key != key:
                obj[new_key] = obj[key]
                del obj[key]
        return obj

    # To transform Unix timestamp to date
    def unix_ts_to_date(unix_ts):
        transformed_date = datetime.datetime.utcfromtimestamp(
            int(unix_ts)
        ).strftime('%Y-%m-%d')
        return transformed_date

    # To transform CoinMarketCap date format to date
    def cmkp_to_date(date):
        transformed_date = datetime.datetime.strptime(date, '%b %d %Y').strftime('%Y-%m-%d')
        return transformed_date

    # To transform date to CoinMarketCap date format
    def date_to_cmkp(date):
        transformed_date = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%b %d, %Y')
        return transformed_date

    # Cleaning string
    def lower_underscore_to_string(self, string):
        string = string.lower().replace(" ", "_")
        return string

    # To fetch min & max date from fact_price_volume_historical_stats_daily
    def check_date(self, ticker_name):
        q = cur.mogrify('SELECT min(day), max(day) FROM fact_price_volume_stats_daily_historical WHERE id=%s', (ticker_name,))
        cur.execute(q)
        rows = cur.fetchall()
        if rows:
            return rows[0]

    # To select the min(date) to fetch for CoinMarketCap
    def cmkp_start_date(self, ticker_name):
        if data_transformers.check_date(self, ticker_name):
            min_date = data_transformers.check_date(self, ticker_name)[0]
            max_date = data_transformers.check_date(self, ticker_name)[1]

        if not min_date:
            start_date = Coinmarketcap.MIN_FETCHING_DATE
        else:
            start_date = max_date + datetime.timedelta(days=1)
            start_date = start_date.strftime('%Y%m%d')

        end_date = datetime.datetime.utcnow().today() + datetime.timedelta(days=1)
        end_date = end_date.date().strftime('%Y%m%d')
        return (start_date, end_date)

    # Parsing Data and storing it in a tuple
    def dict_to_tuple(self, fetched_data, selected_tuple):
        results = []
        for k in fetched_data:
            results.append(selected_tuple(**k))
        return results

    # HTML Table to Tuple
    def parse_html_table(table):
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
                    column_name = dt.lower_underscore_to_string(th.get_text())
                    column_names.append(column_name)

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
                    daily_values.append(column.get_text().replace(',',''))
                    column_marker += 1
                    if len(columns) > 0:
                        row_marker += 1
                daily_dict_values = dict(zip(column_names, daily_values))
                daily_dict_values['date'] = data_transformers.cmkp_to_date(daily_dict_values.get('date'))
                list_storage.append(daily_dict_values)
        return list_storage


# Functions writing in SQL
class data_writers:
    # write in Postgres fact_price_volume_stats_daily
    def fact_price_volume_stats_daily_writer(self, tuple):
        q = cur.mogrify('SELECT * FROM fact_price_volume_stats_daily WHERE id=%s and last_updated=%s', (tuple[0],tuple[8]))
        cur.execute(q)
        rows = cur.fetchall()

        #check if data has already been fetched
        if not rows:
            q = cur.mogrify('INSERT INTO crypto_db.public.fact_price_volume_stats_daily VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', tuple)
            try:
                cur.execute(q)
            except:
                print('Failed to insert line')
                pass

    # To write in Postgres fact_price_volume_stats_daily
    def fact_price_volume_stats_daily_historical_writer(self, tuple):
        q = cur.mogrify('SELECT * FROM fact_price_volume_stats_daily_historical WHERE day=%s and id=%s',
                        (tuple[0], tuple[7]))
        cur.execute(q)
        rows = cur.fetchall()

        # check if data has already been fetched
        if not rows:
            q = cur.mogrify('INSERT INTO crypto_db.public.fact_price_volume_stats_daily_historical VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', tuple)
            print(q)
            try:
                cur.execute(q)
            except:
                print('Failed to insert line')
                pass


# Set up shortcuts
df=data_fetchers()
dw=data_writers()
dt=data_transformers()


# # Fact_price_volume_stats_daily_historical

# Script to fetch the data
for ticker in config.crypto_tickers_name:
    get_date = dt.cmkp_start_date(ticker)
    table = df.html_fetcher(ticker, get_date[0], get_date[1])
    results = table[0][1]

    # Add ticker name to the dict and transform in tuple
    for day in results:
        day['id'] = ticker

    # Script to put data in a tuple
    tuple_results = dt.dict_to_tuple(results, Historical_ticker_info)

    # Script to write the data in Postgres SQL
    for tuple in tuple_results:
        dw.fact_price_volume_stats_daily_historical_writer(tuple)

# # # Fact_price_volume_stats_daily
# # Script to Fetch the Data
# api_json_fetched = []
# for ticker in config.crypto_tickers_name:
#     a = df.api_json_fetcher(ticker,
#                          ['percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'price_btc'])
#     print(a.get('last_updated'))
#     api_json_fetched.append(a)
# print(api_json_fetched)
#
# results_temp=[]
# for k in api_json_fetched:
#     results_temp.append(Daily_ticker_info(**k))
# print(results_temp)
#
# # Script to put the data into a tuple
# b = dt.dict_to_tuple(api_json_fetched, Daily_ticker_info)
# print(b)
#
# #Script to write the data in Postgres SQL
# for x in b:
#    dw.fact_price_volume_stats_daily_writer(x)