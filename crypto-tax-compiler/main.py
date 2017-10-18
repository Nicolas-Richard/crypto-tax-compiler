import psycopg2
import requests
import configparser
import json
import os
import sys
import datetime
#sys.path.insert(0, '../config_directoy')
import config_directory.config_file as config
from collections import namedtuple
from urllib.parse import urljoin

# To get access to the current path of the script
# os.path.abspath(os.path.dirname(sys.argv[0]))

# Constants
class Coinmarketcap:
    DOMAIN_NAME="https://api.coinmarketcap.com/v1/ticker/"

# Defining tuple to hold data from API
Daily_ticker_info = namedtuple('Daily_ticker_info','id, name, symbol, rank, price_usd, market_cap_usd, available_supply, total_supply, last_updated, volume_24h_usd')

# Columns
table_columns_name=('id, name, symbol, rank, price_usd, market_cap_usd, available_supply, total_supply, last_updated, volume_24h_usd')
database_name = table_columns_name.replace("'", "")

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
    def api_json_fetcher(api, ticker, unwanted=[]):
        url = urljoin(api, ticker)
        response = requests.get(url)

        # Modify json to make it tuple compliant
        response = json.loads(response.content, object_hook=data_transformers.modify_key)

        # Remove useless keys
        for unwanted_key in unwanted:
            for x in response:
                if unwanted_key in x:
                    del x[unwanted_key]

        response = response[0]
        return response



# Functions transforming fetched data
class data_transformers:
    #To replace a given key in JSON filed
    def modify_key(obj):
        for key in obj.keys():
            new_key = key.replace("24h_volume_usd","volume_24h_usd")
            if new_key != key:
                obj[new_key] = obj[key]
                del obj[key]
        return obj


    # Parsing Data and storing it in a tuple
    def dict_to_daily_ticker_tuple(api_json_fetched):
        results=[Daily_ticker_info(**k) for k in api_json_fetched]
        return results



# Functions writing in SQL
class data_writer:
    # Write in Postgres Table
    def fact_price_volume_stats_daily_writer(tuple):
        q = cur.mogrify('SELECT * FROM fact_price_volume_stats_daily WHERE id=%s and last_update=%s', (tuple[0],tuple[8]))
        cur.execute(q)
        rows = cur.fetchall()

        #check if data has already been fetched
        if not rows:
            q = cur.mogrify('INSERT INTO crypto_db.public.fact_price_volume_stats_daily VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', tuple)
            try:
                cur.execute(q)
            except:
                print('Failed to insert line')



# Script to Fetch the Data
api_json_fetched = []
for ticker in config.crypto_tickers_name:
    a = api_json_fetcher(Coinmarketcap.DOMAIN_NAME, ticker,
                         ['percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'price_btc'])
    print(type(a))
    api_json_fetched.append(a)
print(api_json_fetched)

# Script to put the data into a tuple
b = data_transformers.dict_to_daily_ticker_tuple(api_json_fetched)
print(b)

# Script to write the data in Postgres SQL
for x in b:
    data_writer.fact_price_volume_stats_daily_writer(x)