
import unittest
from crypto_tax_compiler.main_file import data_fetchers, data_transformers
import datetime

class TestFetcher(unittest.TestCase):

    def test_html_fetcher(self):
        # Not a test, just a display of how this method works
        df = data_fetchers()
        ticker = 'bitcoin'
        start_date = (datetime.datetime.utcnow().today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
        end_date = start_date
        res = df.html_fetcher(ticker, start_date, end_date)
        print(res)


if __name__ == '__main__':
    unittest.main()