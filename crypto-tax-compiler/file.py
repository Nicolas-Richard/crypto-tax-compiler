import os,sys
import config
import urllib.parse

print(urllib.parse)

#Access to the current path of the script
print(os.path.abspath(os.path.dirname(sys.argv[0])))

print(config.crypto_tickers)

