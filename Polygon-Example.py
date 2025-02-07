import requests
import pandas as pd
from datetime import datetime
import numpy as np
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

tickerslist = pd.read_csv('YOUR DIRECTORY') #make your own ticker list

API_KEY = "YOUR KEY" #acquire an qpi key

BASE_URL = "https://api.polygon.io" #url for polygon.io

def get_info(ticker: str, years: int = 3):

    today = datetime(2025, 1, 7, 0, 0, 0, 0)
    start_date = datetime(2022, 1, 7, 0, 0, 0, 0)

    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/5/minute/{start_date.strftime('%Y-%m-%d')}/{today.strftime('%Y-%m-%d')}" #exact syntax to call Polygon for 5 minute data
    params = {'apiKey': API_KEY, 'adjusted': 'false', 'sort': 'asc'}
    total_vol = []
    total_time = []
    total_vw = []
    total_open = []
    total_close = []
    total_low = []
    total_high = []


    while url:

        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)

        try:
            response = session.get(url, params = params, timeout = 10)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            print('Timeout')
            return None
        except requests.exceptions.ConnectionError:
            print('Connection Error')
            return None
        except requests.exceptions.HTTPError as err:
            print(f"HTTP Error {err}")
            return None
        except Exception as e:
            print('Unexpected Error: {e} occurred THIS SHOULD NOT POP UP')
            return None
    
        data = response.json()

        if "results" not in data or not data["results"]:
            print("No data here")
            return [],[],[],[],[],[],[]

        volume = [item['v'] for item in data["results"]]
        time = [item['t'] for item in data['results']]
        vw = [item['vw'] for item in data['results']]
        open = [item['o'] for item in data['results']]
        close = [item['c'] for item in data['results']]
        high = [item['h'] for item in data['results']]
        low = [item['l'] for item in data['results']]
        
        total_vol += volume
        total_time += time
        total_vw += vw
        total_close += close
        total_open += open
        total_high += high
        total_low += low

        url = data.get('next_url') #All of this code is the same as the template
        
    
    print(ticker)
    return total_vol, total_time, total_vw, total_open, total_close, total_high, total_low


FinalDF = pd.DataFrame()
FinalDF['tickers'] = tickerslist

def fetch_stock_data(ticker):
    return ticker, get_info(ticker)

results = list(map(fetch_stock_data, tickerslist[:])) #adjust slice to preferences


all_data = []

for ticker, data in results:
    vol, timestamp_unix, vw, open_prices, close_prices, high_prices, low_prices = data
    times_datetime = [datetime.fromtimestamp(ts / 1000) for ts in timestamp_unix]
    for i, time in enumerate(times_datetime):
        all_data.append({
            "ticker": ticker,
            "timestamp": time,
            "Volume": vol[i],
            "WeightVolume": vw[i],
            "Open": open_prices[i],
            "Close": close_prices[i],
            "High": high_prices[i],
            "Low": low_prices[i]
        })
    print(ticker)    

FinalDF = pd.DataFrame(all_data)

FinalDF.to_csv('YOUR DIRECTORY HERE/file.csv')

