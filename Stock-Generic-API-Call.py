import requests #To access http requests
import pandas as pd #To store the Data Frame
from datetime import datetime #To d
import numpy as np
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

tickerslist = pd.read_csv('YOUR DIRECTORY HERE') #Get a list of stock tickers that you can import
#If you do not have a list of tickers, either download one or create one. This variable has to be either a pandas series, numpy array, or list

API_KEY = "YOUR API KEY HERE" #Make sure to get a key from whatever API you are calling

BASE_URL = "YOUR URL HERE" #Make sure the API gives you a base url to use

def get_info(ticker: str, years: int = 3):

    end_date = datetime(2025, 1, 7, 0, 0, 0, 0) #change these dates accordingly
    start_date = datetime(2022, 1, 7, 0, 0, 0, 0)

    url = f"{BASE_URL}/INSERT NECESSARY URL HERE/{ticker}/INSERT NECESSARY URL HERE/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
    """
    THE URL GIVEN ABOVE IS JUST A GENERIC FRAMEWORK, ADJUST FOR YOUR API IN PARTICULAR, IT WILL USUALLY LOOK SOMETHING LIKE THIS
    """
    params = {'YOUR PARAMETER(S) HERE': 'VALUE'} #Make sure to change your parameters accordingly, one parameter should always be the API key
    total_vol = []
    total_time = []
    total_vw = []
    total_open = []
    total_close = []
    total_low = []
    total_high = [] #For each piece of data you want to extract, create a list for it, this is a good starting example of important stock values
    #You will get totally different data to call if you are working with options, adjust accordingly


    while url: #JSON files work with providing next url parameters to secure more data, which is the reasoning for this while loop

        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=5,
            status_forcelist=[429, 500, 502, 503, 504] #Error checking for establishing connections, a code of 200 means we have an established connection
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)

        try:
            response = session.get(url, params = params, timeout = 10) #To allow for retrying in case of an overload of requests
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
            print('Unexpected Error: {e} occurred THIS SHOULD NOT POP UP') #The try-except blocks are to show you any possible errors you have made.
            return None

        data = response.json() #The sourced data for the respective ticker

        if "results" not in data or not data["results"]: #Your API might not call it 'results', adjust accordingly
            print("No data here")
            return [],[],[],[],[],[],[] #Make sure to adjust the lengths here according to how many data pieces you are sourcing. This is necessary to add empty values to a data frame

        volume = [item['v'] for item in data["results"]]
        time = [item['t'] for item in data['results']]
        vw = [item['vw'] for item in data['results']]
        open = [item['o'] for item in data['results']]
        close = [item['c'] for item in data['results']]
        high = [item['h'] for item in data['results']] 
        low = [item['l'] for item in data['results']] #adjust accordingly dependening on what you source, this means changing the names of any key-value pairs
        
        total_vol += volume
        total_time += time
        total_vw += vw
        total_close += close
        total_open += open
        total_high += high
        total_low += low

        url = data.get('next_url') #Make sure it is called 'next_url' for you to access the next page, otherwise adjust accordingly
        
    
    print(ticker)
    return total_vol, total_time, total_vw, total_open, total_close, total_high, total_low


FinalDF = pd.DataFrame()
FinalDF['tickers'] = tickerslist #adds tickers to Data Frame

def fetch_stock_data(ticker):
    return ticker, get_info(ticker)

results = list(map(fetch_stock_data, tickerslist[:])) #adjust the slice for whatever you deem is appropriate


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
            "High": high_prices[i], #Adjust accordingly according to your get_info function above
            "Low": low_prices[i]
        })
    print(ticker)    

FinalDF = pd.DataFrame(all_data)

FinalDF.to_csv('YOUR DIRECTORY HERE/FILE.csv')

