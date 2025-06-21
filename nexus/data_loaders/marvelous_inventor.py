import io
import pandas as pd
import requests
from pandas import DataFrame
import intrinio_sdk as intrinio
from intrinio_sdk.rest import ApiException
from datetime import datetime
from dateutil.relativedelta import relativedelta
import concurrent.futures
from tqdm import tqdm
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


#get Intrinio API Key secret
API_KEY = get_secret_value('Intrinio_API')

today = datetime.now()
five_years_ago = (today - relativedelta(years=3)).strftime("%Y-%m-%d")
results_dictionary = {}

intrinio.ApiClient().set_api_key(API_KEY) 
intrinio.ApiClient().allow_retries(True)

def get_universe():
    date = '2025-05-14'  # Yesterday's date
    page_size = 100
    start_time = datetime.now()

    # Get Marketcap data with pagination
    marketcap_data = []
    next_page = ''

    while True:
        try:
            response = intrinio.CompanyApi().get_all_companies_daily_metrics(
                on_date=date, 
                page_size=page_size,
                next_page=next_page
            )
            
            print(f'Processing {len(response.daily_metrics)} marketcap entries...')
            
            # Process current page of results
            for daily_metric in response.daily_metrics:
                ticker = daily_metric.company.ticker
                marketcap = daily_metric.market_cap
                
                data = {
                    "ticker" : ticker, 
                    "name": daily_metric.company.name,
                    "date": daily_metric.date.strftime("%Y-%m-%d"),
                    "marketcap": marketcap
                }
                
                marketcap_data.append(data)
            
            # Check if there are more pages
            next_page = response.next_page
            if not next_page:
                break
                
        except ApiException as e:
            print(f"Exception when calling CompanyApi->get_all_companies_daily_metrics: {e}")
            break

    print(f'Found {len(marketcap_data)} marketcap entries for: {date}')
    print(f'Time elapsed: {datetime.now() - start_time}')

    universe = pd.DataFrame(list(marketcap_data))
    selected_universe = universe[((universe.marketcap>2000000000) & (universe.ticker.notna()))]
    
    return selected_universe

def work(ticker):
    # Get EOD Stock Prices with pagination
    # https://docs.intrinio.com/documentation/python/get_security_stock_prices_v2
    identifier = ticker
    start_date = five_years_ago
    page_size = 100  # Maximum allowed page size
    total_prices = 0
    
    try:
        # Initialize pagination
        next_page = ''
        
        while True:
            # Get the current page of results
            response = intrinio.SecurityApi().get_security_stock_prices(
                identifier, 
                start_date=start_date, 
                page_size=page_size, 
                next_page=next_page
            )
            
            security = response.security
            page_prices = len(response.stock_prices)
            total_prices += page_prices
            
            # Process the current page of stock price data
            for stock_price in response.stock_prices:
                key = f'{ticker}|{stock_price.date}'
                data = {
                    'security_id': security.id,
                    'company_id': security.company_id,
                    'ticker': security.ticker,
                    'date': stock_price.date,
                    'open': stock_price.open,
                    'high': stock_price.high,
                    'low': stock_price.low,
                    "close": stock_price.close,
                    'adj_open': stock_price.adj_open,
                    'adj_high': stock_price.adj_high,
                    "adj_low": stock_price.adj_low,
                    "adj_close": stock_price.adj_close,
                    'adj_volume':stock_price.adj_volume,
                    'fifty_two_week_high' : stock_price.fifty_two_week_high,
                    'fifty_two_week_low':stock_price.fifty_two_week_low,
                    'dividend': stock_price.dividend
                }
                results_dictionary[key] = data
            
            # Check if there are more pages
            next_page = response.next_page
            if not next_page:
                break
                
        print(f'Found {total_prices} prices for: {ticker}')
        
    except ApiException as e:
        print(f"Exception when calling SecurityApi->get_security_stock_prices for {ticker}: {e}")


@data_loader
def load_data_from_api(**kwargs) -> DataFrame:

    start_time = datetime.now()

    selected_universe = get_universe()
    tickers = selected_universe.ticker.tolist()
    tickers = ['AAPL', 'IBM'] #DELETE AFTER TESTING
    print(f'Loading {len(tickers)} tickers') 
    print(tickers)

    for ticker in tqdm(tickers):
        work(ticker)
 
    df = pd.DataFrame(list(results_dictionary.values()))
    print(len(df.ticker.unique()))
    
    return(df)