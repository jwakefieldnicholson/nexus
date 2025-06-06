import argparse
import numpy as np
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from time import sleep
import requests

import json
from os.path import join, exists, expanduser

from fredapi import Fred
import yfinance as yf

# from sqlalchemy import create_engine, text as sql_text
# import pymssql

##### Parse Command Line Arguments #####
parser = argparse.ArgumentParser()
parser.add_argument('-e', '--etf')
parser.add_argument('--etf-only', action='store_true')
parser.add_argument('--equity-only', action='store_true')
parser.add_argument('-l', '--list')
parser.add_argument('--list-only', action='store_true')
#parser.add_argument('snodfile')
#parser.add_argument('-a', '--append', action='store_true')
args = parser.parse_args()

##### Constants and Useful Definitions #####
DATA_DIR = 'data'
ETF_DATA_DIR = 'data_etf'
DATABASE_FILE = "ticker_database.csv"
EQUITIES_DATABASE_FILE = "equity_database.csv"
ETF_DATABASE_FILE = "etf_database.csv"
SNOD_DATABASE_FILE = "snod_database.csv"
TICKER_FILE = "ticker_list.csv"

FINVIZ_URL = "https://elite.finviz.com/export.ashx?[allYourFilters]&auth=9c016a28-02c7-47d1-b107-f42f9074cb69"
FINVIZ_FILE = "finviz_list.csv"       # Not used by default, to use uncomment line 412

TICKER_START_DATE = "2010-01-01"
TICKER_END_DATE = "2077-01-01"

MARKET_CAP_THRESHOLD = 2000       # Millions
MINIMUM_DATA_LENGTH = 20

ETF_LIST = ['SPY', 'QQQ', 'IWM',
            'XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY',
            'KBE', 'KRE', 'KIE',
            'XBI', 'XHE', 'XOP', 'XES', 'XME', 'XRT', 'XHB',
            'CSD', 'ITA', 'IYT', 'IBB', 'IHI', 'SOXX', 'IGV', 'ITB', 'PICK', 'ICF', 'ICLN', 'IYZ', 'OIH', 'BBH', 'RTH', 'SMH', 'GDX']
ETF_FILE = 'etf.csv'
#ETF_LIST_DF = pd.DataFrame({'Ticker' : ETF_LIST, 'Region' : 'US'})
#INDICIES = ['DJI', 'GSPC', 'IXIC', 'W5000', 'SP400', 'RMCC', 'W5KMC', 'RUT', 'SP600', 'DWSG', 'DWSV']

FRB_INDICIES = {                     # Key = FRB code, Value = Yahoo symbol
    'SP500' : 'GSPC',
    'DJIA' : 'DJI',
    'NASDAQCOM' : 'IXIC',
    #'WILL5000PR' : 'FTW5000',
    #'WILLMIDCAPPR' : 'W5KMC',
}

FRB_FILE = "fed_series.csv"
FRB_SERIES = ['BAMLHYH0A0HYM2TRIV','CPIAUCSL','DFF','DAAA','T10Y2Y','T10Y3M','PCUOMFGOMFG','UNRATE','CIVPART','TCU','INDPRO']
FRB_RATES = ['DGS3MO', 'DGS6MO', 'DGS1', 'DGS2', 'DGS3', 'DGS10']
FRB_API_KEY = "de3ad0dce2fc23c8a960e6e6ce097e34"

WINDOW_LENGTHS = {
    3 : 63,
    6 : 126,
    9 : 189,
    12 : 252,
    15 : 315,
    18 : 378,
    24 : 503,
}
VOLATILITY_WINDOW_LENGTHS = {
    3 : 63,
    6 : 126,
    9 : 189,
    12 : 252,
    15 : 315,
    18 : 378,
    24 : 378,
}
COUPON_FACTOR = {
    3 : 0.25,
    6 : 0.5,
    9 : 0.75,
    12 : 1,
    15 : 1.25,
    18 : 1.5,
    24 : 2,
}

##### Download Finviz universe #####
def get_ticker_list(url=FINVIZ_URL, data_dir=DATA_DIR, ticker_file=TICKER_FILE, save=True, market_cap_threshold=MARKET_CAP_THRESHOLD, specific_tickers=None):
    ticker_path = join(data_dir, ticker_file)

    cols = ['Ticker', 'Company', 'Country', 'Sector', 'Industry', 'MarketCap']
    
    response = requests.get(url)
    open("export.csv", "wb").write(response.content)
    response = pd.read_csv("export.csv")
    query_data = response.copy()
    query_data.rename(columns={'Market Cap' : 'MarketCap'}, inplace=True)
    query_data.fillna({'MarketCap' : 0}, inplace=True)
    query_data.sort_values(['Ticker'], inplace=True, ignore_index=True)
    
    if market_cap_threshold is not None:
        query_data = query_data.loc[query_data.MarketCap >= market_cap_threshold]
    if specific_tickers is not None:
        f = [k in specific_tickers for k in query_data.Ticker.values]
        query_data = query_data.loc[f]
    
    if save:
        query_data[cols].to_csv(ticker_path, index=False)
    
    return query_data[cols]

def download_ticker_data(ticker_list, start_date=TICKER_START_DATE, end_date=TICKER_END_DATE, data_dir=DATA_DIR, min_history=MINIMUM_DATA_LENGTH):
    for idx, ticker in zip(range(len(ticker_list)), ticker_list):
        if (idx%200) == 0:
            print("Resetting Yahoo! throttling, wait 15 seconds        \t", end="\r")
            sleep(15)
        print(f"{idx+1}/{len(ticker_list)} - {ticker}                                                                               \t", end="\r")
        data_path = join(data_dir, f'{ticker}_data.csv')
        try:
            data = yf.Ticker(ticker).history(start=start_date).reset_index()
            try:
                if len(data) >= min_history:
                    data.to_csv(data_path, index=False)
                else:
                    print(f'{ticker} - Insufficient historical data')
            except:
                print(f'{ticker} - Invalid data file')
        except:
            print(f'{ticker} does not exist')

def get_snod_tickers(filepath):
    if exists(filepath):
        with open(filepath, 'r') as f:
            snod = json.load(f)
            snod_tickers = list(snod['Ref. Asset'].values())
            snod_ticker_df = pd.DataFrame({'Ticker' : snod_tickers})
            snod_ticker_df.loc[:, 'Ticker'] = snod_ticker_df.loc[:, 'Ticker'].str.replace('_', '-')
            download_ticker_data(snod_ticker_df.Ticker)
        return snod_tickers
    else:
        print(f'Warning: Could not locate file {filepath}, skipping SNOD download.')
        return []

##### Download FRB data #####
def get_frb_series(data_dir=DATA_DIR, frb_file=FRB_FILE, frb_series=FRB_RATES, api_key=FRB_API_KEY, save=True, file_per_series=True, prefix=''):
    fred = Fred(api_key=api_key)
    frb_list = []
    for s in frb_series:
        series = fred.get_series(s)
        f = series.index >= '2010-01-01'
        frb_list.append(series[f])
        if save and file_per_series:
            series_file = f"{prefix}{s}.csv"
            series_path = join(data_dir, series_file)
            series.to_csv(series_path)
    frb = pd.concat(frb_list, axis=1)
    frb.columns = frb_series
    if save and not file_per_series:
        frb_path = join(data_dir, frb_file)
        frb.to_csv(frb_path)
    return frb

def download_specific_tickers(ticker_arg, data_dir=DATA_DIR, save=True):
    if ticker_arg is not None:
        ticker_list_df = pd.read_csv(ticker_arg)
        if 'Ticker' not in ticker_list_df.columns:
            raise ValueError("Column 'Ticker' not found in Tickers file.")
        
        specific_tickers = ticker_list_df.Ticker.values
        ticker_list = get_ticker_list(data_dir=data_dir, market_cap_threshold=None, specific_tickers=specific_tickers)
        
        download_ticker_data(ticker_list.Ticker, data_dir=data_dir)
        return ticker_list
    else:
        return pd.DataFrame()

def download_etfs(etf_arg, etf_list=ETF_LIST, etf_file=ETF_FILE, etf_dir=ETF_DATA_DIR, save=True):
    if etf_arg is not None:
        etf_list_df = pd.read_csv(etf_arg)
        if 'Ticker' not in etf_list_df.columns:
            raise ValueError("Column 'Ticker' not found in ETF file.")
        if 'Region' not in etf_list_df.columns:
            etf_list_df['Region'] = 'US'
    
    elif exists(etf_file):
        etf_list_df = pd.read_csv(etf_file)
        if 'Ticker' not in etf_list_df.columns:
            raise ValueError("Column 'Ticker' not found in ETF file.")
        if 'Region' not in etf_list_df.columns:
            etf_list_df['Region'] = 'US'
    
    else:            
        etf_list_df = pd.DataFrame({'Ticker' : etf_list, 'Region' : 'US'})
        if save:
            etf_list_df.to_csv(etf_file, index=False)
    
    download_ticker_data(etf_list_df.Ticker, data_dir=etf_dir)
    return etf_list_df

def download_indicies(indicies=FRB_INDICIES, start_date=TICKER_START_DATE, data_dir=DATA_DIR, api_key=FRB_API_KEY, use_ohlc_format=True, prefix=''):
    fred = Fred(api_key=api_key)
    for k, v in indicies.items():
        print(k)
        series = fred.get_series(k)
        
        series_df = series[series.index>=start_date].reset_index()
        series_df.columns = ['Date', 'Close']
        series_df.dropna(inplace=True)

        if use_ohlc_format:
            series_df['Open'] = pd.NA
            series_df['High'] = pd.NA
            series_df['Low'] = pd.NA
            series_df['Volume'] = pd.NA
            series_df['Dividends'] = pd.NA
            series_df['Stock Splits'] = pd.NA
            series_df = series_df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']]

        series_file = f"{v}_data.csv"
        series_path = join(data_dir, series_file)
        series_df.to_csv(series_path, index=False)

##### Update the database of tickers #####
def update_database(ticker_list, frb=None, db_file=DATABASE_FILE, data_dir=DATA_DIR, append_database=False, save=True):
    if frb is None:
        get_frb_series(frb_series=FRB_RATES)
    db_path = join(data_dir, db_file)
    db = ticker_list[['Ticker', 'Sector', 'MarketCap']]
    db.loc[:, 'Ticker'] = db.loc[:, 'Ticker'].str.replace('.', '_')
    db.loc[:, 'Ticker'] = db.loc[:, 'Ticker'].str.replace('-', '_')
    if append_database and exists(db_path):
        old_db = pd.read_csv(db_path)
        f = [k not in db.Ticker.values for k in old_db.Ticker.values]
        db = pd.concat([db, old_db[f]])
        db.sort_values('Ticker', ignore_index=True, inplace=True)
    db.to_csv(db_path, index=False)

##### Main script #####
if __name__ == '__main__':
    get_frb_series(frb_series=FRB_RATES)
    get_frb_series(frb_series=FRB_SERIES)
    download_indicies()

'''
##### Main script #####
if __name__ == '__main__':
    if args.equity_only:
        ticker_list = get_ticker_list()
        download_ticker_data(ticker_list.Ticker)
        get_frb_series(frb_series=FRB_RATES)
        get_frb_series(frb_series=FRB_SERIES)
        download_indicies()
        update_database(ticker_list, db_file=EQUITIES_DATABASE_FILE)
    elif args.etf_only:
        etf_list = download_etfs(args.etf)
        etf_list['Sector'] = 'ETF'
        etf_list['MarketCap'] = np.nan
        get_frb_series(frb_series=FRB_RATES, data_dir=ETF_DATA_DIR)
        get_frb_series(frb_series=FRB_SERIES, data_dir=ETF_DATA_DIR)
        download_indicies(data_dir=ETF_DATA_DIR)
        update_database(etf_list, db_file=ETF_DATABASE_FILE, data_dir=ETF_DATA_DIR)
    elif args.list_only:
        ticker_list = download_specific_tickers(args.list)
        get_frb_series(frb_series=FRB_RATES)
        get_frb_series(frb_series=FRB_SERIES)
        download_indicies()
        update_database(ticker_list)
    else:
        ticker_list = get_ticker_list()
        download_ticker_data(ticker_list.Ticker)
        etf_list = download_etfs(args.etf)
        specified_ticker_list = download_specific_tickers(args.list)
        download_indicies()
        get_frb_series(frb_series=FRB_RATES)
        get_frb_series(frb_series=FRB_SERIES)
        db_list = pd.concat([ticker_list, etf_list, specified_ticker_list])
        update_database(db_list)
'''
