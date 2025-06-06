import io
import pandas as pd
from fredapi import Fred
import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
import pandas as pd
from fredapi import Fred

def get_frb_series(series_codes, api_key, start_date='2010-01-01', end_date=None):
    """
    Fetch multiple time series from the Federal Reserve Economic Data (FRED).
    
    Parameters:
    -----------
    series_codes : list
        List of FRED series codes to retrieve
    api_key : str
        FRED API key
    start_date : str, optional
        Start date in 'YYYY-MM-DD' format
    end_date : str, optional
        End date in 'YYYY-MM-DD' format (default: None/today)
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with all requested series as columns
    """
    fred = Fred(api_key=api_key)
    
    try:
        # Use dictionary comprehension to fetch all series
        series_dict = {
            code: fred.get_series(code, observation_start=start_date, observation_end=end_date)
            for code in series_codes
        }
        
        # Combine into a single DataFrame
        return pd.DataFrame(series_dict)
        
    except Exception as e:
        print(f"Error fetching FRED data: {e}")
        return None

def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = ''
    response = requests.get(url)

    return pd.read_csv(io.StringIO(response.text), sep=',')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'