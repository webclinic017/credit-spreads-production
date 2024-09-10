
import requests
import pandas as pd
from pandas_market_calendars import get_calendar

def get_ticker_data(ticker: str, start_date: str, end_date: str, timespan: str = "day", polygon_api_key= None, multiplier = 1, limit = 50000) -> pd.DataFrame:
    endpoint = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}?adjusted=true&sort=asc&limit={limit}&apiKey={polygon_api_key}"
    return pd.json_normalize(requests.get(endpoint).json()["results"]).set_index("t")

def get_option_chain(ticker: str, 
                     contract_type: str, 
                     date:str, 
                     exp_date:str, 
                     limit:int = 1000, 
                     polygon_api_key=None) -> pd.DataFrame:
    endpoint = f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={ticker}&contract_type={contract_type}&as_of={date}&expiration_date={exp_date}&limit={limit}&apiKey={polygon_api_key}"
    return pd.json_normalize(requests.get(endpoint).json()["results"])

def get_historical_option_contracts(options_ticker, 
                                    as_of, 
                                    exp_date, 
                                    contract_type, 
                                    polygon_api_key = None, 
                                    limit = 1000) -> pd.DataFrame:
    endpoint = f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={options_ticker}&contract_type={contract_type}&as_of={as_of}&expiration_date={exp_date}&limit={limit}&apiKey={polygon_api_key}"
    return pd.json_normalize(requests.get(endpoint).json()["results"])

def schedule_trading_dates(exchange, start_date, end_date) -> list[str]:
    """Produces list of available trading dates for a given exchange"""
    calendar = get_calendar(exchange)
    return calendar.schedule(start_date = start_date, end_date = end_date).index.strftime("%Y-%m-%d").values

def get_ticker_quote(ticker, start, end, polygon_api_key=None) -> float:
    """Get mid price at a specfic price range"""
    quotes = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/quotes/{ticker}?timestamp.gte={start}&timestamp.lt={end}&order=asc&limit=5000&sort=timestamp&apiKey={polygon_api_key}").json()["results"]).set_index("sip_timestamp")
    quotes.index = pd.to_datetime(quotes.index, unit = "ns", utc = True).tz_convert("America/New_York")
    quote = quotes.median(numeric_only=True).to_frame().copy().T
    quote["mid_price"] = (quote["bid_price"] + quote["ask_price"]) / 2
    return quote["mid_price"].iloc[0]

def initial_spread(short_ticker, long_ticker, start, end, polygon_api_key=None) -> float:
    """Short call/put spread price"""
    short_leg_mid = get_ticker_quote(short_ticker, start, end, polygon_api_key=polygon_api_key)
    long_leg_mid = get_ticker_quote(long_ticker, start, end , polygon_api_key=polygon_api_key)
    return short_leg_mid - long_leg_mid # short_call_quote["bid_price"].iloc[0] - long_call_quote["ask_price"].iloc[0]

def get_latest_ticker_quote(ticker, polygon_api_key=None):
    """For streaming latest mid-price quote of option ticker"""
    quotes = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/quotes/{ticker}?order=desc&limit=100&sort=timestamp&apiKey={polygon_api_key}").json()["results"]).set_index("sip_timestamp")
    quotes.index = pd.to_datetime(quotes.index, unit = "ns", utc = True).tz_convert("America/New_York")
    quotes["mid_price"] = (quotes["bid_price"] + quotes["ask_price"]) / 2
    return quotes.index[0], quotes["mid_price"].iloc[0]

def stream_spread_quote(short_ticker, long_ticker, polygon_api_key = None):
    short_ts, short_leg_mid = get_latest_ticker_quote(short_ticker, polygon_api_key=polygon_api_key)
    long_ts, long_leg_mid = get_latest_ticker_quote(long_ticker, polygon_api_key=polygon_api_key)
    return short_ts, short_leg_mid - long_leg_mid
