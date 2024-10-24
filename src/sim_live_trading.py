""" Short 0DTE call/put spread 

with a directional regime indicator, short the opposite direction spread at x times of expected move from spot.

This code simulates live trading using Polygon API. 
For IBKR integration please refer to the _ibkr script.

Reference:
https://polygon.io/docs/options/get_v3_quotes__optionsticker 

"""

import time
import requests
import pandas as pd
import numpy as np
from dotenv import dotenv_values
from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar
from termcolor import cprint
from utils.polygon import get_ticker_data, get_option_chain, get_historical_option_contracts, get_ticker_quote
from utils.date_util import schedule_trading_dates


def initial_spread(short_ticker, long_ticker, start, end, polygon_api_key=None):
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

config = dotenv_values(".env")
polygon_api_key = config['POLYGON_API_KEY']

trading_dates = schedule_trading_dates("NYSE", "2024-05-01", (datetime.today()-timedelta(days = 1)))

ticker = "I:SPX"
vix_ticker = "I:VIX1D"
options_ticker = "SPX"
underlying_ticker = "SPY"

date = trading_dates[-1]

# calculate previous day's market variables
# calculate vix1D
vix_data = get_ticker_data(vix_ticker, "2024-01-01", date, polygon_api_key=polygon_api_key)
vix_data.index = pd.to_datetime(vix_data.index, unit="ms", utc=True).tz_convert("America/New_York")
vix_data["1_mo_avg"] = vix_data["c"].rolling(window=20).mean()
vix_data["3_mo_avg"] = vix_data["c"].rolling(window=60).mean()
vix_data['vol_regime'] = vix_data.apply(lambda row: 1 if (row['1_mo_avg'] > row['3_mo_avg']) else 0, axis=1)
vix_data["str_date"] = vix_data.index.strftime("%Y-%m-%d")
vol_regime = vix_data["vol_regime"].iloc[-1]
cprint(f"Vol regime: {vol_regime}, vix last close: {vix_data['c'].iloc[-1]}", "green")
# underlying trend regime
# TODO: why SPY instead of SPX here.
hist_underlying_data = get_ticker_data(underlying_ticker, "2024-01-01", date, polygon_api_key=polygon_api_key)
hist_underlying_data.index = pd.to_datetime(hist_underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")
hist_underlying_data["1_mo_avg"] = hist_underlying_data["c"].rolling(window=20).mean()
hist_underlying_data["3_mo_avg"] = hist_underlying_data["c"].rolling(window=60).mean()
hist_underlying_data['regime'] = hist_underlying_data.apply(lambda row: 1 if (row['c'] > row['1_mo_avg']) else 0, axis=1)
trend_regime = hist_underlying_data['regime'].iloc[-1]
cprint(f"Trend regime: {trend_regime}", "green")
# real time trades
calendar = get_calendar("NYSE")
real_trading_dates = calendar.schedule(start_date = (datetime.today()-timedelta(days=10)), end_date = (datetime.today())).index.strftime("%Y-%m-%d").values

today = real_trading_dates[-1]
exp_date = today

# get market open quote timestamp
quote_start_timestamp = (pd.to_datetime(today).tz_localize("America/New_York") + timedelta(hours = pd.Timestamp("09:35").time().hour, minutes = pd.Timestamp("09:35").time().minute)).value
quote_end_timestamp = (pd.to_datetime(today).tz_localize("America/New_York") + timedelta(hours = pd.Timestamp("09:36").time().hour, minutes = pd.Timestamp("09:36").time().minute)).value

trade_list = [] 
side = "call" if trend_regime == 0 else "put"

# monitoring during trading
while True:
    try:
        underlying_data = get_ticker_data(ticker, start_date = today, end_date= today, timespan = "minute", polygon_api_key=polygon_api_key)
        underlying_data.index = pd.to_datetime(underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")

        live_vix_data = get_ticker_data(vix_ticker, start_date = today, end_date= today, timespan = "minute", polygon_api_key=polygon_api_key)
        live_vix_data.index = pd.to_datetime(live_vix_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        
        index_price = live_vix_data[live_vix_data.index.time >= pd.Timestamp("09:35").time()]["c"].iloc[0]
        price = underlying_data[underlying_data.index.time >= pd.Timestamp("09:35").time()]["c"].iloc[0]
        
        expected_move = (round((index_price / np.sqrt(252)), 2)/100)*.50

        lower_price = round(price - (price*expected_move))
        upper_price = round(price + (price*expected_move))

        valid_chain = get_option_chain(options_ticker, side, today, exp_date, polygon_api_key=polygon_api_key)
        valid_chain = valid_chain[valid_chain["ticker"].str.contains("SPXW")].copy() # get weekly options only
        valid_chain["days_to_exp"] = (pd.to_datetime(valid_chain["expiration_date"]) - pd.to_datetime(date)).dt.days
        valid_chain["distance_from_price"] = abs(valid_chain["strike_price"] - price) 

        if trend_regime == 0:
            otm_chain = valid_chain[valid_chain["strike_price"] >= upper_price]
        elif trend_regime == 1: 
            otm_chain = valid_chain[valid_chain["strike_price"] <= lower_price].sort_values("distance_from_price", ascending = True)
        
        short_leg = otm_chain.iloc[[0]]
        long_leg = otm_chain.iloc[[1]] # 1 tick width
        short_ticker = short_leg['ticker'].iloc[0]
        long_ticker = long_leg['ticker'].iloc[0]
        short_strike = short_leg["strike_price"].iloc[0]
        long_strike = long_leg["strike_price"].iloc[0]
        
        init_spread_value = initial_spread(short_ticker, long_ticker, start = quote_start_timestamp, end = quote_end_timestamp, polygon_api_key=polygon_api_key)
        quote_ts, updated_spread_value = stream_spread_quote(short_ticker, long_ticker, polygon_api_key=polygon_api_key)

        if trend_regime == 0:
            underlying_data["distance_from_short_strike"] = round(((short_strike - underlying_data["c"]) / underlying_data["c"].iloc[0])*100, 2)
        elif trend_regime == 1:
            underlying_data["distance_from_short_strike"] = round(((underlying_data["c"] - short_strike) / short_strike)*100, 2)
            
        gross_pnl = init_spread_value - updated_spread_value
        gross_pnl_percent = round((gross_pnl / init_spread_value)*100,2)
            
        cprint(f"Live PnL: ${round(gross_pnl*100,2)} | {gross_pnl_percent}% | {quote_ts.strftime('%H:%M')}", "green")
        cprint(f"initial premium: {round(init_spread_value,2)} | current spread value: {round(updated_spread_value,2)}", "yellow")
        print(f"Side: {side} | Short Strike: {short_strike} | Long Strike: {long_strike} | % Away from strike: {underlying_data['distance_from_short_strike'].iloc[-1]}% | spot: {underlying_data['c'].iloc[-1]}")

        time.sleep(10)
        
    except Exception as e:
        cprint(e, "red")
        continue