import datetime
import pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo

def find_closest_credit(df: pd.DataFrame, target_credit: float, bidask: str = 'ask'):
    if bidask not in ['bid','ask']:
        return
    return df.iloc[(df[bidask] - target_credit).abs().argsort()[0]]

def find_closest_delta(df: pd.DataFrame, target_delta = -0.15):
    return df.iloc[(df['delta'] - target_delta).abs().argsort()[0]]

def find_closest_strike(df: pd.DataFrame, target_strike: float, right: str):
    if right not in ['P','C']:
        return
    df = df.loc[df['right'] == right]
    return df.iloc[(df['strike'] - target_strike).abs().argsort()[0]]

def get_date_today(tz : str = "US/Eastern") -> str:
    """Return today date in yyyymmdd format"""
    dt = datetime.datetime.now(ZoneInfo(tz))
    return dt.date().strftime("%Y%m%d")

def convert_str_date(date: str) -> datetime.datetime:
    """convert string (format: 20230623) to date"""
    return datetime.datetime.strptime(date,'%Y-%m-%d') 

def get_nearest_expiry_from_today(expiries: list, dte: int):
    """Given DTE from today, find the date from the list of expiration date"""
    date = convert_str_date(get_date_today()) # needs %Y%m%d
    target_date = date + timedelta(days = dte)
    nearest_date = min(expiries, key=lambda x: abs(convert_str_date(x) - target_date))
    difference = convert_str_date(nearest_date) - target_date
    return nearest_date