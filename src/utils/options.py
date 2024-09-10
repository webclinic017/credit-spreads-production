import pandas as pd

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
