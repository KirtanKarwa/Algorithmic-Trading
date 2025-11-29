import pandas as pd
import requests
import time

def get_historical_klines_df(symbol, interval="15m", start=None, end=None):
    """
    Fetch historical klines between start and end dates (inclusive).
    Returns DataFrame with open, high, low, close, volume
    """
    url = "https://api.binance.com/api/v3/klines"
    limit = 1000
    start_ts = int(pd.Timestamp(start).timestamp() * 1000)
    end_ts = int(pd.Timestamp(end).timestamp() * 1000)
    all_data = []

    while True:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
            "startTime": start_ts,
            "endTime": end_ts
        }
        resp = requests.get(url, params=params)
        data = resp.json()
        if not data:
            break

        all_data.extend(data)
        last_time = data[-1][0]
        if last_time >= end_ts or len(data) < limit:
            break
        start_ts = last_time + 1
        time.sleep(0.2)

    df = pd.DataFrame(all_data, columns=[
        "timestamp","open","high","low","close","volume",
        "close_time","quote_asset_volume","number_of_trades",
        "taker_buy_base","taker_buy_quote","ignore"
    ])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("timestamp", inplace=True)
    df = df[["open","high","low","close","volume"]].astype(float)
    return df

def get_klines(symbol, interval="1m", limit=100):
    """Helper for live/multi-coin trading"""
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    resp = requests.get(url, params=params)
    data = resp.json()
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=[
        "timestamp","open","high","low","close","volume",
        "close_time","quote_asset_volume","number_of_trades",
        "taker_buy_base","taker_buy_quote","ignore"
    ])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("timestamp", inplace=True)
    df = df[["open","high","low","close","volume"]].astype(float)
    return df
