import requests
import pandas as pd
import time

class BinanceConnector:
    def __init__(self, api_key=None, api_secret=None):
        self.base_url = "https://api.binance.com/api/v3"
        self.api_key = api_key
        self.api_secret = api_secret

    def get_klines(self, symbol, interval, lookback):
        url = f"{self.base_url}/klines"
        limit = lookback
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Binance kline format:
            # [
            #   [
            #     1499040000000,      // Open time
            #     "0.01634790",       // Open
            #     "0.80000000",       // High
            #     "0.01575800",       // Low
            #     "0.01577100",       // Close
            #     "148976.11427815",  // Volume
            #     1499644799999,      // Close time
            #     ...
            #   ]
            # ]
            
            df = pd.DataFrame(data, columns=[
                "timestamp", "open", "high", "low", "close", "volume", 
                "close_time", "quote_asset_volume", "number_of_trades", 
                "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
            ])
            
            # Convert numeric columns
            numeric_cols = ["open", "high", "low", "close", "volume"]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, axis=1)
            
            # Convert timestamp to datetime
            df["time"] = pd.to_datetime(df["timestamp"], unit="ms")
            
            return df
            
        except Exception as e:
            print(f"Error fetching data from Binance: {e}")
            return pd.DataFrame()