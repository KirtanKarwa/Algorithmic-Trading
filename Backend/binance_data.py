import pandas as pd
import requests
import logging
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

# CoinGecko API endpoints (free, no auth required)
COINGECKO_API = "https://api.coingecko.com/api/v3"

# Symbol mapping: our format -> CoinGecko ID
SYMBOL_MAP = {
    'BTC-USD': 'bitcoin',
    'BTCUSDT': 'bitcoin',
    'ETH-USD': 'ethereum',
    'ETHUSDT': 'ethereum',
    'BNB-USD': 'binancecoin',
    'BNBUSDT': 'binancecoin',
    'SOL-USD': 'solana',
    'SOLUSDT': 'solana',
    'XRP-USD': 'ripple',
    'XRPUSDT': 'ripple',
    'ADA-USD': 'cardano',
    'ADAUSDT': 'cardano',
    'DOGE-USD': 'dogecoin',
    'DOGEUSDT': 'dogecoin',
    'MATIC-USD': 'matic-network',
    'MATICUSDT': 'matic-network',
    'DOT-USD': 'polkadot',
    'DOTUSDT': 'polkadot',
    'AVAX-USD': 'avalanche-2',
    'AVAXUSDT': 'avalanche-2',
}

def get_coingecko_id(symbol: str) -> str:
    """Convert symbol to CoinGecko ID"""
    return SYMBOL_MAP.get(symbol.upper(), 'bitcoin')


def get_historical_klines_df(symbol, interval="15m", start=None, end=None):
    """
    Fetch historical data from CoinGecko (free, no geo-restrictions).
    
    Symbol: BTC-USD, ETH-USD, etc (auto-converts to CoinGecko IDs)
    Interval: Not used by CoinGecko (returns daily data), but kept for compatibility
    """
    coin_id = get_coingecko_id(symbol)
    
    # Convert dates to timestamps
    start_ts = int(pd.Timestamp(start).timestamp())
    end_ts = int(pd.Timestamp(end).timestamp())
    
    logger.info(f"Fetching CoinGecko data for {symbol} ({coin_id}) from {start} to {end}")
    
    try:
        # CoinGecko market_chart/range endpoint
        url = f"{COINGECKO_API}/coins/{coin_id}/market_chart/range"
        params = {
            'vs_currency': 'usd',
            'from': start_ts,
            'to': end_ts
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            error_msg = f"CoinGecko API error: {response.status_code} - {response.text}"
            logger.error(error_msg)
            print(error_msg)
            return pd.DataFrame()
        
        data = response.json()
        
        # CoinGecko returns: {prices: [[timestamp, price], ...], market_caps: [...], total_volumes: [...]}
        if 'prices' not in data or not data['prices']:
            logger.warning(f"CoinGecko returned no price data for {coin_id}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        prices_df = pd.DataFrame(data['prices'], columns=['timestamp', 'close'])
        volumes_df = pd.DataFrame(data.get('total_volumes', []), columns=['timestamp', 'volume'])
        
        # Merge price and volume data
        df = prices_df.merge(volumes_df, on='timestamp', how='left')
        
        # Convert timestamp from milliseconds to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        
        # CoinGecko only provides close prices, so we'll approximate OHLC
        # For backtesting purposes, we'll use close price for all OHLC values
        # This is a limitation but acceptable for historical analysis
        df['open'] = df['close']
        df['high'] = df['close']
        df['low'] = df['close']
        
        # Reorder columns to match expected format
        df = df[['open', 'high', 'low', 'close', 'volume']]
        
        # Fill any missing volume data with 0
        df['volume'] = df['volume'].fillna(0)
        
        logger.info(f"✅ Fetched {len(df)} data points from CoinGecko")
        return df
        
    except Exception as e:
        error_msg = f"CoinGecko error for {symbol}: {str(e)}"
        logger.error(error_msg)
        print(error_msg)
        return pd.DataFrame()


def get_klines(symbol, interval="1m", limit=100):
    """
    Fetch recent data for live trading.
    Gets the last 'limit' days of data.
    """
    coin_id = get_coingecko_id(symbol)
    
    try:
        # Get data for last N days
        days = min(limit, 365)  # CoinGecko free tier limit
        
        url = f"{COINGECKO_API}/coins/{coin_id}/market_chart"
        params = {
            'vs_currency': 'usd',
            'days': days,
            'interval': 'daily' if days > 90 else 'hourly'
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"CoinGecko API error: {response.status_code}")
            return pd.DataFrame()
        
        data = response.json()
        
        if 'prices' not in data or not data['prices']:
            return pd.DataFrame()
        
        # Convert to DataFrame
        prices_df = pd.DataFrame(data['prices'], columns=['timestamp', 'close'])
        volumes_df = pd.DataFrame(data.get('total_volumes', []), columns=['timestamp', 'volume'])
        
        df = prices_df.merge(volumes_df, on='timestamp', how='left')
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        
        # Approximate OHLC from close prices
        df['open'] = df['close']
        df['high'] = df['close']
        df['low'] = df['close']
        df['volume'] = df['volume'].fillna(0)
        
        # Take only the last 'limit' rows
        df = df.tail(limit)
        
        df = df[['open', 'high', 'low', 'close', 'volume']]
        
        return df
        
    except Exception as e:
        logger.error(f"CoinGecko error: {e}")
        return pd.DataFrame()
