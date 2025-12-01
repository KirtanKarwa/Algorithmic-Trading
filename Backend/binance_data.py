import pandas as pd
import requests
import logging
from datetime import datetime
import time

logger = logging.getLogger(__name__)

# CryptoCompare API (free, 100k calls/month, no geo-restrictions)
CRYPTOCOMPARE_API = "https://min-api.cryptocompare.com/data"

# Symbol mapping: handle various formats
def get_crypto_symbol(symbol: str) -> str:
    """Extract crypto symbol from various formats"""
    symbol = symbol.upper()
    
    # Remove common suffixes
    if symbol.endswith('USDT'):
        return symbol.replace('USDT', '')
    elif symbol.endswith('-USD'):
        return symbol.replace('-USD', '')
    elif symbol.endswith('USD'):
        return symbol.replace('USD', '')
    
    return symbol


def get_historical_klines_df(symbol, interval="15m", start=None, end=None):
    """
    Fetch historical data from CryptoCompare (free, reliable, no geo-restrictions).
    
    Symbol: BTC, ETH, BTC-USD, BTCUSDT (all formats work)
    Interval: 1m, 5m, 15m, 30m, 1h, 2h, 4h, 1d
    """
    crypto_symbol = get_crypto_symbol(symbol)
    
    # Convert dates to timestamps
    start_ts = int(pd.Timestamp(start).timestamp())
    end_ts = int(pd.Timestamp(end).timestamp())
    
    logger.info(f"Fetching CryptoCompare data for {symbol} ({crypto_symbol}) from {start} to {end}")
    
    # Map interval to CryptoCompare endpoints
    if interval in ['1m', '5m', '15m', '30m']:
        endpoint = 'histominute'
        if interval == '5m':
            aggregate = 5
        elif interval == '15m':
            aggregate = 15
        elif interval == '30m':
            aggregate = 30
        else:
            aggregate = 1
    elif interval in ['1h', '2h', '4h']:
        endpoint = 'histohour'
        if interval == '2h':
            aggregate = 2
        elif interval == '4h':
            aggregate = 4
        else:
            aggregate = 1
    else:  # 1d or any other
        endpoint = 'histoday'
        aggregate = 1
    
    try:
        all_data = []
        current_ts = start_ts
        
        # CryptoCompare returns max 2000 points per request
        limit = 2000
        
        while current_ts < end_ts:
            url = f"{CRYPTOCOMPARE_API}/{endpoint}"
            params = {
                'fsym': crypto_symbol,
                'tsym': 'USD',
                'limit': limit,
                'toTs': min(end_ts, current_ts + (limit * aggregate * 60)),
                'aggregate': aggregate
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                error_msg = f"CryptoCompare API error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                print(error_msg)
                break
            
            data = response.json()
            
            if data.get('Response') == 'Error':
                error_msg = f"CryptoCompare error: {data.get('Message', 'Unknown error')}"
                logger.error(error_msg)
                print(error_msg)
                break
            
            if 'Data' not in data or not data['Data']:
                logger.warning(f"No data returned for {crypto_symbol}")
                break
            
            candles = data['Data']
            all_data.extend(candles)
            
            # Update current_ts to the last timestamp + 1
            last_ts = candles[-1]['time']
            if last_ts >= end_ts or len(candles) < limit:
                break
            current_ts = last_ts + 1
            
            # Avoid rate limits
            time.sleep(0.1)
        
        if not all_data:
            logger.warning(f"CryptoCompare returned no data for {crypto_symbol}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        
        # CryptoCompare returns: time, open, high, low, close, volumefrom, volumeto
        df['timestamp'] = pd.to_datetime(df['time'], unit='s')
        df.set_index('timestamp', inplace=True)
        
        # Filter by date range
        df = df[(df.index >= pd.Timestamp(start)) & (df.index <= pd.Timestamp(end))]
        
        # Rename/select columns to match expected format
        df = df.rename(columns={
            'open': 'open',
            'high': 'high', 
            'low': 'low',
            'close': 'close',
            'volumefrom': 'volume'  # Use volumefrom (crypto volume)
        })
        
        df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        
        logger.info(f"✅ Fetched {len(df)} candles from CryptoCompare")
        return df
        
    except Exception as e:
        error_msg = f"CryptoCompare error for {symbol}: {str(e)}"
        logger.error(error_msg)
        print(error_msg)
        return pd.DataFrame()


def get_klines(symbol, interval="1m", limit=100):
    """
    Fetch recent data for live trading.
    Gets the last 'limit' candles.
    """
    crypto_symbol = get_crypto_symbol(symbol)
    
    # Map interval to endpoint
    if interval in ['1m', '5m', '15m', '30m']:
        endpoint = 'histominute'
        if interval == '5m':
            aggregate = 5
        elif interval == '15m':
            aggregate = 15
        elif interval == '30m':
            aggregate = 30
        else:
            aggregate = 1
    elif interval in ['1h', '2h', '4h']:
        endpoint = 'histohour'
        if interval == '2h':
            aggregate = 2
        elif interval == '4h':
            aggregate = 4
        else:
            aggregate = 1
    else:
        endpoint = 'histoday'
        aggregate = 1
    
    try:
        url = f"{CRYPTOCOMPARE_API}/{endpoint}"
        params = {
            'fsym': crypto_symbol,
            'tsym': 'USD',
            'limit': limit,
            'aggregate': aggregate
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"CryptoCompare API error: {response.status_code}")
            return pd.DataFrame()
        
        data = response.json()
        
        if data.get('Response') == 'Error' or 'Data' not in data or not data['Data']:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(data['Data'])
        df['timestamp'] = pd.to_datetime(df['time'], unit='s')
        df.set_index('timestamp', inplace=True)
        
        df = df.rename(columns={'volumefrom': 'volume'})
        df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        
        return df.tail(limit)
        
    except Exception as e:
        logger.error(f"CryptoCompare error: {e}")
        return pd.DataFrame()
