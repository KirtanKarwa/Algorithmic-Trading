import pandas as pd
import yfinance as yf
import logging

logger = logging.getLogger(__name__)

def get_historical_klines_df(symbol, interval="15m", start=None, end=None):
    """
    Fetch historical klines using Yahoo Finance (no geo-restrictions).
    
    Symbol format: Use Yahoo format like 'BTC-USD', 'ETH-USD' 
    (or pass 'BTCUSDT' and it will auto-convert)
    
    Interval mapping:
    - '1m', '5m', '15m', '30m' -> '1m', '5m', '15m', '30m'
    - '1h', '4h' -> '1h', '4h'  
    - '1d' -> '1d'
    """
    # Convert Binance-style symbols to Yahoo format
    if symbol.endswith('USDT'):
        yf_symbol = symbol.replace('USDT', '-USD')
    elif symbol.endswith('BUSD'):
        yf_symbol = symbol.replace('BUSD', '-USD')
    elif '-' not in symbol:
        yf_symbol = f"{symbol}-USD"
    else:
        yf_symbol = symbol
    
    # Yahoo Finance interval mapping
    interval_map = {
        '1m': '1m',
        '5m': '5m', 
        '15m': '15m',
        '30m': '30m',
        '1h': '1h',
        '2h': '1h',  # Yahoo doesn't have 2h, use 1h
        '4h': '1h',  # Yahoo doesn't have 4h, use 1h
        '1d': '1d',
    }
    yf_interval = interval_map.get(interval, '1h')
    
    logger.info(f"Fetching Yahoo Finance data for {yf_symbol} ({yf_interval}) from {start} to {end}")
    
    try:
        # Download data from Yahoo Finance
        df = yf.download(
            yf_symbol, 
            start=start, 
            end=end, 
            interval=yf_interval,
            progress=False,
            auto_adjust=True  # Adjust for splits/dividends
        )
        
        if df is None or df.empty:
            logger.warning(f"Yahoo Finance returned empty data for {yf_symbol}")
            return pd.DataFrame()
        
        # Rename columns to match expected format
        df.columns = df.columns.str.lower()
        
        # Yahoo Finance returns: Open, High, Low, Close, Volume
        # We need: open, high, low, close, volume
        expected_cols = ['open', 'high', 'low', 'close', 'volume']
        
        # Keep only the columns we need
        df = df[[col for col in expected_cols if col in df.columns]]
        
        logger.info(f"✅ Fetched {len(df)} candles from Yahoo Finance")
        return df
        
    except Exception as e:
        error_msg = f"Yahoo Finance error for {yf_symbol}: {str(e)}"
        logger.error(error_msg)
        print(error_msg)
        return pd.DataFrame()


def get_klines(symbol, interval="1m", limit=100):
    """
    Helper for live/recent data fetching.
    Fetches the most recent 'limit' candles.
    """
    # Convert symbol format
    if symbol.endswith('USDT'):
        yf_symbol = symbol.replace('USDT', '-USD')
    elif '-' not in symbol:
        yf_symbol = f"{symbol}-USD"
    else:
        yf_symbol = symbol
    
    interval_map = {
        '1m': '1m',
        '5m': '5m',
        '15m': '15m', 
        '1h': '1h',
        '1d': '1d'
    }
    yf_interval = interval_map.get(interval, '1m')
    
    try:
        # Get recent data (last few days to ensure we get enough candles)
        period = '7d' if interval in ['1m', '5m'] else '60d'
        
        df = yf.download(
            yf_symbol,
            period=period,
            interval=yf_interval,
            progress=False,
            auto_adjust=True
        )
        
        if df is None or df.empty:
            return pd.DataFrame()
        
        # Take only the last 'limit' rows
        df = df.tail(limit)
        
        # Rename columns
        df.columns = df.columns.str.lower()
        expected_cols = ['open', 'high', 'low', 'close', 'volume']
        df = df[[col for col in expected_cols if col in df.columns]]
        
        return df
        
    except Exception as e:
        logger.error(f"Yahoo Finance error: {e}")
        return pd.DataFrame()
