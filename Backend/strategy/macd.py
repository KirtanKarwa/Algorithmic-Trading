# strategy/macd.py
import pandas as pd
import ta

class MACDStrategy:
    """
    Smoothed MACD + EMA200 Trend Filter Strategy
    - Buy when MACD crosses above signal & price > EMA200
    - Short when MACD crosses below signal & price < EMA200
    - Hold position until opposite signal
    """

    def __init__(self, df, window_fast: int = 12, window_slow: int = 26, window_sign: int = 9, ema200_span: int = 200):
        self.df = df
        self.window_fast = int(window_fast)
        self.window_slow = int(window_slow)
        self.window_sign = int(window_sign)
        self.ema200_span = int(ema200_span)

    def generate_signals(self):
        df = self.df.copy()

        # --- MACD Calculation ---
        macd = ta.trend.MACD(close=df['close'], window_slow=self.window_slow, window_fast=self.window_fast, window_sign=self.window_sign)
        df['macd'] = macd.macd()
        df['signal_line'] = macd.macd_signal()
        df['macd_diff'] = df['macd'] - df['signal_line']

        # --- EMA200 Trend Filter ---
        df['ema200'] = df['close'].ewm(span=self.ema200_span, adjust=False).mean()

        # --- Raw MACD Crossovers ---
        df['raw_signal'] = 0
        df.loc[(df['macd_diff'] > 0) & (df['macd_diff'].shift(1) <= 0), 'raw_signal'] = 1
        df.loc[(df['macd_diff'] < 0) & (df['macd_diff'].shift(1) >= 0), 'raw_signal'] = -1

        # --- Apply Trend Filter ---
        df['signal'] = 0
        df.loc[(df['raw_signal'] == 1) & (df['close'] > df['ema200']), 'signal'] = 1
        df.loc[(df['raw_signal'] == -1) & (df['close'] < df['ema200']), 'signal'] = -1

        # --- Hold until opposite signal ---
        df['signal'] = df['signal'].replace(0, method='ffill')

        # --- Ignore first 200 candles for EMA warm-up safely ---
        if len(df) > 200:
            df.loc[df.index[:200], 'signal'] = 0

        df['signal'] = df['signal'].fillna(0)

        print("Signal counts:")
        print(df['signal'].value_counts())

        return df
