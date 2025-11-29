import pandas as pd
import ta


class SupertrendRsiStrategy:
    def __init__(self, df: pd.DataFrame, stc_fast: int = 23, stc_slow: int = 50, stc_cycle: int = 10, stc_buy: float = 50.0, stc_sell: float = 50.0, rsi_window: int = 14, rsi_buy: float = 30.0, rsi_sell: float = 70.0):
        self.df = df.copy()
        self.stc_fast = int(stc_fast)
        self.stc_slow = int(stc_slow)
        self.stc_cycle = int(stc_cycle)
        self.stc_buy = float(stc_buy)
        self.stc_sell = float(stc_sell)
        self.rsi_window = int(rsi_window)
        self.rsi_buy = float(rsi_buy)
        self.rsi_sell = float(rsi_sell)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        supertrend = ta.trend.STCIndicator(close=df['close'], window_slow=self.stc_slow, window_fast=self.stc_fast, cycle=self.stc_cycle, fillna=True)
        df['supertrend'] = supertrend.stc()
        rsi = ta.momentum.RSIIndicator(close=df['close'], window=self.rsi_window).rsi()
        df['rsi'] = rsi
        df['signal'] = 0
        df.loc[(df['supertrend'] > self.stc_buy) & (df['rsi'] < self.rsi_buy), 'signal'] = 1
        df.loc[(df['supertrend'] < self.stc_sell) & (df['rsi'] > self.rsi_sell), 'signal'] = -1
        return df
