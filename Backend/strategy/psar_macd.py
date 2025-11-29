import pandas as pd
import ta


class PsarMacdStrategy:
    def __init__(self, df: pd.DataFrame, psar_step: float = 0.02, psar_max: float = 0.2, window_fast: int = 12, window_slow: int = 26, window_sign: int = 9):
        self.df = df.copy()
        self.psar_step = float(psar_step)
        self.psar_max = float(psar_max)
        self.window_fast = int(window_fast)
        self.window_slow = int(window_slow)
        self.window_sign = int(window_sign)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        psar = ta.trend.PSARIndicator(df['high'], df['low'], df['close'], step=self.psar_step, max_step=self.psar_max)
        df['psar'] = psar.psar()
        macd = ta.trend.MACD(df['close'], window_slow=self.window_slow, window_fast=self.window_fast, window_sign=self.window_sign)
        df['macd'] = macd.macd()
        df['macd_signal'] = macd.macd_signal()
        df['signal'] = 0
        df.loc[(df['close'] < df['psar']) & (df['macd'] > df['macd_signal']), 'signal'] = 1
        df.loc[(df['close'] > df['psar']) & (df['macd'] < df['macd_signal']), 'signal'] = -1
        return df
