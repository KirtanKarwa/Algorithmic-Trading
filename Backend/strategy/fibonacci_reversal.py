import pandas as pd


class FibonacciReversalStrategy:
    def __init__(self, df: pd.DataFrame, lookback: int = 50, retrace: float = 0.618):
        self.df = df.copy()
        self.lookback = int(lookback)
        self.retrace = float(retrace)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        recent_high = df['high'].rolling(self.lookback).max()
        recent_low = df['low'].rolling(self.lookback).min()
        retracement = recent_high - (recent_high - recent_low) * self.retrace
        df['signal'] = 0
        df.loc[df['close'] < retracement, 'signal'] = 1
        return df
