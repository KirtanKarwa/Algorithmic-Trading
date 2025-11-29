import pandas as pd


class SMACrossStrategy:
    def __init__(self, df: pd.DataFrame, short_window: int = 50, long_window: int = 200):
        self.df = df.copy()
        self.short_window = int(short_window)
        self.long_window = int(long_window)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        df['SMA50'] = df['close'].rolling(window=self.short_window).mean()
        df['SMA200'] = df['close'].rolling(window=self.long_window).mean()

        df['signal'] = 0
        df.loc[df['SMA50'] > df['SMA200'], 'signal'] = 1
        df.loc[df['SMA50'] < df['SMA200'], 'signal'] = -1

        return df
