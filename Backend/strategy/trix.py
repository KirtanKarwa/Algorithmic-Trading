import pandas as pd
import ta


class TrixStrategy:
    def __init__(self, df: pd.DataFrame, signal_window: int = 9):
        self.df = df.copy()
        self.signal_window = int(signal_window)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        trix = ta.trend.TRIXIndicator(df['close'])
        df['trix'] = trix.trix()
        df['trix_signal'] = df['trix'].rolling(self.signal_window).mean()
        df['signal'] = 0
        df.loc[df['trix'] > df['trix_signal'], 'signal'] = 1
        df.loc[df['trix'] < df['trix_signal'], 'signal'] = -1
        return df
