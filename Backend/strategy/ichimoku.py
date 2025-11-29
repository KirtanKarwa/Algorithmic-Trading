import pandas as pd
import ta


class IchimokuStrategy:
    def __init__(self, df: pd.DataFrame, window1: int = 9, window2: int = 26):
        self.df = df.copy()
        self.window1 = int(window1)
        self.window2 = int(window2)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        ichimoku = ta.trend.IchimokuIndicator(df['high'], df['low'], window1=self.window1, window2=self.window2)
        df['base_line'] = ichimoku.ichimoku_base_line()
        df['conversion_line'] = ichimoku.ichimoku_conversion_line()
        df['signal'] = 0
        df.loc[df['conversion_line'] > df['base_line'], 'signal'] = 1
        df.loc[df['conversion_line'] < df['base_line'], 'signal'] = -1
        return df
