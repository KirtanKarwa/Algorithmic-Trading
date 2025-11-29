import pandas as pd
import ta


class KeltnerBreakoutStrategy:
    def __init__(self, df: pd.DataFrame, window: int = 20, window_atr: int = 10, original: bool = False):
        self.df = df.copy()
        self.window = int(window)
        self.window_atr = int(window_atr)
        self.original = bool(original)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        kc = ta.volatility.KeltnerChannel(df['high'], df['low'], df['close'], window=self.window, window_atr=self.window_atr, original=self.original)
        df['upper'] = kc.keltner_channel_hband()
        df['lower'] = kc.keltner_channel_lband()
        df['signal'] = 0
        df.loc[df['close'] > df['upper'], 'signal'] = 1
        df.loc[df['close'] < df['lower'], 'signal'] = -1
        return df
