import pandas as pd


class HeikinAshiEmaStrategy:
    def __init__(self, df: pd.DataFrame, ema_span: int = 20):
        self.df = df.copy()
        self.ema_span = int(ema_span)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        ha_df = pd.DataFrame(index=df.index)
        ha_df['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        ha_df['open'] = (df['open'] + df['close']) / 2
        ha_df['high'] = df[['high', 'open', 'close']].max(axis=1)
        ha_df['low'] = df[['low', 'open', 'close']].min(axis=1)

        df['ema'] = df['close'].ewm(span=self.ema_span).mean()
        df['signal'] = 0
        df.loc[ha_df['close'] > df['ema'], 'signal'] = 1
        df.loc[ha_df['close'] < df['ema'], 'signal'] = -1
        return df
