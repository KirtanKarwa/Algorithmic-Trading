import pandas as pd


class Ema200PriceActionStrategy:
    def __init__(self, df: pd.DataFrame, ema_span: int = 200):
        self.df = df.copy()
        self.ema_span = int(ema_span)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        ema = df['close'].ewm(span=self.ema_span).mean()
        df['ema_200'] = ema
        df['signal'] = 0
        df.loc[df['close'] > df['ema_200'], 'signal'] = 1
        df.loc[df['close'] < df['ema_200'], 'signal'] = -1
        return df
    