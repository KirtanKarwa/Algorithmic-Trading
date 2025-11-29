import pandas as pd
import ta


class AdxEmaStrategy:
    def __init__(self, df: pd.DataFrame, ema_span: int = 20, adx_threshold: float = 25.0):
        self.df = df.copy()
        self.ema_span = int(ema_span)
        self.adx_threshold = float(adx_threshold)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        ema = df['close'].ewm(span=self.ema_span).mean()
        adx = ta.trend.ADXIndicator(df['high'], df['low'], df['close']).adx()
        df['ema'] = ema
        df['adx'] = adx
        df['signal'] = 0
        df.loc[(df['close'] > df['ema']) & (df['adx'] > self.adx_threshold), 'signal'] = 1
        df.loc[(df['close'] < df['ema']) & (df['adx'] > self.adx_threshold), 'signal'] = -1
        return df
