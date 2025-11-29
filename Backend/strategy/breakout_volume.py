import pandas as pd


class BreakoutVolumeStrategy:
    def __init__(self, df: pd.DataFrame, breakout_window: int = 20, min_vol_mult: float = 1.0):
        self.df = df.copy()
        self.breakout_window = int(breakout_window)
        self.min_vol_mult = float(min_vol_mult)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        df['HighBreakout'] = df['high'] > df['high'].rolling(self.breakout_window).max().shift(1)
        df['VolumeSpike'] = df['volume'] > (self.min_vol_mult * df['volume'].rolling(self.breakout_window).mean())
        df['signal'] = 0
        df.loc[df['HighBreakout'] & df['VolumeSpike'], 'signal'] = 1
        return df
