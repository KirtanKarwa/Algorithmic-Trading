import pandas as pd


class VolumeBreakoutStrategy:
    def __init__(self, df: pd.DataFrame, avg_window: int = 20, min_change: float = 0.0, min_vol_mult: float = 1.0):
        self.df = df.copy()
        self.avg_window = int(avg_window)
        self.min_change = float(min_change)
        self.min_vol_mult = float(min_vol_mult)

    def generate_signals(self) -> pd.DataFrame:
        df = self.df
        df['AvgVolume'] = df['volume'].rolling(window=self.avg_window).mean()

        df['signal'] = 0
        up = (df['close'].pct_change() > self.min_change) & (df['volume'] > self.min_vol_mult * df['AvgVolume'])
        down = (df['close'].pct_change() < -self.min_change) & (df['volume'] > self.min_vol_mult * df['AvgVolume'])
        df.loc[up, 'signal'] = 1
        df.loc[down, 'signal'] = -1

        return df
