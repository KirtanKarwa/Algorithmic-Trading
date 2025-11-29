import pandas as pd
import numpy as np

class BollingerRSIStrategy:
    def __init__(self, df):
        self.df = df

    def generate_signals(self):
        df = self.df.copy()

        # --- Bollinger Bands (standard 20-period) ---
        period = 20
        mult = 2.0
        df["sma"] = df["close"].rolling(window=period).mean()
        df["std"] = df["close"].rolling(window=period).std()
        df["upper_band"] = df["sma"] + mult * df["std"]
        df["lower_band"] = df["sma"] - mult * df["std"]

        # --- RSI (14-period) ---
        delta = df["close"].diff()
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        avg_gain = pd.Series(gain).rolling(window=14).mean()
        avg_loss = pd.Series(loss).rolling(window=14).mean()
        rs = avg_gain / (avg_loss + 1e-10)
        df["rsi"] = 100 - (100 / (1 + rs))

        # --- Signal generation ---
        df["signal"] = 0

        # BUY when RSI < 40 and price below lower band
        df.loc[(df["rsi"] < 40) & (df["close"] < df["lower_band"]), "signal"] = 1

        # SELL (short) when RSI > 60 and price above upper band
        df.loc[(df["rsi"] > 60) & (df["close"] > df["upper_band"]), "signal"] = -1

        # HOLD / EXIT conditions to flip positions naturally
        # Exit long if RSI > 55
        df.loc[(df["rsi"] > 55) & (df["signal"] == 1), "signal"] = 0
        # Exit short if RSI < 45
        df.loc[(df["rsi"] < 45) & (df["signal"] == -1), "signal"] = 0

        # Replace NaNs and ensure early data is neutral
        df["signal"].fillna(0, inplace=True)
        df.loc[:period, "signal"] = 0

        return df