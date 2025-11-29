import pandas as pd
import numpy as np

class BollingerRSIStrategy:
    def __init__(self, df, bb_window: int = 20, bb_std: float = 2.0, rsi_window: int = 14, rsi_buy: int = 30, rsi_sell: int = 70):
        self.df = df
        self.bb_window = int(bb_window)
        self.bb_std = float(bb_std)
        self.rsi_window = int(rsi_window)
        self.rsi_buy = int(rsi_buy)
        self.rsi_sell = int(rsi_sell)

    def generate_signals(self):
        df = self.df.copy()
        
        # --- Bollinger Bands ---
        df['middle_band'] = df['close'].rolling(window=self.bb_window).mean()
        df['std_dev'] = df['close'].rolling(window=self.bb_window).std()
        df['upper_band'] = df['middle_band'] + (self.bb_std * df['std_dev'])
        df['lower_band'] = df['middle_band'] - (self.bb_std * df['std_dev'])
        
        # --- RSI Calculation ---
        delta = df['close'].diff()
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        avg_gain = pd.Series(gain).rolling(window=self.rsi_window).mean()
        avg_loss = pd.Series(loss).rolling(window=self.rsi_window).mean()
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # --- Signal Logic ---
        df['signal'] = 0
        df.loc[(df['close'] < df['lower_band']) & (df['rsi'] < self.rsi_buy), 'signal'] = 1
        df.loc[(df['close'] > df['upper_band']) & (df['rsi'] > self.rsi_sell), 'signal'] = -1
        
        return df
