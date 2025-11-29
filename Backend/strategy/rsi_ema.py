# strategy/rsi_ema.py
import pandas as pd
import ta  # pip install ta

class RSIEMAStrategy:
    """
    RSI + EMA Strategy for multi-coin paper trading.
    Generates numeric signals: 1 = BUY/long, -1 = SELL/short, 0 = HOLD
    """

    def __init__(self, df, rsi_period=7, ema_period=21, rsi_buy=45, rsi_sell=55):
        """
        df: OHLCV DataFrame with 'close' column
        rsi_period: period for RSI
        ema_period: period for EMA
        rsi_buy: RSI threshold to buy
        rsi_sell: RSI threshold to sell
        """
        self.df = df.copy()
        self.rsi_period = rsi_period
        self.ema_period = ema_period
        self.rsi_buy = rsi_buy
        self.rsi_sell = rsi_sell

    def generate_signals(self):
        # Compute EMA
        self.df['ema'] = self.df['close'].ewm(span=self.ema_period, adjust=False).mean()

        # Compute RSI
        self.df['rsi'] = ta.momentum.RSIIndicator(self.df['close'], window=self.rsi_period).rsi()

        # Initialize signals
        self.df['signal'] = 0

        # Long signal: RSI below rsi_buy and price above EMA
        self.df.loc[(self.df['rsi'] < self.rsi_buy) & (self.df['close'] > self.df['ema']), 'signal'] = 1

        # Short signal: RSI above rsi_sell and price below EMA
        self.df.loc[(self.df['rsi'] > self.rsi_sell) & (self.df['close'] < self.df['ema']), 'signal'] = -1

        # Ensure numeric 0 for HOLD
        self.df['signal'] = self.df['signal'].fillna(0).astype(int)

        # Debug: print signal counts
        print("Signal counts:")
        print(self.df['signal'].value_counts())

        return self.df
