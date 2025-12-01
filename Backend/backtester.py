# backtester.py

import matplotlib
matplotlib.use("Agg")
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import mplfinance as mpf
from datetime import datetime
from typing import Optional
from strategy_loader import load_strategy_class
from binance_data import get_historical_klines_df

# === CONFIG ===
INITIAL_BALANCE = 1000
TRADING_FEE = 0.001  # 0.1%
SLIPPAGE = 0.0005    # 0.05%

class Backtester:
    def __init__(self, symbol, interval, strategy_class, start, end, strategy_params: Optional[dict] = None):
        self.symbol = symbol
        self.interval = interval
        self.strategy_class = strategy_class
        self.start = start
        self.end = end
        self.strategy_params = strategy_params or {}
        self.balance = INITIAL_BALANCE
        self.position = 0       # 0 = no position, 1 = long, -1 = short
        self.entry_price = None
        self.trades = []
        self.equity_curve = []
        self.timestamps = []
        self.logs_dir = f"logs/{strategy_class.__name__}"
        os.makedirs(self.logs_dir, exist_ok=True)

    def fetch_data(self):
        print(f"🌐 Fetching data for {self.symbol} ({self.interval})...")
        print(f"   Date range: {self.start} to {self.end}")
        df = get_historical_klines_df(self.symbol, self.interval, self.start, self.end)
        if df is None or df.empty:
            error_msg = f"No data fetched for {self.symbol} ({self.interval}) from {self.start} to {self.end}. Check symbol name and date range."
            print(f"❌ {error_msg}")
            raise ValueError(error_msg)
        print(f"✅ Fetched {len(df)} candles")
        return df

    def apply_strategy(self, df):
        # Instantiate with optional parameters if accepted
        try:
            strategy = self.strategy_class(df, **self.strategy_params)
        except TypeError:
            strategy = self.strategy_class(df)
        df = strategy.generate_signals()
        if 'signal' not in df.columns:
            raise ValueError("Strategy must return a 'signal' column.")
        return df

    def run(self):
        df = self.fetch_data()
        df = self.apply_strategy(df)
        print("Signal counts:")
        print(df['signal'].value_counts())

        print(f"\n🚀 Starting backtest: {self.symbol} | Strategy: {self.strategy_class.__name__}\n")

        for i in range(len(df)):
            price = df.iloc[i]["close"]
            signal = df.iloc[i]["signal"]
            timestamp = df.index[i]

            # --- LONG SIGNAL ---
            if signal == 1:
                if self.position == 0:
                    # Open long
                    self.entry_price = price * (1 + SLIPPAGE)
                    self.position = 1
                    self.trades.append({"type": "LONG_ENTRY", "price": self.entry_price, "time": timestamp})
                elif self.position == -1:
                    # Close short, open long
                    exit_price = price * (1 - SLIPPAGE)
                    pnl = (self.entry_price - exit_price) / self.entry_price
                    self.balance *= (1 + pnl - TRADING_FEE)
                    self.trades.append({"type": "SHORT_EXIT", "price": exit_price, "time": timestamp, "pnl": pnl})
                    self.entry_price = price * (1 + SLIPPAGE)
                    self.position = 1
                    self.trades.append({"type": "LONG_ENTRY", "price": self.entry_price, "time": timestamp})

            # --- SHORT SIGNAL ---
            elif signal == -1:
                if self.position == 0:
                    # Open short
                    self.entry_price = price * (1 - SLIPPAGE)
                    self.position = -1
                    self.trades.append({"type": "SHORT_ENTRY", "price": self.entry_price, "time": timestamp})
                elif self.position == 1:
                    # Close long, open short
                    exit_price = price * (1 - SLIPPAGE)
                    pnl = (exit_price - self.entry_price) / self.entry_price
                    self.balance *= (1 + pnl - TRADING_FEE)
                    self.trades.append({"type": "LONG_EXIT", "price": exit_price, "time": timestamp, "pnl": pnl})
                    self.entry_price = price * (1 - SLIPPAGE)
                    self.position = -1
                    self.trades.append({"type": "SHORT_ENTRY", "price": self.entry_price, "time": timestamp})

            # --- EQUITY TRACKING ---
            equity = self.balance
            if self.position == 1:
                equity += self.balance * 0 + (price - self.entry_price) / self.entry_price * self.balance
            elif self.position == -1:
                equity += self.balance * 0 + (self.entry_price - price) / self.entry_price * self.balance
            self.equity_curve.append(equity)
            self.timestamps.append(timestamp)

        # --- CLOSE FINAL POSITION ---
        if self.position != 0:
            price = df.iloc[-1]['close']
            exit_price = price * (1 - SLIPPAGE) if self.position == 1 else price * (1 + SLIPPAGE)
            pnl = ((exit_price - self.entry_price) / self.entry_price) if self.position == 1 else ((self.entry_price - exit_price)/self.entry_price)
            self.balance *= (1 + pnl - TRADING_FEE)
            self.trades.append({
                "type": "LONG_EXIT" if self.position == 1 else "SHORT_EXIT",
                "price": exit_price,
                "time": df.index[-1],
                "pnl": pnl
            })
            self.position = 0
            self.equity_curve.append(self.balance)
            self.timestamps.append(df.index[-1])

        # --- SAVE LOGS & PLOTS ---
        self.save_logs(df)
        self.plot_equity()
        self.plot_trades(df)
        self.print_summary()

    # --- STATS CALCULATION ---
    def calculate_stats(self):
        if not self.trades:
            return {
                "Final Balance": self.balance,
                "Total Return (%)": 0,
                "Win Rate (%)": 0,
                "Sharpe Ratio": 0,
                "Max Drawdown (%)": 0
            }

        df_trades = pd.DataFrame(self.trades)
        wins = df_trades[df_trades.get("pnl", 0) > 0].shape[0]
        losses = df_trades[df_trades.get("pnl", 0) <= 0].shape[0]
        win_rate = (wins / (wins + losses) * 100) if wins + losses > 0 else 0

        equity = pd.Series(self.equity_curve, index=self.timestamps)
        returns = equity.pct_change().dropna()
        sharpe = np.sqrt(252) * returns.mean() / returns.std() if returns.std() != 0 else 0
        drawdown = (equity / equity.cummax() - 1).min() * 100
        total_return = ((equity.iloc[-1] - INITIAL_BALANCE) / INITIAL_BALANCE) * 100

        return {
            "Final Balance": equity.iloc[-1],
            "Total Return (%)": total_return,
            "Win Rate (%)": win_rate,
            "Sharpe Ratio": sharpe,
            "Max Drawdown (%)": drawdown
        }

    # --- LOGS & PLOTS ---
    def save_logs(self, df):
        equity_df = pd.DataFrame({
            "time": self.timestamps,
            "equity": self.equity_curve
        })
        equity_df.to_csv(f"{self.logs_dir}/equity.csv", index=False)

        if self.trades:
            trades_df = pd.DataFrame(self.trades)
            # Deprecated: backtest_trades.csv (remove if exists)
            legacy_path = f"{self.logs_dir}/backtest_trades.csv"
            try:
                if os.path.exists(legacy_path):
                    os.remove(legacy_path)
            except Exception:
                pass

            # Write consolidated one-line-per-trade CSV as backtester.csv
            consolidated = self._consolidate_trades()
            if not consolidated.empty:
                consolidated.to_csv(f"{self.logs_dir}/backtester.csv", index=False)

    def _consolidate_trades(self) -> pd.DataFrame:
        """Build one-line-per-trade rows from entry/exit events.
        Columns: time (entry time), symbol, side (Long/Short), entry, exit, pnl, strategy
        """
        if not self.trades:
            return pd.DataFrame()
        rows = sorted(self.trades, key=lambda r: r.get("time"))
        result = []
        open_long = None
        open_short = None
        for r in rows:
            typ = str(r.get("type", "")).upper()
            price = r.get("price")
            ts = r.get("time")
            if "LONG_ENTRY" in typ:
                open_long = {"time": ts, "entry": price}
            elif "LONG_EXIT" in typ:
                exit_price = price
                entry = open_long["entry"] if open_long else r.get("entry", None)
                entry_time = open_long["time"] if open_long else ts
                pnl = r.get("pnl")
                if pnl is None and entry not in (None, 0) and exit_price is not None:
                    pnl = (exit_price - entry) / entry
                result.append({
                    "time": str(entry_time),
                    "symbol": self.symbol,
                    "side": "Long",
                    "entry": entry,
                    "exit": exit_price,
                    "pnl": pnl,
                    "strategy": self.strategy_class.__name__,
                })
                open_long = None
            elif "SHORT_ENTRY" in typ:
                open_short = {"time": ts, "entry": price}
            elif "SHORT_EXIT" in typ:
                exit_price = price
                entry = open_short["entry"] if open_short else r.get("entry", None)
                entry_time = open_short["time"] if open_short else ts
                pnl = r.get("pnl")
                if pnl is None and entry not in (None, 0) and exit_price is not None:
                    pnl = (entry - exit_price) / entry
                result.append({
                    "time": str(entry_time),
                    "symbol": self.symbol,
                    "side": "Short",
                    "entry": entry,
                    "exit": exit_price,
                    "pnl": pnl,
                    "strategy": self.strategy_class.__name__,
                })
                open_short = None
        return pd.DataFrame(result)

    def plot_equity(self):
        plt.figure(figsize=(12, 6))
        plt.plot(self.timestamps, self.equity_curve, label="Equity Curve", linewidth=2)
        plt.title(f"Equity Curve - {self.symbol} ({self.strategy_class.__name__})")
        plt.xlabel("Time")
        plt.ylabel("Portfolio Value ($)")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"{self.logs_dir}/equity_plot.png")
        plt.show()

    def plot_trades(self, df):
        df_plot = df[['open','high','low','close','volume']].copy()
        df_plot.index = pd.to_datetime(df_plot.index)

        buy_signals = [t for t in self.trades if 'LONG_ENTRY' in t['type']]
        sell_signals = [t for t in self.trades if 'LONG_EXIT' in t['type']]

        short_buy_signals = [t for t in self.trades if 'SHORT_ENTRY' in t['type']]
        short_sell_signals = [t for t in self.trades if 'SHORT_EXIT' in t['type']]

        ap = []

        def add_scatter(signals, marker, color):
            series = pd.Series(index=df_plot.index, data=np.nan)
            for t in signals:
                if t['time'] in series.index:
                    series[t['time']] = t['price']
            return mpf.make_addplot(series, type='scatter', markersize=50, marker=marker, color=color)

        if buy_signals:
            ap.append(add_scatter(buy_signals, '^', 'green'))
        if sell_signals:
            ap.append(add_scatter(sell_signals, 'v', 'red'))
        if short_buy_signals:
            ap.append(add_scatter(short_buy_signals, 'v', 'orange'))
        if short_sell_signals:
            ap.append(add_scatter(short_sell_signals, '^', 'purple'))

        mpf.plot(df_plot, type='candle', style='charles', addplot=ap,
                 title=f"{self.symbol} Trades", volume=True,
                 savefig=f"{self.logs_dir}/trades_plot.png")

    def print_summary(self):
        stats = self.calculate_stats()
        summary_path = f"{self.logs_dir}/summary.txt"
        with open(summary_path, "w") as f:
            f.write(f"Backtest Summary - {self.symbol}\n")
            f.write(f"Strategy: {self.strategy_class.__name__}\n")
            f.write(f"Time Period: {self.start} to {self.end}\n\n")
            for k, v in stats.items():
                f.write(f"{k}: {v:.2f}\n")

        print("\n📊 Backtest Summary:")
        for k, v in stats.items():
            print(f"{k}: {v:.2f}")

# === MAIN ===
def main():
    parser = argparse.ArgumentParser(description="AI Crypto Trading Bot Backtester")
    parser.add_argument("--symbol", type=str, required=True)
    parser.add_argument("--interval", type=str, default="15m")
    parser.add_argument("--strategy", type=str, required=True)
    parser.add_argument("--start", type=str, required=True)
    parser.add_argument("--end", type=str, required=True)
    args = parser.parse_args()

    strategy_class = load_strategy_class(args.strategy)
    backtester = Backtester(args.symbol, args.interval, strategy_class, args.start, args.end)
    backtester.run()

if __name__ == "__main__":
    main()
