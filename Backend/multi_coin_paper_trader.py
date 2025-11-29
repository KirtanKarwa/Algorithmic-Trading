# multi_coin_paper_trader.py
import time
import argparse
import pandas as pd
import os
from datetime import datetime
from rich.console import Console
from rich.table import Table
from strategy_loader import load_strategy_class
from binance_data import get_klines
from utils.telegram_alert import send_telegram_message  # optional
import matplotlib.pyplot as plt

console = Console()

class MultiCoinPaperTrader:
    def __init__(self, symbols, strategy_class, live=False, starting_balance=1000, slippage_pct=0.05, fee_pct=0.04):
        self.symbols = symbols
        self.strategy_class = strategy_class
        self.positions = {s: 0 for s in symbols}  # 1=long, -1=short, 0=flat
        self.entry_prices = {s: None for s in symbols}
        self.balance = starting_balance * len(symbols)
        self.initial_balance = self.balance
        self.equity_history = []
        self.trade_log = []
        self.latest_signals = []
        self.live = live
        self.slippage_pct = slippage_pct / 100  # convert to decimal
        self.fee_pct = fee_pct / 100
        self.logs_dir = f"logs/{strategy_class.__name__}"
        os.makedirs(self.logs_dir, exist_ok=True)

    def update(self):
        table = Table(title="📊 Live Portfolio Status")
        table.add_column("Symbol", justify="center")
        table.add_column("Side", justify="center")
        table.add_column("Entry", justify="center")
        table.add_column("Price", justify="center")
        table.add_column("PnL %", justify="center")
        table.add_column("Signal", justify="center")
        table.add_column("Action", justify="center")

        positions_data = []
        self.latest_signals = []

        for symbol in self.symbols:
            df = get_klines(symbol, interval="1m", limit=100)
            df = self.strategy_class(df).generate_signals()
            if df is None or 'signal' not in df.columns:
                continue

            signal = df.iloc[-1]["signal"]
            price = df.iloc[-1]["close"]
            position = self.positions[symbol]
            entry = self.entry_prices[symbol]
            action = "🔁 Hold"
            executed_price = price
            pnl_display = "-"

            # ------------------ LONG SIGNAL ------------------
            if signal == 1:
                if position == 0:  # open long
                    executed_price = price * (1 + self.slippage_pct)
                    fee_cost = executed_price * self.fee_pct
                    self.positions[symbol] = 1
                    self.entry_prices[symbol] = executed_price
                    self.balance -= fee_cost
                    action = f"🟢 Buy (Price w/ slippage: {executed_price:.2f})"
                    send_telegram_message(f"🟢 {symbol}: Buy at {executed_price:.2f}, fee: {fee_cost:.2f}")

                elif position == -1:  # close short & open long
                    executed_price = price * (1 + self.slippage_pct)
                    fee_cost = executed_price * self.fee_pct
                    pnl = ((self.entry_prices[symbol] - executed_price)/self.entry_prices[symbol]*100) - (self.fee_pct*2*100)
                    self.balance *= (1 + pnl/100)
                    self.trade_log.append({
                        "symbol": symbol,
                        "entry_price": self.entry_prices[symbol],
                        "exit_price": executed_price,
                        "pnl_pct": pnl,
                        "exit_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "side": "SHORT"
                    })
                    self.positions[symbol] = 1
                    self.entry_prices[symbol] = executed_price
                    self.balance -= fee_cost
                    action = f"🔴 Close Short ({pnl:.2f}%) → 🟢 Buy"
                    send_telegram_message(f"🔴 {symbol}: Close Short at {executed_price:.2f} | PnL: {pnl:.2f}% → 🟢 Buy")

            # ------------------ SHORT SIGNAL ------------------
            elif signal == -1:
                if position == 0:  # open short
                    executed_price = price * (1 - self.slippage_pct)
                    fee_cost = executed_price * self.fee_pct
                    self.positions[symbol] = -1
                    self.entry_prices[symbol] = executed_price
                    self.balance -= fee_cost
                    action = f"🔴 Short (Price w/ slippage: {executed_price:.2f})"
                    send_telegram_message(f"🔴 {symbol}: Short at {executed_price:.2f}, fee: {fee_cost:.2f}")

                elif position == 1:  # close long & open short
                    executed_price = price * (1 - self.slippage_pct)
                    fee_cost = executed_price * self.fee_pct
                    pnl = ((executed_price - self.entry_prices[symbol])/self.entry_prices[symbol]*100) - (self.fee_pct*2*100)
                    self.balance *= (1 + pnl/100)
                    self.trade_log.append({
                        "symbol": symbol,
                        "entry_price": self.entry_prices[symbol],
                        "exit_price": executed_price,
                        "pnl_pct": pnl,
                        "exit_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "side": "LONG"
                    })
                    self.positions[symbol] = -1
                    self.entry_prices[symbol] = executed_price
                    self.balance -= fee_cost
                    action = f"🟢 Close Long ({pnl:.2f}%) → 🔴 Short"
                    send_telegram_message(f"🟢 {symbol}: Close Long at {executed_price:.2f} | PnL: {pnl:.2f}% → 🔴 Short")

            # ------------------ CURRENT PNL ------------------
            if self.positions[symbol] != 0 and self.entry_prices[symbol]:
                pnl_display = ((price - self.entry_prices[symbol])/self.entry_prices[symbol]*100 if self.positions[symbol]==1 else
                               (self.entry_prices[symbol] - price)/self.entry_prices[symbol]*100)
                pnl_display = f"{pnl_display:.2f}%"

            table.add_row(
                symbol,
                str(self.positions[symbol]),
                f"{self.entry_prices[symbol]:.2f}" if self.entry_prices[symbol] else "-",
                f"{price:.2f}",
                pnl_display,
                str(signal),
                action
            )

            positions_data.append({
                "symbol": symbol,
                "position": self.positions[symbol],
                "entry_price": self.entry_prices[symbol] if self.entry_prices[symbol] else "",
                "current_price": price,
                "pnl_pct": pnl_display,
                "signal": signal,
                "action": action,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

            # Save latest signals with OHLC for dashboard
            latest_row = df.iloc[-1].copy()
            latest_row["pnl_pct"] = pnl_display
            latest_row["position"] = self.positions[symbol]
            self.latest_signals.append(latest_row)

        console.clear()
        console.print(table)

        # Save logs
        self.equity_history.append((datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.calculate_equity()))
        self.save_equity_log()
        self.save_positions_log(positions_data)
        self.save_trades_log()
        self.save_latest_signals()

    def calculate_equity(self):
        equity = self.balance
        for s in self.symbols:
            pos = self.positions[s]
            entry = self.entry_prices[s]
            if pos != 0 and entry:
                latest = get_klines(s, interval="1m", limit=1)
                if not latest.empty:
                    price = latest.iloc[-1]["close"]
                    if pos == 1:
                        equity += (price - entry)
                    elif pos == -1:
                        equity += (entry - price)
        return equity

    def save_equity_log(self):
        df = pd.DataFrame(self.equity_history, columns=["time", "equity"])
        df.to_csv(f"{self.logs_dir}/equity.csv", index=False)

    def save_positions_log(self, data):
        df = pd.DataFrame(data)
        df.to_csv(f"{self.logs_dir}/positions.csv", index=False)

    def save_trades_log(self):
        if not self.trade_log:
            return
        df = pd.DataFrame(self.trade_log)
        df.to_csv(f"{self.logs_dir}/trades.csv", index=False)

    def save_latest_signals(self):
        if not self.latest_signals:
            return
        df = pd.DataFrame(self.latest_signals)
        df.to_csv(f"{self.logs_dir}/latest_signals.csv", index=False)

    def save_logs_and_plot(self):
        self.save_equity_log()
        df = pd.DataFrame(self.equity_history, columns=["time", "equity"])
        df.set_index("time", inplace=True)
        df.plot(title="Equity Curve")
        plt.tight_layout()
        plt.show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--strategy", type=str, required=True)
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()

    strategy_class = load_strategy_class(args.strategy)
    trader = MultiCoinPaperTrader(args.symbols, strategy_class, live=args.live)

    console.print(f"📈 Starting multi-coin paper trading for: {', '.join(args.symbols)} using strategy: {args.strategy}")

    try:
        while True:
            trader.update()
            if not args.live:
                break
            time.sleep(60)
    except KeyboardInterrupt:
        console.print("🛑 Paper trading stopped.")
        trader.save_logs_and_plot()
        print(f"💰 Total Portfolio Value: ${trader.calculate_equity():.2f}")
        print("✅ Logs saved and equity plot displayed.")


if __name__ == "__main__":
    main()
