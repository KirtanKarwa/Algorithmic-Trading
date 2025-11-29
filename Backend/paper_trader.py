import warnings
warnings.filterwarnings("ignore")

import csv
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from rich.console import Console  # type: ignore
from rich.table import Table  # type: ignore
from rich.live import Live  # type: ignore

from utils.binance_connector import BinanceConnector
from utils.telegram_alert import send_telegram_message
import config

from config import STRATEGY_NAME

import argparse

from strategy_loader import load_strategy_class



parser = argparse.ArgumentParser()
parser.add_argument('--strategy', type=str, default="RSI_EMA")
args = parser.parse_args()

strategy_class = load_strategy_class(args.strategy)

# Parameters
SYMBOL = "BTCUSDT"
INTERVAL = "15m"
TRADE_QUANTITY_USD = 100
FEE_PCT = 0.001
SLIPPAGE_PCT = 0.001
MAX_CANDLES = 100

# Initialize
binance = BinanceConnector(config.API_KEY, config.API_SECRET)
balance = 1000.0
position = None
buy_price = None
trades = []

console = Console()

# Equity curve setup
equity_curve = [balance]
timestamps = [pd.Timestamp.now()]
plt.ion()
fig, ax = plt.subplots()
line, = ax.plot(timestamps, equity_curve, label='Equity Curve')
ax.set_title(f'📊 Equity Curve - {args.strategy}')
ax.set_xlabel('Time')
ax.set_ylabel('Balance ($)')
ax.legend()

console.print(f"🚀 Starting PAPER TRADING for [bold]{SYMBOL}[/bold] using [cyan]{args.strategy}[/cyan] strategy...", style="bold green")

# CSV logging
def log_trade_to_csv(trade):
    os.makedirs(f'logs/{args.strategy}', exist_ok=True)
    log_filename = f'logs/{args.strategy}/paperTrading.csv'
    file_exists = os.path.isfile(log_filename)

    with open(log_filename, mode="a", newline='') as file:
        writer = csv.DictWriter(file, fieldnames=trade.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(trade)

# Binance data fetch
def fetch_latest_data():
    klines = binance.get_klines(SYMBOL, INTERVAL, MAX_CANDLES)
    df = pd.DataFrame(klines)
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    return df[['open', 'high', 'low', 'close', 'volume']]

# Build Rich table
def build_status_table(current_time, signal, current_price, position, balance, last_trade):
    table = Table(title="📺 Live Paper Trading Status", title_style="bold cyan")
    table.add_column("⏰ Time", style="dim", no_wrap=True)
    table.add_column("📊 Signal", style="bold")
    table.add_column("💰 Price", justify="right")
    table.add_column("📈 Position", style="bold magenta")
    table.add_column("💼 Balance", justify="right", style="green")
    table.add_column("📝 Last Trade")

    last_trade_info = (
        f"{last_trade['type']} @ {last_trade['price']:.2f}" +
        (f" | PnL: {last_trade.get('profit_pct', 0)*100:.2f}%" if last_trade['type'] == 'SELL' else "")
    ) if last_trade else "None"

    table.add_row(
        str(current_time),
        signal,
        f"{current_price:.2f}",
        position or "None",
        f"${balance:.2f}",
        last_trade_info
    )
    return table

# 📉 Live Trading Loop
try:
    with Live(refresh_per_second=1, console=console) as live:
        while True:
            df = fetch_latest_data()
            df = strategy_class(df).generate_signals()
            last_row = df.iloc[-1]
            signal = last_row['signal']
            current_price = last_row['close']
            current_time = df.index[-1]
            last_trade = trades[-1] if trades else None

            # 🟢 BUY logic
            if signal == 1 and position is None:
                buy_price = current_price * (1 + SLIPPAGE_PCT)
                position = "LONG"
                trade = {'type': 'BUY', 'price': buy_price, 'time': current_time}
                trades.append(trade)
                log_trade_to_csv(trade)
                send_telegram_message(f"🟢 [{args.strategy}] BUY {SYMBOL} @ {buy_price:.2f}")

            # 🔴 SELL logic
            elif signal == -1 and position == "LONG":
                sell_price = current_price * (1 - SLIPPAGE_PCT)
                profit_pct = (sell_price - buy_price) / buy_price - (2 * FEE_PCT)
                balance *= (1 + profit_pct)
                position = None
                trade = {
                    'type': 'SELL',
                    'price': sell_price,
                    'time': current_time,
                    'profit_pct': profit_pct,
                    'balance': balance
                }
                trades.append(trade)
                log_trade_to_csv(trade)
                send_telegram_message(
                    f"🔴 [{args.strategy}] SELL {SYMBOL} @ {sell_price:.2f} | Profit: {profit_pct*100:.2f}% | Balance: ${balance:.2f}"
                )

                # Update equity curve
                timestamps.append(current_time)
                equity_curve.append(balance)
                line.set_xdata(timestamps)
                line.set_ydata(equity_curve)
                ax.relim()
                ax.autoscale_view()
                plt.draw()
                plt.pause(0.01)

            # 🖥️ Update terminal UI
            live.update(build_status_table(current_time, signal, current_price, position, balance, last_trade))

            time.sleep(60)  # For real-time: use 900 for 15-minute candle

# 🛑 Graceful Exit & Save Logs
except KeyboardInterrupt:
    console.print("\n🛑 Trading session stopped by user.", style="bold red")

    os.makedirs(f'logs/{args.strategy}', exist_ok=True)

    # Save equity curve plot
    equity_plot_path = f'logs/{args.strategy}/equity_curve.png'
    fig.savefig(equity_plot_path)
    console.print(f"📈 Equity curve saved to [green]{equity_plot_path}[/green]")

    # Save session summary
    summary_path = f'logs/{args.strategy}/session_summary.txt'
    wins = sum(1 for t in trades if t['type'] == 'SELL' and t.get('profit_pct', 0) > 0)
    losses = sum(1 for t in trades if t['type'] == 'SELL' and t.get('profit_pct', 0) <= 0)
    total_trades = sum(1 for t in trades if t['type'] == 'SELL')

    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(f"📊 STRATEGY: {args.strategy}\n")
        f.write(f"💼 Final Balance: ${balance:.2f}\n")
        f.write(f"📈 Total Trades: {total_trades}\n")
        f.write(f"✅ Wins: {wins}\n")
        f.write(f"❌ Losses: {losses}\n")
        f.write(f"🏆 Win Rate: {(wins / total_trades * 100):.2f}%\n" if total_trades > 0 else "🏆 Win Rate: N/A\n")

    console.print(f"📄 Session summary saved to [green]{summary_path}[/green]")
