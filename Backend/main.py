# main.py

import time
from utils.binance_connector import BinanceConnector
from strategy.rsi_ema import RSIEMAStrategy
from alerts.telegram_alert import TelegramAlert  # type: ignore # Make sure this file exists
import config  # type: ignore # contains API_KEY and API_SECRET
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

telegram = TelegramAlert(
    token=TELEGRAM_TOKEN,
    chat_id=TELEGRAM_CHAT_ID
)



SYMBOL = "BTCUSDT"
INTERVAL = "15m"
LOOKBACK = 100

# 🚨 Replace with your actual token & chat_id
telegram = TelegramAlert(
    token="7711623533:AAEyb4Sqt4F8aRdNudbQFLCCZQ39qCvOkgw",
    chat_id="8111985665"
)

# 🔧 TEMP TEST — REMOVE AFTER TESTING
telegram.send_alert("✅ Test message from your AI trading bot!")


def main():
    print("🚀 AI Trading Bot Started...")
    binance = BinanceConnector(config.API_KEY, config.API_SECRET)

    while True:
        try:
            df = binance.get_klines(symbol=SYMBOL, interval=INTERVAL, lookback=LOOKBACK)
            strategy = RSIEMAStrategy(df)
            signals = strategy.generate_signals()
            latest = signals.iloc[-1]

            print(f"\n🕒 {latest['time']} | {SYMBOL} | Price: {latest['close']}")
            print(f"→ EMA: {latest['EMA']:.2f} | RSI: {latest['RSI']:.2f} | Signal: {latest['Signal']}")

            # 📢 Send alert if there's a BUY or SELL signal
            if latest['Signal'] in ['BUY', 'SELL']:
                alert_msg = f"""
📢 {latest['Signal']} Signal!
Symbol: {SYMBOL}
Price: ${latest['close']}
RSI: {latest['RSI']:.2f}
EMA: {latest['EMA']:.2f}
Time: {latest['time']}
"""
                telegram.send_alert(alert_msg.strip())

        except Exception as e:
            print(f"❌ Error: {e}")

        time.sleep(60 * 15)  # Wait for the next 15-minute candle

if __name__ == "__main__":
    main()
