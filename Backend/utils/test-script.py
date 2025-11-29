from utils.binance_connector import BinanceConnector # type: ignore
import config  # type: ignore # where your API_KEY/SECRET live

binance = BinanceConnector(config.API_KEY, config.API_SECRET)

df = binance.get_klines(symbol="BTCUSDT", interval="15m", lookback="200")
print(df.tail())
