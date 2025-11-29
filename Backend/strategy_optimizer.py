import pandas as pd
from backtester import Backtester
from strategy_loader import load_strategy_class

COINS = ["BTCUSDT","ETHUSDT"]
INTERVAL = "15m"
START = "2024-01-01"
END = "2024-06-01"
STRATEGIES = ["RSI_EMA","MACD","BOLLINGER_RSI"]

results = []

for strat_name in STRATEGIES:
    strategy_class = load_strategy_class(strat_name)
    for coin in COINS:
        bt = Backtester(coin, INTERVAL, strategy_class, START, END)
        bt.run()
        stats = bt.calculate_stats()
        stats['strategy'] = strat_name
        stats['coin'] = coin
        results.append(stats)

df = pd.DataFrame(results)
df.to_csv("logs/strategy_optimizer_results.csv", index=False)
print(df.sort_values("Total Return (%)", ascending=False))
