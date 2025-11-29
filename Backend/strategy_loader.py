def _strategy_map():
    return {
        "RSI_EMA": ("rsi_ema", "RSIEMAStrategy"),
        "MACD": ("macd", "MACDStrategy"),
        "BOLLINGER_RSI": ("bollinger_rsi", "BollingerRSIStrategy"),
        "SMA_CROSS": ("sma_cross", "SMACrossStrategy"),
        "VOLUME_BREAKOUT": ("volume_breakout", "VolumeBreakoutStrategy"),
        "BREAKOUT_VOLUME": ("breakout_volume", "BreakoutVolumeStrategy"),
        "PSAR_MACD": ("psar_macd", "PsarMacdStrategy"),
        "FIBONACCI_REVERSAL": ("fibonacci_reversal", "FibonacciReversalStrategy"),
        "TRIX": ("trix", "TrixStrategy"),
        "HEIKIN_ASHI_EMA": ("heikin_ashi_ema", "HeikinAshiEmaStrategy"),
        "SUPERTREND_RSI": ("supertrend_rsi", "SupertrendRsiStrategy"),
        "ADX_EMA": ("adx_ema", "AdxEmaStrategy"),
        "ICHIMOKU": ("ichimoku", "IchimokuStrategy"),
        "EMA200_PRICE_ACTION": ("ema200_price_action", "Ema200PriceActionStrategy"),
        "KELTNER_BREAKOUT": ("keltner_channel", "KeltnerBreakoutStrategy"),
    }

def list_strategy_names():
    return list(_strategy_map().keys())

def load_strategy_class(strategy_name):
    strategy_map = _strategy_map()

    if strategy_name not in strategy_map:
        raise ValueError(f"❌ Strategy '{strategy_name}' not found.")
    module_name, class_name = strategy_map[strategy_name]
    module = __import__(f"strategy.{module_name}", fromlist=[class_name])
    return getattr(module, class_name)
