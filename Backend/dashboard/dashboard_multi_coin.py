# dashboard_multi_coin.py
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import os

# === CONFIG ===
STRATEGY_NAME = "RSI_EMA"
LOG_DIR = f"../logs/{STRATEGY_NAME}"

# Read symbols dynamically from latest_signals.csv if exists
latest_signals_path = os.path.join(LOG_DIR, "latest_signals.csv")
if os.path.exists(latest_signals_path):
    COINS = pd.read_csv(latest_signals_path)["symbol"].unique()
else:
    COINS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]  # fallback

# === DASH APP ===
app = dash.Dash(__name__)
app.title = "AI Trading Bot Dashboard"

app.layout = html.Div([
    html.H1("📊 AI Trading Bot Multi-Coin Dashboard", style={"textAlign": "center"}),

    dcc.Interval(id="interval", interval=60000, n_intervals=0),  # refresh every 60s

    html.H2("💰 Equity Curve"),
    dcc.Graph(id="equity-curve"),

    html.H2("💼 Current Positions"),
    dash_table.DataTable(
        id="positions-table",
        style_table={'overflowX': 'auto'},
        style_data_conditional=[],
        page_size=10
    ),

    html.H2("🧾 Recent Trades"),
    dash_table.DataTable(id="trades-table", style_table={'overflowX': 'auto'}, page_size=10),

    html.H2("📈 Live Signals & Price Charts (All Coins)"),
    dcc.Graph(id="coins-chart")
])


@app.callback(
    Output("equity-curve", "figure"),
    Output("positions-table", "data"),
    Output("positions-table", "columns"),
    Output("positions-table", "style_data_conditional"),
    Output("trades-table", "data"),
    Output("trades-table", "columns"),
    Output("coins-chart", "figure"),
    Input("interval", "n_intervals")
)
def update_dashboard(n):
    # --- Equity Curve ---
    equity_path = os.path.join(LOG_DIR, "equity.csv")
    if os.path.exists(equity_path):
        equity_df = pd.read_csv(equity_path)
        fig_equity = go.Figure()
        fig_equity.add_trace(go.Scatter(x=equity_df["time"], y=equity_df["equity"], mode='lines+markers'))
        fig_equity.update_layout(title="Equity Over Time", xaxis_title="Time", yaxis_title="Portfolio Value")
    else:
        fig_equity = go.Figure()

    # --- Current Positions ---
    pos_data, pos_columns, style_conditional = [], [], []
    positions_path = os.path.join(LOG_DIR, "positions.csv")
    if os.path.exists(positions_path):
        pos_df = pd.read_csv(positions_path)
        pos_df["pnl_pct"] = pd.to_numeric(pos_df["pnl_pct"].str.replace("%",""), errors='coerce')
        pos_data = pos_df.to_dict("records")
        pos_columns = [{"name": col, "id": col} for col in pos_df.columns]

        style_conditional = [
            {'if': {'filter_query': '{pnl_pct} >= 0', 'column_id': 'pnl_pct'},
             'color': 'green', 'fontWeight': 'bold'},
            {'if': {'filter_query': '{pnl_pct} < 0', 'column_id': 'pnl_pct'},
             'color': 'red', 'fontWeight': 'bold'}
        ]

    # --- Recent Trades ---
    trade_data, trade_columns = [], []
    trades_path = os.path.join(LOG_DIR, "trades.csv")
    if os.path.exists(trades_path):
        trades_df = pd.read_csv(trades_path)
        trades_df = trades_df.sort_values("exit_time", ascending=False).head(20)
        trade_data = trades_df.to_dict("records")
        trade_columns = [{"name": col, "id": col} for col in trades_df.columns]

    # --- Multi-Coin Candlestick Charts with Signals ---
    coins_fig = make_subplots(rows=len(COINS), cols=1, shared_xaxes=True, vertical_spacing=0.05,
                              subplot_titles=COINS)

    latest_signals_path = os.path.join(LOG_DIR, "latest_signals.csv")
    if os.path.exists(latest_signals_path):
        signals_df = pd.read_csv(latest_signals_path)
        for i, coin in enumerate(COINS, start=1):
            coin_df = signals_df[signals_df["symbol"] == coin].copy()
            if coin_df.empty:
                continue
            coin_df["time"] = pd.to_datetime(coin_df["time"])

            # Candlestick (approximate OHLC)
            coins_fig.add_trace(go.Candlestick(
                x=coin_df["time"], open=coin_df["close"], high=coin_df["close"],
                low=coin_df["close"], close=coin_df["close"], name=coin
            ), row=i, col=1)

            # Long signals
            long_signals = coin_df[coin_df["signal"] == 1]
            coins_fig.add_trace(go.Scatter(
                x=long_signals["time"], y=long_signals["close"],
                mode="markers", marker=dict(symbol="triangle-up", color="green", size=10),
                name=f"{coin} Long", showlegend=(i==1)
            ), row=i, col=1)

            # Short signals
            short_signals = coin_df[coin_df["signal"] == -1]
            coins_fig.add_trace(go.Scatter(
                x=short_signals["time"], y=short_signals["close"],
                mode="markers", marker=dict(symbol="triangle-down", color="red", size=10),
                name=f"{coin} Short", showlegend=(i==1)
            ), row=i, col=1)

    coins_fig.update_layout(height=300*len(COINS), title="Live Coin Signals & Prices", xaxis_rangeslider_visible=False)

    return fig_equity, pos_data, pos_columns, style_conditional, trade_data, trade_columns, coins_fig


if __name__ == "__main__":
    app.run(debug=True)
