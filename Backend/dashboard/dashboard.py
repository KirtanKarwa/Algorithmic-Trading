# dashboard.py
import os
import pandas as pd
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from plotly.subplots import make_subplots

# === CONFIG ===
STRATEGY_NAME = "RSI_EMA"
LOG_DIR = os.path.join("..", "logs", STRATEGY_NAME)
LAST_N_CANDLES = 50  # for auto-zoom

# Get coins dynamically from latest_signals.csv if exists
latest_signals_path = os.path.join(LOG_DIR, "latest_signals.csv")
if os.path.exists(latest_signals_path):
    try:
        coins_df = pd.read_csv(latest_signals_path)
        COINS = coins_df["symbol"].unique().tolist()
    except pd.errors.EmptyDataError:
        COINS = []
else:
    COINS = []

if not COINS:
    COINS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]  # fallback

# === DASH APP ===
app = dash.Dash(__name__)
app.title = "AI Trading Bot Multi-Coin Dashboard"

app.layout = html.Div([
    html.H1("📊 AI Trading Bot Multi-Coin Dashboard", style={"textAlign": "center"}),

    dcc.Interval(id="interval", interval=60*1000, n_intervals=0),  # refresh every 60s

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
    dash_table.DataTable(
        id="trades-table",
        style_table={'overflowX': 'auto'},
        page_size=10
    ),

    html.H2("📈 Live Signals & Price Charts (All Coins)"),
    html.Label("Select Coins for Zoomed Chart:"),
    dcc.Dropdown(
        id="coin-dropdown",
        options=[{"label": c, "value": c} for c in COINS],
        value=COINS,
        multi=True
    ),
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
    Input("interval", "n_intervals"),
    Input("coin-dropdown", "value")
)
def update_dashboard(n, selected_coins):
    # --- Equity Curve ---
    equity_path = os.path.join(LOG_DIR, "equity.csv")
    try:
        equity_df = pd.read_csv(equity_path)
        if equity_df.empty:
            raise pd.errors.EmptyDataError
        fig_equity = go.Figure()
        fig_equity.add_trace(go.Scatter(
            x=equity_df["time"], 
            y=equity_df["equity"], 
            mode='lines+markers'
        ))
        fig_equity.update_layout(title="Equity Over Time", xaxis_title="Time", yaxis_title="Portfolio Value")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        fig_equity = go.Figure()
        fig_equity.update_layout(title="Equity Over Time", xaxis_title="Time", yaxis_title="Portfolio Value")

    # --- Current Positions ---
    pos_data, pos_columns, style_conditional = [], [], []
    positions_path = os.path.join(LOG_DIR, "positions.csv")
    try:
        pos_df = pd.read_csv(positions_path)
        if not pos_df.empty:
            pos_df["pnl_pct"] = pd.to_numeric(pos_df["pnl_pct"].str.replace("%", ""), errors='coerce')
            pos_data = pos_df.to_dict("records")
            pos_columns = [{"name": col, "id": col} for col in pos_df.columns]

            style_conditional = [
                {'if': {'filter_query': '{pnl_pct} >= 0', 'column_id': 'pnl_pct'},
                 'color': 'green', 'fontWeight': 'bold'},
                {'if': {'filter_query': '{pnl_pct} < 0', 'column_id': 'pnl_pct'},
                 'color': 'red', 'fontWeight': 'bold'}
            ]
    except (FileNotFoundError, pd.errors.EmptyDataError):
        pass

    # --- Recent Trades ---
    trade_data, trade_columns = [], []
    trades_path = os.path.join(LOG_DIR, "trades.csv")
    try:
        trades_df = pd.read_csv(trades_path)
        if not trades_df.empty:
            # Only sort if column exists
            if "exit_time" in trades_df.columns:
                trades_df = trades_df.sort_values("exit_time", ascending=False).head(20)
            trade_data = trades_df.to_dict("records")
            trade_columns = [{"name": col, "id": col} for col in trades_df.columns]
    except (FileNotFoundError, pd.errors.EmptyDataError):
        pass


    # --- Multi-Coin Candlestick Charts ---
    coins_fig = make_subplots(
        rows=len(selected_coins),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=selected_coins
    )

    try:
        signals_df = pd.read_csv(latest_signals_path)
        if not signals_df.empty:
            signals_df["time"] = pd.to_datetime(signals_df["time"])
            for i, coin in enumerate(selected_coins, start=1):
                coin_df = signals_df[signals_df["symbol"] == coin].copy()
                if coin_df.empty:
                    continue

                # Auto-zoom last N candles
                coin_df = coin_df.tail(LAST_N_CANDLES)

                # Candlestick
                coins_fig.add_trace(go.Candlestick(
                    x=coin_df["time"],
                    open=coin_df["open"],
                    high=coin_df["high"],
                    low=coin_df["low"],
                    close=coin_df["close"],
                    name=coin,
                    increasing_line_color='blue',
                    decreasing_line_color='blue',
                    showlegend=False
                ), row=i, col=1)

                # Long signals
                long_signals = coin_df[coin_df["signal"] == 1]
                if not long_signals.empty:
                    hover_text = [f"Long | PnL: {pnl}%" for pnl in long_signals.get("pnl_pct", ["-"]*len(long_signals))]
                    coins_fig.add_trace(go.Scatter(
                        x=long_signals["time"],
                        y=long_signals["close"],
                        mode="markers",
                        marker=dict(symbol="triangle-up", color="green", size=10),
                        name=f"{coin} Long",
                        showlegend=(i==1),
                        text=hover_text,
                        hoverinfo="text"
                    ), row=i, col=1)

                # Short signals
                short_signals = coin_df[coin_df["signal"] == -1]
                if not short_signals.empty:
                    hover_text = [f"Short | PnL: {pnl}%" for pnl in short_signals.get("pnl_pct", ["-"]*len(short_signals))]
                    coins_fig.add_trace(go.Scatter(
                        x=short_signals["time"],
                        y=short_signals["close"],
                        mode="markers",
                        marker=dict(symbol="triangle-down", color="red", size=10),
                        name=f"{coin} Short",
                        showlegend=(i==1),
                        text=hover_text,
                        hoverinfo="text"
                    ), row=i, col=1)
    except (FileNotFoundError, pd.errors.EmptyDataError):
        pass

    coins_fig.update_layout(
        height=300*len(selected_coins) if selected_coins else 300,
        title="Live Coin Signals & Prices",
        xaxis_rangeslider_visible=False
    )

    return fig_equity, pos_data, pos_columns, style_conditional, trade_data, trade_columns, coins_fig


if __name__ == "__main__":
    app.run(debug=True)
