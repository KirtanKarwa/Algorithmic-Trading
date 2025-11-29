# callbacks.py
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
import os

def register_callbacks(app, log_dir):
    @app.callback(
        Output("equity-curve", "figure"),
        Output("positions-table", "data"),
        Output("positions-table", "columns"),
        Output("positions-table", "style_data_conditional"),
        Output("trades-table", "data"),
        Output("trades-table", "columns"),
        Output("signal-chart", "figure"),
        Input("interval", "n_intervals")
    )
    def update_dashboard(n):
        # === Equity Curve ===
        equity_path = os.path.join(log_dir, "equity.csv")
        if os.path.exists(equity_path):
            equity_df = pd.read_csv(equity_path)
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=equity_df["time"], 
                y=equity_df["equity"], 
                mode='lines+markers',
                line=dict(color='blue', width=2),
                name="Portfolio Value"
            ))
            fig.update_layout(
                title="Equity Over Time",
                xaxis_title="Time",
                yaxis_title="Portfolio Value ($)",
                template="plotly_dark"
            )
        else:
            fig = go.Figure()

        # === Current Positions ===
        pos_data, pos_columns, style_conditional = [], [], []
        positions_path = os.path.join(log_dir, "positions.csv")
        if os.path.exists(positions_path):
            pos_df = pd.read_csv(positions_path)
            pos_df["pnl_percent"] = pd.to_numeric(pos_df.get("pnl_percent", 0), errors='coerce')
            pos_df["side"] = pos_df.get("side", "0").astype(str)

            pos_data = pos_df.to_dict("records")
            pos_columns = [{"name": col, "id": col} for col in pos_df.columns]

            style_conditional = [
                # PnL coloring
                {'if': {'filter_query': '{pnl_percent} >= 0', 'column_id': 'pnl_percent'}, 'color': 'green', 'fontWeight': 'bold'},
                {'if': {'filter_query': '{pnl_percent} < 0', 'column_id': 'pnl_percent'}, 'color': 'red', 'fontWeight': 'bold'},
                # Side coloring
                {'if': {'filter_query': '{side} == "1"', 'column_id': 'side'}, 'backgroundColor': 'rgba(0,200,0,0.3)'},
                {'if': {'filter_query': '{side} == "-1"', 'column_id': 'side'}, 'backgroundColor': 'rgba(200,0,0,0.3)'}
            ]

        # === Recent Trades ===
        trade_data, trade_columns = [], []
        trades_path = os.path.join(log_dir, "trades.csv")
        if os.path.exists(trades_path):
            trades_df = pd.read_csv(trades_path)
            time_col = "exit_time" if "exit_time" in trades_df.columns else "time"
            trades_df = trades_df.sort_values(time_col, ascending=False).head(50)
            trade_data = trades_df.to_dict("records")
            trade_columns = [{"name": col, "id": col} for col in trades_df.columns]

        # === Signal Chart ===
        signal_fig = go.Figure()
        coins_path = os.path.join(log_dir, "latest_signals.csv")
        # latest_signals.csv should contain: symbol, time, open, high, low, close, signal
        if os.path.exists(coins_path):
            coins_df = pd.read_csv(coins_path)
            symbols = coins_df['symbol'].unique()
            for sym in symbols:
                df_coin = coins_df[coins_df['symbol'] == sym]
                df_coin['time'] = pd.to_datetime(df_coin['time'])
                # Candle
                signal_fig.add_trace(go.Candlestick(
                    x=df_coin['time'],
                    open=df_coin['open'],
                    high=df_coin['high'],
                    low=df_coin['low'],
                    close=df_coin['close'],
                    name=f"{sym} Price"
                ))
                # Long signals
                longs = df_coin[df_coin['signal'] == 1]
                signal_fig.add_trace(go.Scatter(
                    x=longs['time'], y=longs['close'], mode='markers',
                    marker=dict(symbol='triangle-up', color='green', size=12),
                    name=f"{sym} Long"
                ))
                # Short signals
                shorts = df_coin[df_coin['signal'] == -1]
                signal_fig.add_trace(go.Scatter(
                    x=shorts['time'], y=shorts['close'], mode='markers',
                    marker=dict(symbol='triangle-down', color='red', size=12),
                    name=f"{sym} Short"
                ))

            signal_fig.update_layout(title="Live Signals", template="plotly_dark", xaxis_title="Time", yaxis_title="Price")

        return fig, pos_data, pos_columns, style_conditional, trade_data, trade_columns, signal_fig
