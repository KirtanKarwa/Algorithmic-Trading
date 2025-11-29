# layout.py
from dash import dcc, html, dash_table

def get_layout():
    return html.Div([
        html.H1("📊 AI Trading Bot Dashboard", style={"textAlign": "center"}),

        dcc.Interval(id="interval", interval=60*1000, n_intervals=0),

        html.H2("💰 Equity Curve"),
        dcc.Graph(id="equity-curve"),

        html.H2("💼 Current Positions"),
        dash_table.DataTable(
            id="positions-table",
            style_table={'overflowX': 'auto'},
            style_data_conditional=[],
            page_size=20
        ),

        html.H2("🧾 Recent Trades"),
        dash_table.DataTable(
            id="trades-table",
            style_table={'overflowX': 'auto'},
            page_size=20
        ),

        html.H2("⚡ Live Signals"),
        dcc.Graph(id="signal-chart"),
    ])
