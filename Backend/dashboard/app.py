# app.py
import dash
from layout import get_layout
from callbacks import register_callbacks
import os

STRATEGY_NAME = "RSI_EMA"
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs", STRATEGY_NAME)

app = dash.Dash(__name__)
app.title = "AI Trading Bot Dashboard"

# Layout
app.layout = get_layout()

# Callbacks
register_callbacks(app, LOG_DIR)

if __name__ == "__main__":
    app.run_server(debug=True)
