import datetime as dt
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Trade(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    strategy = db.Column(db.String(64), index=True, nullable=False)
    symbol = db.Column(db.String(32), index=True, nullable=False)
    side = db.Column(db.String(8), nullable=False)  # BUY/SELL or LONG/SHORT
    entry = db.Column(db.Float, nullable=False)
    exit = db.Column(db.Float)
    pnl = db.Column(db.Float)
    time = db.Column(db.DateTime, index=True, default=dt.datetime.utcnow)

class Position(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    strategy = db.Column(db.String(64), index=True, nullable=False)
    symbol = db.Column(db.String(32), index=True, nullable=False)
    side = db.Column(db.String(8), nullable=False)
    entry = db.Column(db.Float, nullable=False)
    current = db.Column(db.Float)
    opened_at = db.Column(db.DateTime, default=dt.datetime.utcnow)

class EquitySnapshot(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    strategy = db.Column(db.String(64), index=True, nullable=False)
    time = db.Column(db.DateTime, index=True, default=dt.datetime.utcnow)
    equity = db.Column(db.Float, nullable=False)


