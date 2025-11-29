import os
import logging
import datetime as dt
from functools import wraps
from typing import Optional, List, Dict, Any
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_compress import Compress
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv
import jwt as pyjwt
import json
import time

load_dotenv()

app = Flask(__name__)
Compress(app)

# === Config (env-driven with safe defaults) ===
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me")
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:///ai_trader.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# CORS: allow your Next.js dev server; tighten in production
CORS(app, resources={r"/api/*": {"origins": [os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")]}},
     expose_headers=["Authorization", "Content-Type"],
     allow_headers=["Authorization", "Content-Type"]) 

# Optional separate models module
try:
    from models import db, Trade, Position, EquitySnapshot  # type: ignore
    db.init_app(app)
except Exception:
    Trade = Position = EquitySnapshot = None  # type: ignore
    db = SQLAlchemy(app)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("api")


# === DB Models ===
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False)
    name = db.Column(db.String(255))
    password_hash = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=dt.datetime.utcnow)

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)


with app.app_context():
    db.create_all()

@app.get("/api/health")
def health():
    return jsonify(ok=True)


# === Auth helpers ===
def create_jwt(user_id: int) -> str:
    payload = {
        "sub": user_id,
        "exp": dt.datetime.utcnow() + dt.timedelta(days=7),
        "iat": dt.datetime.utcnow(),
    }
    return pyjwt.encode(payload, app.config["SECRET_KEY"], algorithm="HS256")


def token_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return jsonify({"message": "Missing or invalid token"}), 401
        token = auth_header.split(" ", 1)[1]
        try:
            decoded = pyjwt.decode(token, app.config["SECRET_KEY"], algorithms=["HS256"])
            request.user_id = decoded.get("sub")
        except Exception:
            return jsonify({"message": "Invalid or expired token"}), 401
        return fn(*args, **kwargs)
    return wrapper


@app.post("/api/register")
def register():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    name = data.get("name") or ""
    if not email or not password:
        return jsonify({"message": "Missing email or password"}), 400
    if User.query.filter_by(email=email).first():
        return jsonify({"message": "Email already registered"}), 409
    user = User(email=email, name=name)
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    return jsonify({"ok": True}), 200


@app.post("/api/login")
def login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not email or not password:
        return jsonify({"message": "Missing email or password"}), 400
    user = User.query.filter_by(email=email).first()
    if not user or not user.check_password(password):
        return jsonify({"message": "Invalid credentials"}), 401
    token = create_jwt(user.id)
    return jsonify({"token": token}), 200


_cache: dict[str, tuple[float, any]] = {}
_CACHE_TTL = float(os.getenv("CACHE_TTL_SECONDS", "10"))

def _cache_get(key: str):
    try:
        ts, val = _cache.get(key, (0, None))
        if time.time() - ts < _CACHE_TTL:
            return val
    except Exception:
        pass
    return None

def _cache_set(key: str, val: any):
    try:
        _cache[key] = (time.time(), val)
    except Exception:
        pass


@app.get("/api/equity")
@token_required
def equity():
    strategy = request.args.get("strategy", "RSI_EMA")
    ck = f"equity:{strategy}"
    cached = _cache_get(ck)
    if cached is not None:
        return jsonify({"points": cached})
    # Prefer DB if available
    if EquitySnapshot is not None:
        try:
            snaps = (
                EquitySnapshot.query.filter_by(strategy=strategy)  # type: ignore
                .order_by(EquitySnapshot.time.asc())  # type: ignore
                .all()
            )
            if snaps:
                points = [{"t": str(s.time), "v": float(s.equity)} for s in snaps]
                _cache_set(ck, points)
                return jsonify({"points": points})
        except Exception as e:
            logger.warning("Equity from DB failed: %s", e)
    # Fallback to logs
    try:
        from strategy_loader import load_strategy_class  # type: ignore
        strategy_class = load_strategy_class(strategy)
        logs_dir = f"logs/{strategy_class.__name__}"
        import pandas as pd  # type: ignore
        eq_df = pd.read_csv(f"{logs_dir}/equity.csv")
        points = [{"t": str(r.time), "v": float(r.equity)} for r in eq_df.itertuples()]  # type: ignore
        _cache_set(ck, points)
        return jsonify({"points": points})
    except Exception as e:
        logger.warning("Equity from logs failed: %s", e)
        return jsonify({"points": []})


@app.get("/api/pnl")
@token_required
def pnl():
    strategy = request.args.get("strategy", "RSI_EMA")
    ck = f"pnl:{strategy}"
    cached = _cache_get(ck)
    if cached is not None:
        return jsonify(cached)
    # Prefer DB
    if EquitySnapshot is not None:
        try:
            snaps = (
                EquitySnapshot.query.filter_by(strategy=strategy)  # type: ignore
                .order_by(EquitySnapshot.time.asc())  # type: ignore
                .all()
            )
            if snaps:
                start_eq = float(snaps[0].equity)
                end_eq = float(snaps[-1].equity)
                change_pct = ((end_eq - start_eq) / start_eq * 100.0) if start_eq else 0.0
                trade_count = 0
                if Trade is not None:
                    trade_count = Trade.query.filter_by(strategy=strategy).count()  # type: ignore
                payload = {
                    "balance": f"${end_eq:,.2f}",
                    "change24h": f"{change_pct:.2f}%",
                    "openPositions": 0,
                    "tradeCount": trade_count,
                }
                _cache_set(ck, payload)
                return jsonify(payload)
        except Exception as e:
            logger.warning("PnL from DB failed: %s", e)
    # Fallback to logs
    try:
        from strategy_loader import load_strategy_class  # type: ignore
        strategy_class = load_strategy_class(strategy)
        logs_dir = f"logs/{strategy_class.__name__}"
        import pandas as pd  # type: ignore
        eq_df = pd.read_csv(f"{logs_dir}/equity.csv")
        start_eq = float(eq_df.iloc[0]["equity"]) if not eq_df.empty else 0.0
        end_eq = float(eq_df.iloc[-1]["equity"]) if not eq_df.empty else 0.0
        change_pct = ((end_eq - start_eq) / start_eq * 100.0) if start_eq else 0.0
        try:
            tr_df = pd.read_csv(f"{logs_dir}/backtester.csv")
            trade_count = len(tr_df)
        except Exception:
            trade_count = 0
        payload = {
            "balance": f"${end_eq:,.2f}",
            "change24h": f"{change_pct:.2f}%",
            "openPositions": 0,
            "tradeCount": trade_count,
        }
        _cache_set(ck, payload)
        return jsonify(payload)
    except Exception as e:
        logger.warning("PnL from logs failed: %s", e)
        return jsonify({"balance": "$0.00", "change24h": "0.00%", "openPositions": 0, "tradeCount": 0})


@app.get("/api/trades")
@token_required
def trades():
    strategy = request.args.get("strategy", "RSI_EMA")
    ck = f"trades:{strategy}"
    cached = _cache_get(ck)
    if cached is not None:
        return jsonify({"rows": cached})
    # Prefer DB
    if Trade is not None:
        try:
            q = Trade.query.filter_by(strategy=strategy).order_by(Trade.time.desc()).limit(200)  # type: ignore
            rows = []
            for t in q.all():  # type: ignore
                rows.append({
                    "id": t.id,
                    "time": str(t.time),
                    "symbol": t.symbol,
                    "side": t.side,
                    "entry": f"{t.entry:.2f}",
                    "exit": f"{(t.exit or 0):.2f}",
                    "pnl": f"{(t.pnl or 0):.2f}",
                    "strategy": t.strategy,
                })
            _cache_set(ck, rows)
            return jsonify({"rows": rows})
        except Exception as e:
            logger.warning("Trades from DB failed: %s", e)
    # Fallback to logs
    try:
        from strategy_loader import load_strategy_class  # type: ignore
        strategy_class = load_strategy_class(strategy)
        logs_dir = f"logs/{strategy_class.__name__}"
        import pandas as pd  # type: ignore
        tr_df = pd.read_csv(f"{logs_dir}/backtester.csv")
        rows = tr_df.to_dict(orient="records")  # type: ignore
        _cache_set(ck, rows)
        return jsonify({"rows": rows})
    except Exception as e:
        logger.warning("Trades from logs failed: %s", e)
        return jsonify({"rows": []})


@app.get("/api/positions")
@token_required
def positions():
    # Return positions from DB if model is available
    if Position is None:
        return jsonify({"rows": []})
    rows = []
    for p in Position.query.order_by(Position.opened_at.desc()).limit(100).all():  # type: ignore
        rows.append({
            "id": p.id,
            "symbol": p.symbol,
            "side": p.side,
            "entry": f"{p.entry:.2f}",
            "current": f"{(p.current or 0):.2f}",
            "pnl": "",
            "strategy": p.strategy,
            "duration": "",
        })
    return jsonify({"rows": rows})


@app.post("/api/backtest")
@token_required
def backtest():
    # Use real backtester
    try:
        payload = request.get_json(silent=True) or {}
        symbol = (payload.get("symbol") or "BTCUSDT").upper()
        interval = payload.get("interval") or "15m"
        strategy_name = payload.get("strategy") or "RSI_EMA"
        date_range = payload.get("range") or {}
        start = date_range.get("from") or "2025-01-01"
        end = date_range.get("to") or "2025-02-01"

        # Lazy import to keep startup fast
        from strategy_loader import load_strategy_class  # type: ignore
        from backtester import Backtester  # type: ignore

        strategy_class = load_strategy_class(strategy_name)
        backtester = Backtester(symbol, interval, strategy_class, start, end)
        backtester.run()

        # Build equity points directly from in-memory results first
        equity_points = []
        try:
            for ts, eq in zip(backtester.timestamps, backtester.equity_curve):
                equity_points.append({"t": str(ts), "v": float(eq)})
        except Exception as e:
            logger.warning("Failed to build in-memory equity points: %s", e)

        # If empty, fall back to logs
        if not equity_points:
            try:
                logs_dir = f"logs/{strategy_class.__name__}"
                import pandas as pd  # type: ignore
                eq_df = pd.read_csv(f"{logs_dir}/equity.csv")
                for _, row in eq_df.iterrows():
                    equity_points.append({"t": str(row["time"]), "v": float(row["equity"])})
            except Exception as e:
                logger.warning("Failed to read equity.csv: %s", e)

        # Compute stats from backtester
        stats = backtester.calculate_stats()

        # Build trades directly from in-memory results first
        trades_rows: List[Dict[str, Any]] = []
        try:
            trades_rows = list(backtester.trades)
            logger.info(f"Found {len(trades_rows)} trades from backtester.trades")
            # enrich with symbol and strategy
            for tr in trades_rows:
                tr.setdefault("symbol", symbol)
                tr.setdefault("strategy", strategy_name)
        except Exception as e:
            logger.warning(f"Failed to get trades from backtester: {e}")
            trades_rows = []

        # If empty, fall back to logs
        if not trades_rows:
            try:
                logs_dir = f"logs/{strategy_class.__name__}"
                import pandas as pd  # type: ignore
                tr_df = pd.read_csv(f"{logs_dir}/backtester.csv")
                rows = tr_df.to_dict(orient="records")  # type: ignore
                logger.info(f"Loaded {len(rows)} trades from CSV file")
                # enrich with symbol and strategy
                for r in rows:
                    r.setdefault("symbol", symbol)
                    r.setdefault("strategy", strategy_name)
                trades_rows = rows
            except Exception as e:
                logger.warning(f"Failed to load trades from CSV: {e}")
                pass

        resp = {
            "equity": {"points": equity_points},
            "stats": {
                "finalBalance": f"${stats.get('Final Balance', 0):.2f}",
                "totalReturn": f"{stats.get('Total Return (%)', 0):.2f}%",
                "maxDD": f"{stats.get('Max Drawdown (%)', 0):.2f}%",
                "winRate": f"{stats.get('Win Rate (%)', 0):.2f}%",
                "sharpe": f"{stats.get('Sharpe Ratio', 0):.2f}",
            },
            "trades": {"rows": trades_rows},
        }

        # Ingest into DB if available
        if EquitySnapshot is not None:
            try:
                # clear previous snapshots for this strategy to avoid duplication
                db.session.query(EquitySnapshot).filter_by(strategy=strategy_name).delete()  # type: ignore
                import pandas as pd  # type: ignore
                for p in equity_points:
                    snap = EquitySnapshot(strategy=strategy_name, time=pd.to_datetime(p["t"]), equity=float(p["v"]))  # type: ignore
                    db.session.add(snap)
                db.session.commit()
            except Exception as e:
                logger.warning("Failed to ingest equity snapshots: %s", e)
                db.session.rollback()
        try:
            logger.info(f"Saving {len(trades_rows)} trades for strategy {strategy_name}")
            # naive ingestion: delete then insert current backtest trades for this strategy
            db.session.query(Trade).filter_by(strategy=strategy_name).delete()  # type: ignore
            import pandas as pd  # type: ignore
            for row in trades_rows:
                trade = Trade(
                    strategy=strategy_name,
                    symbol=row.get("symbol") or symbol,
                    side=row.get("type") or row.get("side") or "",
                    entry=float(row.get("price") or row.get("entry") or 0),
                    exit=float(row.get("exit") or 0) if row.get("exit") not in (None, "") else None,
                    pnl=float(row.get("pnl") or 0) if str(row.get("pnl") or "").replace("-","",1).replace(".","",1).isdigit() else None,
                    time=pd.to_datetime(row.get("time")) if row.get("time") else dt.datetime.utcnow(),  # type: ignore
                )
                db.session.add(trade)
            db.session.commit()
            logger.info(f"Successfully saved {len(trades_rows)} trades to database")
        except Exception as e:
            logger.warning("Failed to ingest trades: %s", e)
            db.session.rollback()

        return jsonify(resp)
    except Exception as e:
        logger.exception("Backtest error")
        return jsonify({"message": "Backtest failed", "error": str(e)}), 500


@app.post("/api/backtest/load-csv")
@token_required
def load_csv_trades():
    try:
        strategy = request.args.get("strategy", "RSI_EMA")
        symbol = request.args.get("symbol", "BTCUSDT")
        
        # Load trades from CSV
        logs_dir = f"logs/{strategy}Strategy"
        import pandas as pd
        tr_df = pd.read_csv(f"{logs_dir}/backtester.csv")
        rows = tr_df.to_dict(orient="records")
        
        # Clear existing trades for this strategy
        db.session.query(Trade).filter_by(strategy=strategy).delete()
        
        # Add new trades
        for row in rows:
            trade = Trade(
                strategy=strategy,
                symbol=row.get("symbol") or symbol,
                side=row.get("side") or "",
                entry=float(row.get("entry") or 0),
                exit=float(row.get("exit") or 0) if row.get("exit") not in (None, "") else None,
                pnl=float(row.get("pnl") or 0) if str(row.get("pnl") or "").replace("-","",1).replace(".","",1).isdigit() else None,
                time=pd.to_datetime(row.get("time")) if row.get("time") else dt.datetime.utcnow(),
            )
            db.session.add(trade)
        
        db.session.commit()
        return jsonify({"message": f"Loaded {len(rows)} trades for {strategy}"})
    except Exception as e:
        logger.warning(f"Failed to load CSV trades: {e}")
        return jsonify({"error": str(e)}), 500

@app.get("/api/backtest/results")
@token_required
def get_backtest_results():
    try:
        symbol = request.args.get("symbol", "")
        strategy = request.args.get("strategy", "")
        
        logger.info(f"Fetching backtest results for symbol={symbol}, strategy={strategy}")
        
        # Get trades from database
        trades_query = db.session.query(Trade)
        if symbol:
            trades_query = trades_query.filter(Trade.symbol.like(f"%{symbol}%"))
        if strategy:
            trades_query = trades_query.filter(Trade.strategy == strategy)
        
        trades = trades_query.order_by(Trade.time.desc()).limit(1000).all()
        logger.info(f"Found {len(trades)} trades in database")
        
        trades_rows = []
        for trade in trades:
            trades_rows.append({
                "time": trade.time.isoformat() if trade.time else "",
                "symbol": trade.symbol or "",
                "side": trade.side or "",
                "entry": trade.entry or 0,
                "exit": trade.exit or 0,
                "pnl": trade.pnl or 0,
                "strategy": trade.strategy or ""
            })
        
        logger.info(f"Returning {len(trades_rows)} trade rows")
        return jsonify({"trades": {"rows": trades_rows}})
    except Exception as e:
        logger.warning("Failed to get backtest results: %s", e)
        return jsonify({"trades": {"rows": []}})


@app.post("/api/papertrading")
@token_required
def papertrading():
    data = request.get_json(silent=True) or {}
    action = data.get("action")
    if action not in ("start", "stop"):
        return jsonify({"message": "action must be 'start' or 'stop'"}), 400

    # update local paper state
    symbol = (data.get("symbol") or "BTCUSDT").upper()
    strategy = data.get("strategy") or "RSI_EMA"
    interval = data.get("interval") or data.get("timeframe") or "15m"

    try:
        state = _load_paper_state()
        if action == "start":
            state["live"] = True
            state["symbol"] = symbol
            state["strategy"] = strategy
            state["interval"] = interval
        else:
            state["live"] = False
        _save_paper_state(state)
        return jsonify({"ok": True, "action": action, "state": state})
    except Exception as e:
        return jsonify({"message": "Failed to update paper state", "error": str(e)}), 500

@app.post("/api/optimizer")
@token_required
def optimizer_run():
    try:
        from strategy_loader import load_strategy_class, list_strategy_names  # type: ignore
        from backtester import Backtester  # type: ignore
        import pandas as pd  # type: ignore

        payload = request.get_json(silent=True) or {}
        symbols = payload.get("symbols") or ["BTCUSDT", "ETHUSDT"]
        strategies = payload.get("strategies") or list_strategy_names()
        # Accept both interval or timeframe
        interval = payload.get("interval") or payload.get("timeframe") or "1h"
        # Accept direct start/end or {range:{from,to}}
        if isinstance(payload.get("range"), dict):
            start = payload["range"].get("from") or "2025-01-01"
            end = payload["range"].get("to") or "2025-02-01"
        else:
            start = payload.get("start") or "2025-01-01"
            end = payload.get("end") or "2025-02-01"

        rows = []
        # --- Parameter grids per strategy (kept small to control runtime) ---
        param_grids: dict[str, list[dict]] = {
            "RSI_EMA": [
                {"rsi_period": rp, "ema_period": ep, "rsi_buy": rb, "rsi_sell": rs}
                for rp in [7, 14]
                for ep in [20, 50]
                for rb in [30, 40, 45]
                for rs in [55, 60, 70]
            ],
            "MACD": [
                {"window_fast": wf, "window_slow": ws, "window_sign": sg, "ema200_span": e2}
                for wf in [8, 12]
                for ws in [24, 26, 35]
                for sg in [9, 12]
                for e2 in [100, 200]
                if wf < ws
            ],
            "SMA_CROSS": [
                {"short_window": sw, "long_window": lw}
                for sw in [20, 50]
                for lw in [100, 200]
                if sw < lw
            ],
            "EMA200_PRICE_ACTION": [
                {"ema_span": s} for s in [100, 200]
            ],
            "TRIX": [
                {"signal_window": w} for w in [5, 9, 14]
            ],
            "BOLLINGER_RSI": [
                {"bb_window": bw, "bb_std": bs, "rsi_window": rw, "rsi_buy": rb, "rsi_sell": rs}
                for bw in [14, 20]
                for bs in [1.5, 2.0]
                for rw in [14]
                for rb in [25, 30]
                for rs in [70, 75]
            ],
            "VOLUME_BREAKOUT": [
                {"avg_window": aw, "min_change": mc, "min_vol_mult": vm}
                for aw in [20, 30]
                for mc in [0.0, 0.005]
                for vm in [1.0, 1.5]
            ],
            "BREAKOUT_VOLUME": [
                {"breakout_window": bw, "min_vol_mult": vm}
                for bw in [20, 50]
                for vm in [1.0, 1.5]
            ],
            "PSAR_MACD": [
                {"psar_step": p, "psar_max": pm, "window_fast": wf, "window_slow": ws, "window_sign": sg}
                for p in [0.02, 0.03]
                for pm in [0.2]
                for wf in [8, 12]
                for ws in [24, 26]
                for sg in [9]
            ],
            "FIBONACCI_REVERSAL": [
                {"lookback": lb, "retrace": rt}
                for lb in [50, 100]
                for rt in [0.5, 0.618]
            ],
            "HEIKIN_ASHI_EMA": [
                {"ema_span": s} for s in [20, 50]
            ],
            "SUPERTREND_RSI": [
                {"stc_fast": sf, "stc_slow": ss, "stc_cycle": sc, "stc_buy": sb, "stc_sell": sl, "rsi_window": rw, "rsi_buy": rb, "rsi_sell": rs}
                for sf in [23]
                for ss in [50]
                for sc in [10]
                for sb in [50]
                for sl in [50]
                for rw in [14]
                for rb in [25, 30]
                for rs in [70, 75]
            ],
            "ADX_EMA": [
                {"ema_span": es, "adx_threshold": at}
                for es in [20, 50]
                for at in [20, 25, 30]
            ],
            "ICHIMOKU": [
                {"window1": w1, "window2": w2}
                for w1 in [9]
                for w2 in [26, 34]
            ],
            "KELTNER_BREAKOUT": [
                {"window": w, "window_atr": wa, "original": o}
                for w in [20, 30]
                for wa in [10, 20]
                for o in [False, True]
            ],
        }

        for strat in strategies:
            strat_class = load_strategy_class(strat)
            grid = param_grids.get(strat, [{}])
            best_row = None
            for params in grid:
                # Evaluate across symbols and keep the best per-strategy over both params and symbols
                for sym in symbols:
                    bt = Backtester(sym, interval, strat_class, start, end, strategy_params=params)
                    bt.run()
                    stats = bt.calculate_stats()
                    row = {
                        "strategy": strat,
                        "symbol": sym,
                        "totalReturn": float(stats.get("Total Return (%)", 0)),
                        "maxDD": float(stats.get("Max Drawdown (%)", 0)),
                        "winRate": float(stats.get("Win Rate (%)", 0)),
                        "sharpe": float(stats.get("Sharpe Ratio", 0)),
                        "params": params,
                    }
                    if best_row is None or row["totalReturn"] > best_row["totalReturn"]:
                        best_row = row
            if best_row is not None:
                rows.append(best_row)
        import os, json as _json
        df = pd.DataFrame(rows)
        # Already one row per strategy (best over params and symbols)
        os.makedirs("logs/optimizer", exist_ok=True)
        out_path = "logs/optimizer/optimizer_results.csv"
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        df.to_csv(out_path, index=False)
        # Persist meta used for this run
        try:
            meta_path = "logs/optimizer/meta.json"
            used_params = {"interval": interval, "start": start, "end": end, "symbols": symbols, "strategies": strategies}
            with open(meta_path, "w", encoding="utf-8") as f:
                _json.dump(used_params, f)
        except Exception:
            pass
        return jsonify({"ok": True})
    except Exception as e:
        logger.exception("Optimizer run failed")
        return jsonify({"message": "Optimizer failed", "error": str(e)}), 500


@app.get("/api/optimizer")
@token_required
def optimizer_results():
    try:
        import pandas as pd  # type: ignore
        try:
            df = pd.read_csv("logs/optimizer/optimizer_results.csv")
        except Exception:
            # default mock rows
            df = pd.DataFrame([
                {"strategy": "RSI_EMA", "symbol": "BTCUSDT", "totalReturn": 18.4, "maxDD": -6.1, "winRate": 54.0, "sharpe": 1.42},
                {"strategy": "MACD", "symbol": "ETHUSDT", "totalReturn": 12.7, "maxDD": -8.9, "winRate": 51.0, "sharpe": 1.12},
            ])
        df = df.copy()
        # Defensive deduplication: keep only best (max totalReturn) per strategy
        try:
            if not df.empty and "strategy" in df.columns and "totalReturn" in df.columns:
                best_idx = df.groupby("strategy")["totalReturn"].idxmax()
                df = df.loc[best_idx].reset_index(drop=True)
        except Exception:
            pass
        # Keep a consistent display order by totalReturn desc
        df["rank"] = df["totalReturn"].rank(ascending=False, method="first").astype(int)
        df = df.sort_values(["rank", "strategy"]).reset_index(drop=True)
        # format for frontend
        out = []
        for _, r in df.iterrows():
            out.append({
                "strategy": str(r.get("strategy", "")),
                "totalReturn": f"{float(r.get('totalReturn', 0)):.1f}%",
                "maxDD": f"{float(r.get('maxDD', 0)):.1f}%",
                "winRate": f"{float(r.get('winRate', 0)):.0f}%",
                "sharpe": f"{float(r.get('sharpe', 0)):.2f}",
                "rank": int(r.get("rank", 0)),
                "params": str(r.get("params", "")),
            })
        # include meta if available
        meta = {}
        try:
            import json as _json
            with open("logs/optimizer/meta.json", "r", encoding="utf-8") as f:
                meta = _json.load(f)
        except Exception:
            pass
        return jsonify({"rows": out, "meta": meta})
    except Exception as e:
        logger.exception("Optimizer results failed")
        return jsonify({"message": "Failed to load optimizer results", "error": str(e)}), 500

# TODO: wire your real endpoints here, e.g.
# @app.post("/api/login")
# @app.post("/api/backtest")
# @app.get("/api/equity")
# @app.get("/api/pnl")
# @app.get("/api/trades")
# @app.get("/api/positions")2
# @app.post("/api/papertrading")

STATE_DIR = os.path.join("logs", "paper")
os.makedirs(STATE_DIR, exist_ok=True)
STATE_PATH = os.path.join(STATE_DIR, "state.json")


def _load_paper_state():
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    state = {"balance": 1000.0, "symbol": "BTCUSDT", "live": False, "strategy": "RSI_EMA", "interval": "15m"}
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception:
        pass
    return state


def _save_paper_state(state):
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception:
        pass


@app.get("/api/paper/balance")
@token_required
def paper_balance():
    state = _load_paper_state()
    return jsonify({
        "balance": state.get("balance", 0.0),
        "symbol": state.get("symbol", "BTCUSDT"),
        "live": bool(state.get("live", False)),
        "strategy": state.get("strategy", "RSI_EMA"),
        "interval": state.get("interval", "15m"),
    })


@app.post("/api/paper/deposit")
@token_required
def paper_deposit():
    data = request.get_json(silent=True) or {}
    amount = float(data.get("amount") or 0)
    if amount <= 0:
        return jsonify({"message": "amount must be > 0"}), 400
    state = _load_paper_state()
    if bool(state.get("live")):
        return jsonify({"message": "Cannot deposit while paper trading is live"}), 400
    state["balance"] = float(state.get("balance", 0.0)) + amount
    _save_paper_state(state)
    return jsonify({"ok": True, "balance": state["balance"]})


@app.post("/api/paper/withdraw")
@token_required
def paper_withdraw():
    data = request.get_json(silent=True) or {}
    amount = float(data.get("amount") or 0)
    if amount <= 0:
        return jsonify({"message": "amount must be > 0"}), 400
    state = _load_paper_state()
    if bool(state.get("live")):
        return jsonify({"message": "Cannot withdraw while paper trading is live"}), 400
    bal = float(state.get("balance", 0.0))
    if amount > bal:
        return jsonify({"message": "insufficient balance"}), 400
    state["balance"] = bal - amount
    _save_paper_state(state)
    return jsonify({"ok": True, "balance": state["balance"]})


@app.post("/api/paper/symbol")
@token_required
def paper_set_symbol():
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "BTCUSDT").upper()
    state = _load_paper_state()
    state["symbol"] = symbol
    _save_paper_state(state)
    return jsonify({"ok": True, "symbol": symbol})


@app.get("/api/paper/results")
@token_required
def paper_results():
    try:
        strategy = request.args.get("strategy", "RSI_EMA")
        import pandas as pd  # type: ignore
        path = os.path.join("logs", strategy, "paperTrading.csv")
        if not os.path.exists(path):
            return jsonify({"rows": []})
        df = pd.read_csv(path)
        return jsonify({"rows": df.to_dict(orient="records")})
    except Exception as e:
        logger.warning("paper results failed: %s", e)
        return jsonify({"rows": []})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False, threaded=True)