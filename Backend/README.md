AI Trading Backend

Setup

1) Python env

```
cd Backend
python -m venv venv
# PowerShell
.\venv\Scripts\Activate.ps1
# or cmd
venv\Scripts\activate.bat
```

2) Install deps

```
pip install flask flask-cors flask-sqlalchemy werkzeug PyJWT python-dotenv pandas mplfinance alembic
```

3) Env vars (create .env)

```
SECRET_KEY=change-me
DATABASE_URL=sqlite:///ai_trader.db
FRONTEND_ORIGIN=http://localhost:3000
LOG_LEVEL=INFO
```

4) Run

```
python api.py
```

Alembic

Initialize (once):

```
alembic init migrations
```

Set sqlalchemy.url in alembic.ini to match DATABASE_URL.

Generate migration:

```
alembic revision --autogenerate -m "init tables"
```

Apply migrations:

```
alembic upgrade head
```

Endpoints

- POST /api/register
- POST /api/login
- GET /api/equity
- GET /api/pnl
- GET /api/trades
- GET /api/positions
- POST /api/backtest
- POST /api/papertrading

All except register/login require Authorization: Bearer <token>.
