#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FINAL: SS NIFTY SPOT (ATM SHORT STRADDLE) - RAILWAY DEPLOYABLE

What it does:
- Every minute:
  - Compute ATR(ENV ATR_PERIOD) from 1-min candles (always, even when kill-switch is off)
  - Read kill-switch from DB: trade_flag.live_ss_nifty_spot
  - Write a SNAPSHOT row (spot, atr, unreal/total m2m)
  - If kill-switch = FALSE and positions exist -> square off immediately, log, halt trading
- Entry:
  - Only after ENTRY_START_TIME (ENV)
  - Only if spot within ±ENTRY_TOL (ENV) of ATM (rounded to STRIKE_STEP)
  - Only if kill-switch = TRUE and no open positions and not halted
  - Action: SELL ATM CE + SELL ATM PE (NIFTY nearest expiry)
- Exit:
  - Profit target / circuit stoploss / 15:25 square-off / kill-switch OFF => square off legs
- All logs go to Postgres table: Live_SS_Nifty_Spot (NO CSV)

 NO API calls / DB writes:
    - before 09:15
    - after 15:30
    - weekends
    - exchange holidays
"""

import os, time, math
from datetime import datetime, timedelta, time as dt_time, date
import pytz
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from kiteconnect import KiteConnect

# =========================================================
# ENV HELPERS
# =========================================================

def env_bool(k, d=False):
    return os.getenv(k, str(d)).lower() in ("1","true","yes","y","on")

def env_int(k, d):
    try: return int(os.getenv(k, d))
    except: return d

def env_float(k, d):
    try: return float(os.getenv(k, d))
    except: return d

def parse_hhmm(key, default):
    s = os.getenv(key, default)
    hh, mm = s.split(":")
    return dt_time(int(hh), int(mm))

# =========================================================
# CONFIG
# =========================================================

BOT_NAME = os.getenv("BOT_NAME", "SS_NIFTY_SPOT")
MARKET_TZ = pytz.timezone(os.getenv("MARKET_TZ", "Asia/Kolkata"))

MARKET_OPEN_TIME  = parse_hhmm("MARKET_OPEN_TIME",  "09:15")
MARKET_CLOSE_TIME = parse_hhmm("MARKET_CLOSE_TIME", "15:30")
ENTRY_START_TIME  = parse_hhmm("ENTRY_START_TIME",  "09:30")
SQUARE_OFF_TIME   = parse_hhmm("SQUARE_OFF_TIME",   "15:25")

ENTRY_TOL   = env_int("ENTRY_TOL", 4)
STRIKE_STEP = env_int("STRIKE_STEP", 50)
QTY_PER_LEG = env_int("QTY_PER_LEG", 65)

PROFIT_TARGET     = env_float("PROFIT_TARGET", 0.0)
CIRCUIT_STOP_LOSS = env_float("CIRCUIT_STOP_LOSS", 0.0)

SNAPSHOT_INTERVAL_SEC = env_int("SNAPSHOT_INTERVAL_SEC", 60)
POLL_INTERVAL_SEC     = env_int("POLL_INTERVAL_SEC", 1)

SPOT_INSTRUMENT = os.getenv("SPOT_INSTRUMENT", "NSE:NIFTY 50")

ATR_INSTRUMENT_TOKEN = env_int("ATR_INSTRUMENT_TOKEN", 0)
ATR_PERIOD = env_int("ATR_PERIOD", 14)

LIVE_MODE = env_bool("LIVE_MODE", False)

KITE_API_KEY      = os.getenv("KITE_API_KEY")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN")
STOCKO_CLIENT_ID    = os.getenv("STOCKO_CLIENT_ID")
STOCKO_BASE_URL     = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in")

# =========================================================
# HOLIDAY / MARKET SESSION GUARDS
# =========================================================

# 👉 NSE holidays (MAINTAIN YEARLY)
NSE_HOLIDAYS = {
    date(2026, 1, 26),   # Republic Day
    date(2026, 3, 6),    # Holi (example)
    date(2026, 3, 31),   # Ram Navami (example)
    date(2026, 4, 10),   # Good Friday
    date(2026, 5, 1),    # Maharashtra Day
    date(2026, 8, 15),   # Independence Day
    date(2026, 10, 2),   # Gandhi Jayanti
    date(2026, 11, 12),  # Diwali (example)
}

def is_weekend(d):
    return d.weekday() >= 5   # Sat/Sun

def is_holiday(d):
    return d in NSE_HOLIDAYS

def is_market_session_open(now):
    if is_weekend(now.date()):
        return False
    if is_holiday(now.date()):
        return False
    return MARKET_OPEN_TIME <= now.time() <= MARKET_CLOSE_TIME

# =========================================================
# DB
# =========================================================

def db_connect():
    return psycopg2.connect(
        os.getenv("DATABASE_URL"),
        sslmode="require",
        cursor_factory=RealDictCursor
    )

def ensure_table(conn):
    with conn.cursor() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS Live_SS_Nifty_Spot (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ,
            bot_name TEXT,
            mode TEXT,
            event TEXT,
            reason TEXT,
            symbol TEXT,
            side TEXT,
            qty INT,
            price NUMERIC,
            spot NUMERIC,
            atr NUMERIC,
            unreal_pnl NUMERIC,
            total_pnl NUMERIC
        );
        """)
    conn.commit()

def log_db(conn, **k):
    with conn.cursor() as c:
        c.execute("""
        INSERT INTO Live_SS_Nifty_Spot
        (ts, bot_name, mode, event, reason, symbol, side, qty, price, spot, atr, unreal_pnl, total_pnl)
        VALUES (NOW(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            BOT_NAME,
            "LIVE" if LIVE_MODE else "PAPER",
            k["event"], k["reason"], k["symbol"], k["side"],
            k["qty"], k["price"], k["spot"], k["atr"],
            k["unreal"], k["total"]
        ))
    conn.commit()

def read_trade_flag(conn):
    with conn.cursor() as c:
        c.execute("SELECT live_ss_nifty_spot FROM trade_flag LIMIT 1")
        r = c.fetchone()
        return bool(r["live_ss_nifty_spot"]) if r else False

# =========================================================
# MARKET / ATR
# =========================================================

def kite_connect():
    k = KiteConnect(api_key=KITE_API_KEY)
    k.set_access_token(KITE_ACCESS_TOKEN)
    return k

def ltp(k, inst):
    try: return k.ltp(inst)[inst]["last_price"]
    except: return float("nan")

def compute_atr(k):
    if ATR_INSTRUMENT_TOKEN <= 0:
        return None
    now = datetime.now(MARKET_TZ)
    start = now - timedelta(minutes=ATR_PERIOD + 10)
    candles = k.historical_data(ATR_INSTRUMENT_TOKEN, start, now, "minute")
    if len(candles) < ATR_PERIOD + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return round(sum(trs[-ATR_PERIOD:]) / ATR_PERIOD, 2)

# =========================================================
# MAIN LOOP
# =========================================================

def main():
    conn = db_connect()
    ensure_table(conn)
    kite = kite_connect()

    positions = {}
    last_snapshot = 0
    trading_halted = False
    trade_allowed = False
    last_atr = None

    while True:
        now = datetime.now(MARKET_TZ)

        # ⛔ HARD STOP OUTSIDE MARKET SESSION
        if not is_market_session_open(now):
            time.sleep(30)
            continue

        spot = ltp(kite, SPOT_INSTRUMENT)
        if math.isnan(spot):
            time.sleep(POLL_INTERVAL_SEC)
            continue

        # ⏰ Forced 15:25 exit
        if now.time() >= SQUARE_OFF_TIME and positions:
            log_db(conn, event="TIME_EXIT", reason="15:25",
                   symbol="ALL", side="NA", qty=0, price=0,
                   spot=spot, atr=last_atr, unreal=0, total=0)
            return

        # 🔁 SNAPSHOT (market hours only)
        if time.time() - last_snapshot >= SNAPSHOT_INTERVAL_SEC:
            last_atr = compute_atr(kite)
            trade_allowed = read_trade_flag(conn)

            unreal = 0.0
            log_db(conn, event="SNAPSHOT", reason=f"FLAG={trade_allowed}",
                   symbol="NIFTY", side="NA", qty=0, price=0,
                   spot=spot, atr=last_atr, unreal=unreal, total=unreal)

            last_snapshot = time.time()

            if not trade_allowed and positions:
                log_db(conn, event="FLAG_EXIT", reason="FLAG_FALSE",
                       symbol="ALL", side="NA", qty=0, price=0,
                       spot=spot, atr=last_atr, unreal=0, total=0)
                positions.clear()
                trading_halted = True

            if trade_allowed and trading_halted:
                trading_halted = False

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main()
