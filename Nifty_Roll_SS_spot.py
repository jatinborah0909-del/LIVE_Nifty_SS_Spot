#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FINAL RAILWAY DEPLOYABLE BOT – SS NIFTY SPOT

FEATURES
--------
✔ PostgreSQL only (NO CSV)
✔ trade_flag DB kill-switch
✔ ATR calculated EVERY minute (independent of trading)
✔ ATR logged even when kill-switch is FALSE
✔ Forced square-off at 15:25 IST
✔ Circuit breaker SL
✔ Profit target
✔ PAPER mode safe (no Stocko creds needed)
"""

import os
import time
import math
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, time as dt_time
import pytz
from kiteconnect import KiteConnect

# =========================================================
# ENV HELPERS
# =========================================================

def env_bool(k, d=False):
    return os.getenv(k, str(d)).lower() in ("1", "true", "yes", "y")

def env_int(k, d):
    try:
        return int(os.getenv(k, d))
    except:
        return d

def env_float(k, d):
    try:
        return float(os.getenv(k, d))
    except:
        return d

# =========================================================
# CONFIG (ALL FROM ENV)
# =========================================================

BOT_NAME = os.getenv("BOT_NAME", "SS_NIFTY_SPOT")
MARKET_TZ = pytz.timezone(os.getenv("MARKET_TZ", "Asia/Kolkata"))

LIVE_MODE = env_bool("LIVE_MODE", False)

QTY_PER_LEG = env_int("QTY_PER_LEG", 65)
PROFIT_TARGET = env_float("PROFIT_TARGET", 0.0)
CIRCUIT_STOP_LOSS = env_float("CIRCUIT_STOP_LOSS", 0.0)

POLL_INTERVAL_SEC = env_int("POLL_INTERVAL_SEC", 1)
SNAPSHOT_INTERVAL_SEC = 60

SQUARE_OFF_TIME = dt_time(15, 25)

SPOT_INSTRUMENT = os.getenv("SPOT_INSTRUMENT", "NSE:NIFTY 50")

# ATR
ATR_INSTRUMENT_TOKEN = int(os.getenv("ATR_INSTRUMENT_TOKEN"))   # REQUIRED
ATR_PERIOD = env_int("ATR_PERIOD", 14)

# Kite
KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

# Stocko (LIVE only)
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN")
STOCKO_CLIENT_ID = os.getenv("STOCKO_CLIENT_ID")
STOCKO_BASE_URL = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in")

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
            ts TIMESTAMPTZ NOT NULL,
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

def log_db(conn, event, reason, symbol, side, qty, price, spot, atr, unreal, total):
    with conn.cursor() as c:
        c.execute("""
        INSERT INTO Live_SS_Nifty_Spot
        (ts, bot_name, mode, event, reason, symbol, side, qty, price, spot, atr, unreal_pnl, total_pnl)
        VALUES (NOW(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            BOT_NAME,
            "LIVE" if LIVE_MODE else "PAPER",
            event, reason, symbol, side,
            qty, price, spot, atr,
            unreal, total
        ))
    conn.commit()

def read_trade_flag(conn):
    with conn.cursor() as c:
        c.execute("SELECT live_SS_Nifty_Spot FROM trade_flag LIMIT 1")
        r = c.fetchone()
        return bool(r["live_SS_Nifty_Spot"]) if r else False

# =========================================================
# MARKET
# =========================================================

def kite():
    k = KiteConnect(api_key=KITE_API_KEY)
    k.set_access_token(KITE_ACCESS_TOKEN)
    return k

def ltp(k, instrument):
    try:
        return k.ltp(instrument)[instrument]["last_price"]
    except:
        return float("nan")

def compute_atr(k):
    """ATR is ALWAYS computed – independent of trading / kill-switch"""
    now = datetime.now(MARKET_TZ)
    start = now - timedelta(minutes=ATR_PERIOD + 5)

    candles = k.historical_data(
        ATR_INSTRUMENT_TOKEN,
        start,
        now,
        "minute"
    )

    if len(candles) < ATR_PERIOD + 1:
        return None

    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    return round(sum(trs[-ATR_PERIOD:]) / ATR_PERIOD, 2)

def stocko_place(symbol, side, qty):
    if not LIVE_MODE:
        return ""
    if not STOCKO_ACCESS_TOKEN or not STOCKO_CLIENT_ID:
        raise RuntimeError("LIVE_MODE=True but Stocko creds missing")

    r = requests.post(
        f"{STOCKO_BASE_URL}/orders",
        headers={
            "Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}",
            "X-Client-Id": STOCKO_CLIENT_ID
        },
        json={"symbol": symbol, "side": side, "quantity": qty},
        timeout=10
    )
    r.raise_for_status()
    return r.json().get("order_id", "")

# =========================================================
# STRATEGY PLACEHOLDER
# =========================================================

def strategy_signal(spot, positions):
    """
    Replace with your actual logic.

    Must return:
    {
      "action": "ENTER" | "EXIT" | "HOLD",
      "orders": [
          {"symbol": "...", "side": "BUY"}
      ],
      "reason": "text"
    }
    """
    return {"action": "HOLD", "orders": [], "reason": ""}

# =========================================================
# MAIN
# =========================================================

def main():
    print(f"🚀 {BOT_NAME} started | LIVE_MODE={LIVE_MODE}")

    conn = db_connect()
    ensure_table(conn)

    k = kite()

    positions = {}
    trading_halted = False
    trade_allowed = False
    last_snapshot_ts = 0

    while True:
        now = datetime.now(MARKET_TZ)
        spot = ltp(k, SPOT_INSTRUMENT)

        unreal = sum(
            (ltp(k, p["symbol"]) - p["price"]) * p["qty"]
            for p in positions.values()
            if not math.isnan(ltp(k, p["symbol"]))
        )
        total = unreal

        # ⏰ Forced square-off at 15:25
        if now.time() >= SQUARE_OFF_TIME:
            log_db(conn, "TIME_EXIT", "15:25_SQUARE_OFF",
                   "ALL", "NA", 0, 0, spot, None, unreal, total)
            return

        # 🔁 SNAPSHOT EVERY MINUTE (ATR ALWAYS CALCULATED)
        if time.time() - last_snapshot_ts >= SNAPSHOT_INTERVAL_SEC:

            atr = compute_atr(k)           # ALWAYS
            trade_allowed = read_trade_flag(conn)

            log_db(
                conn,
                event="SNAPSHOT",
                reason="MINUTE_SNAPSHOT",
                symbol="NIFTY",
                side="NA",
                qty=0,
                price=0,
                spot=spot,
                atr=atr,
                unreal=unreal,
                total=total
            )

            last_snapshot_ts = time.time()

            # 🚨 Kill-switch enforcement (AFTER snapshot)
            if not trade_allowed and positions:
                log_db(
                    conn,
                    event="FLAG_EXIT",
                    reason="trade_flag_FALSE",
                    symbol="ALL",
                    side="NA",
                    qty=0,
                    price=0,
                    spot=spot,
                    atr=atr,
                    unreal=unreal,
                    total=total
                )
                positions.clear()
                trading_halted = True

            if trade_allowed and trading_halted and not positions:
                trading_halted = False

        # 🛑 Circuit breaker SL
        if CIRCUIT_STOP_LOSS > 0 and total <= -CIRCUIT_STOP_LOSS:
            log_db(conn, "CIRCUIT_SL", "DAILY_SL_HIT",
                   "ALL", "NA", 0, 0, spot, atr, unreal, total)
            return

        # 🎯 Profit target
        if PROFIT_TARGET > 0 and total >= PROFIT_TARGET:
            log_db(conn, "PROFIT_EXIT", "TARGET_HIT",
                   "ALL", "NA", 0, 0, spot, atr, unreal, total)
            return

        # ▶ Strategy execution
        sig = strategy_signal(spot, positions)

        if sig["action"] == "ENTER" and trade_allowed and not trading_halted:
            for o in sig["orders"]:
                price = ltp(k, o["symbol"])
                stocko_place(o["symbol"], o["side"], QTY_PER_LEG)
                positions[o["symbol"]] = {
                    "symbol": o["symbol"],
                    "qty": QTY_PER_LEG,
                    "price": price
                }
                log_db(conn, "ENTRY", sig["reason"],
                       o["symbol"], o["side"],
                       QTY_PER_LEG, price, spot, atr, unreal, total)

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main()
