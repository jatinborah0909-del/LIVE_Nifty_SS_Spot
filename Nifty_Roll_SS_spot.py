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
"""

import os
import time
import math
from datetime import datetime, timedelta, time as dt_time, date

import pytz
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from kiteconnect import KiteConnect

# =========================================================
# ENV HELPERS
# =========================================================

def env_bool(k: str, d: bool = False) -> bool:
    return os.getenv(k, str(d)).strip().lower() in ("1", "true", "yes", "y", "on")

def env_int(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)).strip())
    except Exception:
        return d

def env_float(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)).strip())
    except Exception:
        return d

def parse_hhmm(key: str, default: str) -> dt_time:
    s = os.getenv(key, default).strip()
    try:
        hh, mm = s.split(":")
        return dt_time(int(hh), int(mm))
    except Exception:
        raise ValueError(f"{key} must be HH:MM (e.g. {default}). Got: {s!r}")

# =========================================================
# CONFIG (ENV)
# =========================================================

BOT_NAME = os.getenv("BOT_NAME", "SS_NIFTY_SPOT")

MARKET_TZ = pytz.timezone(os.getenv("MARKET_TZ", "Asia/Kolkata"))

LIVE_MODE = env_bool("LIVE_MODE", False)

# Strategy params
ENTRY_START_TIME = parse_hhmm("ENTRY_START_TIME", "09:30")
ENTRY_TOL        = env_int("ENTRY_TOL", 4)
STRIKE_STEP      = env_int("STRIKE_STEP", 50)
QTY_PER_LEG      = env_int("QTY_PER_LEG", 65)

# Risk params
PROFIT_TARGET     = env_float("PROFIT_TARGET", 0.0)      # 0 disables
CIRCUIT_STOP_LOSS = env_float("CIRCUIT_STOP_LOSS", 0.0)  # 0 disables

# Time exits / loops
SQUARE_OFF_TIME         = parse_hhmm("SQUARE_OFF_TIME", "15:25")
POLL_INTERVAL_SEC       = env_int("POLL_INTERVAL_SEC", 1)
SNAPSHOT_INTERVAL_SEC   = env_int("SNAPSHOT_INTERVAL_SEC", 60)

# Instruments
SPOT_INSTRUMENT = os.getenv("SPOT_INSTRUMENT", "NSE:NIFTY 50")

# ATR
ATR_INSTRUMENT_TOKEN = env_int("ATR_INSTRUMENT_TOKEN", 0)  # REQUIRED (e.g., NIFTY index token)
ATR_PERIOD           = env_int("ATR_PERIOD", 14)

# Kite creds
KITE_API_KEY      = os.getenv("KITE_API_KEY", "").strip()
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()

# Stocko (LIVE only)
STOCKO_BASE_URL     = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in").strip()
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN", "").strip()
STOCKO_CLIENT_ID    = os.getenv("STOCKO_CLIENT_ID", "").strip()

# =========================================================
# DB
# =========================================================

TABLE_TRADES = "Live_SS_Nifty_Spot"
FLAG_TABLE   = "trade_flag"
FLAG_COL     = "live_ss_nifty_spot"  # as you specified (boolean)

def db_connect():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise ValueError("DATABASE_URL env variable is missing.")
    return psycopg2.connect(url, sslmode="require", cursor_factory=RealDictCursor)

def ensure_table(conn):
    with conn.cursor() as c:
        c.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_TRADES} (
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

def log_db(conn, *, event, reason, symbol, side, qty, price, spot, atr, unreal, total):
    with conn.cursor() as c:
        c.execute(f"""
            INSERT INTO {TABLE_TRADES}
            (ts, bot_name, mode, event, reason, symbol, side, qty, price, spot, atr, unreal_pnl, total_pnl)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            BOT_NAME,
            "LIVE" if LIVE_MODE else "PAPER",
            str(event),
            str(reason),
            str(symbol),
            str(side),
            int(qty),
            float(price) if price is not None else 0.0,
            float(spot) if spot is not None else 0.0,
            float(atr) if atr is not None else None,
            float(unreal) if unreal is not None else 0.0,
            float(total) if total is not None else 0.0
        ))
    conn.commit()

def read_trade_flag(conn) -> bool:
    # Expect exactly 1 row in trade_flag
    with conn.cursor() as c:
        c.execute(f"SELECT {FLAG_COL} FROM {FLAG_TABLE} LIMIT 1")
        r = c.fetchone()
        return bool(r[FLAG_COL]) if r and (FLAG_COL in r) else False

# =========================================================
# KITE / MARKET DATA
# =========================================================

def kite_connect() -> KiteConnect:
    if not KITE_API_KEY or not KITE_ACCESS_TOKEN:
        raise ValueError("KITE_API_KEY or KITE_ACCESS_TOKEN missing in env.")
    k = KiteConnect(api_key=KITE_API_KEY)
    k.set_access_token(KITE_ACCESS_TOKEN)
    return k

def safe_ltp(kite: KiteConnect, instrument: str) -> float:
    try:
        q = kite.ltp(instrument)
        return float(q[instrument]["last_price"])
    except Exception:
        return float("nan")

def compute_atr(kite: KiteConnect) -> float | None:
    """
    ATR(ATR_PERIOD) from 1-minute candles using TR average.
    Always computed every snapshot minute, regardless of trading permission.
    """
    if ATR_INSTRUMENT_TOKEN <= 0:
        return None

    now = datetime.now(MARKET_TZ)
    start = now - timedelta(minutes=ATR_PERIOD + 10)

    try:
        candles = kite.historical_data(ATR_INSTRUMENT_TOKEN, start, now, "minute")
    except Exception:
        return None

    if not candles or len(candles) < ATR_PERIOD + 1:
        return None

    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    atr = sum(trs[-ATR_PERIOD:]) / ATR_PERIOD
    return round(float(atr), 2)

# =========================================================
# INSTRUMENT RESOLUTION (NIFTY CE/PE for nearest expiry)
# =========================================================

def pick_nearest_expiry_nifty_options(kite: KiteConnect):
    """
    Fetch NFO instruments once and return a list filtered for NIFTY options.
    This is heavy; do once at startup.
    """
    inst = kite.instruments("NFO")
    # Filter NIFTY options only
    nifty_opts = [
        r for r in inst
        if r.get("segment") == "NFO-OPT"
        and r.get("name") == "NIFTY"
        and r.get("instrument_type") in ("CE", "PE")
    ]
    if not nifty_opts:
        raise RuntimeError("Could not find NIFTY options in NFO instruments.")
    return nifty_opts

def resolve_atm_ce_pe(nifty_opts, atm_strike: int, today: date):
    """
    Choose nearest expiry >= today, and find CE+PE at that strike.
    Returns (ce_row, pe_row).
    """
    expiries = sorted({r["expiry"] for r in nifty_opts if r["expiry"] >= today})
    if not expiries:
        raise RuntimeError("No upcoming NIFTY option expiries found.")

    expiry = expiries[0]  # nearest expiry

    ce = next((r for r in nifty_opts if r["expiry"] == expiry and r["strike"] == atm_strike and r["instrument_type"] == "CE"), None)
    pe = next((r for r in nifty_opts if r["expiry"] == expiry and r["strike"] == atm_strike and r["instrument_type"] == "PE"), None)

    if not ce or not pe:
        raise RuntimeError(f"Could not resolve CE/PE for strike={atm_strike} expiry={expiry}.")

    return ce, pe, expiry

# =========================================================
# STOCKO ORDER PLACEMENT (LIVE) / PAPER SIMULATION
# =========================================================

def stocko_place(symbol: str, side: str, qty: int) -> str:
    """
    Live mode: place order via Stocko.
    Paper mode: return empty string (no crash if creds missing).
    """
    if not LIVE_MODE:
        return ""

    if not STOCKO_ACCESS_TOKEN or not STOCKO_CLIENT_ID:
        raise RuntimeError("LIVE_MODE=True but STOCKO_ACCESS_TOKEN / STOCKO_CLIENT_ID missing.")

    url = f"{STOCKO_BASE_URL}/orders"
    headers = {
        "Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}",
        "X-Client-Id": STOCKO_CLIENT_ID,
        "Content-Type": "application/json"
    }
    payload = {"symbol": symbol, "side": side, "quantity": qty, "order_type": "MARKET"}
    r = requests.post(url, headers=headers, json=payload, timeout=15)
    r.raise_for_status()
    data = r.json()
    return str(data.get("order_id", ""))

# =========================================================
# POSITION & PNL
# =========================================================

def compute_unreal_m2m(kite: KiteConnect, positions: dict) -> float:
    """
    positions: dict with keys "CE","PE" each has:
      - instrument (e.g. "NFO:NIFTY24JAN25000CE")
      - side ("SELL")
      - qty
      - entry_price
    For SELL: pnl = (entry - ltp) * qty
    """
    unreal = 0.0
    for leg in positions.values():
        inst = leg["instrument"]
        ltp = safe_ltp(kite, inst)
        if math.isnan(ltp):
            continue
        entry = float(leg["entry_price"])
        qty = int(leg["qty"])
        side = leg["side"].upper()
        if side == "SELL":
            unreal += (entry - ltp) * qty
        else:
            unreal += (ltp - entry) * qty
    return float(unreal)

def square_off_all(conn, kite: KiteConnect, positions: dict, *, spot: float, atr: float | None, reason: str, event: str = "EXIT_ALL"):
    """
    Exit both legs (if present) and log each transaction + one summary row.
    """
    if not positions:
        return

    # Exit each leg
    for leg_name in list(positions.keys()):
        leg = positions[leg_name]
        inst = leg["instrument"]
        tradingsymbol = leg["tradingsymbol"]
        qty = int(leg["qty"])

        # For short straddle SELL entry, exit is BUY
        exit_side = "BUY" if leg["side"].upper() == "SELL" else "SELL"

        exit_price = safe_ltp(kite, inst)
        if math.isnan(exit_price):
            exit_price = 0.0

        if LIVE_MODE:
            stocko_place(tradingsymbol, exit_side, qty)

        # Log exit transaction
        unreal = compute_unreal_m2m(kite, positions)
        total = unreal
        log_db(
            conn,
            event="EXIT",
            reason=reason,
            symbol=inst,
            side=exit_side,
            qty=qty,
            price=exit_price,
            spot=spot,
            atr=atr,
            unreal=unreal,
            total=total
        )

        positions.pop(leg_name, None)

    # Summary row
    log_db(
        conn,
        event=event,
        reason=reason,
        symbol="ALL",
        side="NA",
        qty=0,
        price=0,
        spot=spot,
        atr=atr,
        unreal=0.0,
        total=0.0
    )

# =========================================================
# STRATEGY: ATM SHORT STRADDLE ENTRY LOGIC
# =========================================================

def strategy_signal(now_local: dt_time, spot: float, positions: dict) -> dict:
    """
    Returns:
      { action: "ENTER"|"HOLD", reason: str }
    """
    # Entry only after configured time
    if now_local < ENTRY_START_TIME:
        return {"action": "HOLD", "reason": f"BEFORE_ENTRY_START_{ENTRY_START_TIME.strftime('%H:%M')}"}

    # Only one position at a time
    if positions:
        return {"action": "HOLD", "reason": "POSITION_OPEN"}

    # ATM strike rounding
    atm = int(round(spot / STRIKE_STEP) * STRIKE_STEP)

    # Entry threshold check (± ENTRY_TOL)
    if abs(spot - atm) <= ENTRY_TOL:
        return {"action": "ENTER", "reason": f"ATM_WITHIN_{ENTRY_TOL}_STRIKE_{atm}"}

    return {"action": "HOLD", "reason": "NO_ENTRY_THRESHOLD"}

# =========================================================
# MAIN LOOP
# =========================================================

def main():
    print(f"[START] {BOT_NAME} | LIVE_MODE={LIVE_MODE}")
    print(f"[CFG] ENTRY_START_TIME={ENTRY_START_TIME.strftime('%H:%M')} ENTRY_TOL={ENTRY_TOL} STRIKE_STEP={STRIKE_STEP} QTY={QTY_PER_LEG}")
    print(f"[CFG] PROFIT_TARGET={PROFIT_TARGET} CIRCUIT_STOP_LOSS={CIRCUIT_STOP_LOSS} SQUARE_OFF_TIME={SQUARE_OFF_TIME.strftime('%H:%M')}")
    print(f"[CFG] SNAPSHOT_INTERVAL_SEC={SNAPSHOT_INTERVAL_SEC} SPOT_INSTRUMENT={SPOT_INSTRUMENT}")

    conn = db_connect()
    ensure_table(conn)

    kite = kite_connect()

    # Load instruments once
    nifty_opts = pick_nearest_expiry_nifty_options(kite)

    positions = {}          # {"CE": {...}, "PE": {...}}
    trading_halted = False  # set True after flag exit until flag turns True again
    trade_allowed = False

    last_snapshot_ts = 0.0
    last_atr = None

    while True:
        now = datetime.now(MARKET_TZ)
        now_time = now.time()

        # Spot
        spot = safe_ltp(kite, SPOT_INSTRUMENT)
        if math.isnan(spot):
            time.sleep(POLL_INTERVAL_SEC)
            continue

        # Forced square-off time check (exit and stop)
        if now_time >= SQUARE_OFF_TIME:
            # log and square off if open
            if positions:
                square_off_all(conn, kite, positions, spot=spot, atr=last_atr, reason="TIME_1525", event="TIME_EXIT")
            else:
                log_db(conn, event="TIME_EXIT", reason="TIME_1525_NO_POS", symbol="ALL", side="NA",
                       qty=0, price=0, spot=spot, atr=last_atr, unreal=0.0, total=0.0)
            print("[STOP] Time-based exit hit.")
            return

        # Compute unreal m2m for checks/logging
        unreal = compute_unreal_m2m(kite, positions) if positions else 0.0
        total = unreal  # (you can add realized later if needed)

        # Snapshot + ATR + flag check every minute (ATR ALWAYS computed, independent of trading)
        if time.time() - last_snapshot_ts >= SNAPSHOT_INTERVAL_SEC:
            last_atr = compute_atr(kite)
            trade_allowed = read_trade_flag(conn)

            # Always log snapshot (even when kill-switch is false)
            unreal = compute_unreal_m2m(kite, positions) if positions else 0.0
            total = unreal
            log_db(conn, event="SNAPSHOT", reason=f"FLAG={trade_allowed}", symbol="NIFTY", side="NA",
                   qty=0, price=0, spot=spot, atr=last_atr, unreal=unreal, total=total)

            last_snapshot_ts = time.time()

            # Kill switch enforcement (after snapshot)
            if (not trade_allowed) and positions:
                square_off_all(conn, kite, positions, spot=spot, atr=last_atr, reason="FLAG_FALSE", event="FLAG_EXIT")
                trading_halted = True

            # Resume if flag becomes true and no positions
            if trade_allowed and trading_halted and not positions:
                trading_halted = False
                log_db(conn, event="RESUME", reason="FLAG_TRUE_RESUME", symbol="ALL", side="NA",
                       qty=0, price=0, spot=spot, atr=last_atr, unreal=0.0, total=0.0)

        # If trading halted, do nothing except keep loop alive (snapshots still happen above)
        if trading_halted:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        # Risk checks (only meaningful if positions exist)
        if positions:
            # update m2m right before checks
            unreal = compute_unreal_m2m(kite, positions)
            total = unreal

            if CIRCUIT_STOP_LOSS > 0 and total <= -abs(CIRCUIT_STOP_LOSS):
                square_off_all(conn, kite, positions, spot=spot, atr=last_atr, reason="CIRCUIT_SL", event="CIRCUIT_SL")
                trading_halted = True
                continue

            if PROFIT_TARGET > 0 and total >= PROFIT_TARGET:
                square_off_all(conn, kite, positions, spot=spot, atr=last_atr, reason="PROFIT_TARGET", event="PROFIT_EXIT")
                trading_halted = True
                continue

        # Entry logic (only if kill-switch is true)
        if trade_allowed and (not positions):
            sig = strategy_signal(now_time, spot, positions)
            if sig["action"] == "ENTER":
                atm = int(round(spot / STRIKE_STEP) * STRIKE_STEP)

                # Resolve CE/PE symbols for nearest expiry at ATM strike
                ce_row, pe_row, expiry = resolve_atm_ce_pe(nifty_opts, atm, today=now.date())

                ce_ts = ce_row["tradingsymbol"]
                pe_ts = pe_row["tradingsymbol"]

                ce_inst = f"NFO:{ce_ts}"
                pe_inst = f"NFO:{pe_ts}"

                # Prices
                ce_price = safe_ltp(kite, ce_inst)
                pe_price = safe_ltp(kite, pe_inst)
                if math.isnan(ce_price) or math.isnan(pe_price):
                    # can't price, skip entry
                    time.sleep(POLL_INTERVAL_SEC)
                    continue

                # Place orders (SELL short straddle)
                if LIVE_MODE:
                    stocko_place(ce_ts, "SELL", QTY_PER_LEG)
                    stocko_place(pe_ts, "SELL", QTY_PER_LEG)

                # Store positions in memory
                positions["CE"] = {
                    "instrument": ce_inst,
                    "tradingsymbol": ce_ts,
                    "side": "SELL",
                    "qty": QTY_PER_LEG,
                    "entry_price": float(ce_price),
                    "strike": atm,
                    "expiry": str(expiry)
                }
                positions["PE"] = {
                    "instrument": pe_inst,
                    "tradingsymbol": pe_ts,
                    "side": "SELL",
                    "qty": QTY_PER_LEG,
                    "entry_price": float(pe_price),
                    "strike": atm,
                    "expiry": str(expiry)
                }

                # Log each leg entry transaction
                unreal = compute_unreal_m2m(kite, positions)
                total = unreal
                log_db(conn, event="ENTRY", reason=sig["reason"], symbol=ce_inst, side="SELL",
                       qty=QTY_PER_LEG, price=ce_price, spot=spot, atr=last_atr, unreal=unreal, total=total)
                log_db(conn, event="ENTRY", reason=sig["reason"], symbol=pe_inst, side="SELL",
                       qty=QTY_PER_LEG, price=pe_price, spot=spot, atr=last_atr, unreal=unreal, total=total)

                # Optional summary row
                log_db(conn, event="ENTRY_ALL", reason=f"{sig['reason']}_EXP_{expiry}", symbol="ALL", side="NA",
                       qty=0, price=0, spot=spot, atr=last_atr, unreal=unreal, total=total)

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main()
