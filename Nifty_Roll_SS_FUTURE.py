#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SS NIFTY FUT – ATM SHORT STRADDLE (Railway Deployable) – FUT ONLY
----------------------------------------------------------------
✅ FUT-based underlying for EVERYTHING (ATM, entry logic, logging reference price)
✅ FUT-based ATR (built live from NIFTY FUT minute candle builder)
✅ Options chosen from next immediate expiry (nearest expiry >= today)
✅ Kill-switch every minute from trade_flag.<FLAG_COL>
✅ Auto-creates trade_flag table + FLAG_COL + ensures exactly ONE row (id=1)
✅ M2M snapshots every minute to DB with CE/PE entry/ltp/exit prices
✅ Entry/Exit transactions stored in DB too
✅ Forced square-off at 15:25
✅ NO API calls + NO DB writes outside market session (before 09:15 / after 15:30), weekends, holidays
✅ PAPER mode won’t fail without Stocko credentials

DB table (default): live_ss_nifty_fut
Kill switch table: trade_flag, column: live_ss_nifty_fut

IMPORTANT:
- DB column "spot" is retained for compatibility but now stores FUT underlying price.
- CIRCUIT_STOP_LOSS uses magnitude. For -4000 loss circuit, set CIRCUIT_STOP_LOSS=4000
"""

import os
import re
import time
import math
from datetime import datetime, time as dt_time, date
import pytz
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from kiteconnect import KiteConnect

# =========================================================
# ENV HELPERS
# =========================================================

def env_bool(k, d=False):
    return os.getenv(k, str(d)).strip().lower() in ("1", "true", "yes", "y", "on")

def env_int(k, d):
    try:
        return int(os.getenv(k, str(d)).strip())
    except Exception:
        return d

def env_float(k, d):
    try:
        return float(os.getenv(k, str(d)).strip())
    except Exception:
        return d

def parse_hhmm(key, default):
    s = os.getenv(key, default).strip()
    try:
        hh, mm = s.split(":")
        return dt_time(int(hh), int(mm))
    except Exception:
        raise ValueError(f"{key} must be HH:MM (e.g. {default}). Got: {s!r}")

def parse_dates_csv(key, default=""):
    """
    NSE_HOLIDAYS env example:
      NSE_HOLIDAYS=2026-01-26,2026-03-06,2026-04-10
    """
    s = os.getenv(key, default).strip()
    out = set()
    if not s:
        return out
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            y, m, d = part.split("-")
            out.add(date(int(y), int(m), int(d)))
        except Exception:
            raise ValueError(f"{key} must be comma-separated YYYY-MM-DD. Bad value: {part!r}")
    return out

def safe_ident(name: str, fallback: str) -> str:
    """Allow only a-zA-Z0-9_ for table/column env vars."""
    name = (name or "").strip()
    if not name:
        return fallback
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        return fallback
    return name

# =========================================================
# CONFIG (ENV)
# =========================================================

BOT_NAME   = os.getenv("BOT_NAME", "SS_NIFTY_FUT").strip()
MARKET_TZ  = pytz.timezone(os.getenv("MARKET_TZ", "Asia/Kolkata").strip())

MARKET_OPEN_TIME  = parse_hhmm("MARKET_OPEN_TIME",  "09:15")
MARKET_CLOSE_TIME = parse_hhmm("MARKET_CLOSE_TIME", "15:30")
ENTRY_START_TIME  = parse_hhmm("ENTRY_START_TIME",  "09:30")
SQUARE_OFF_TIME   = parse_hhmm("SQUARE_OFF_TIME",   "15:25")

STRIKE_STEP = env_int("STRIKE_STEP", 50)
ENTRY_TOL   = env_int("ENTRY_TOL", 25)
QTY_PER_LEG = env_int("QTY_PER_LEG", 65)

PROFIT_TARGET     = env_float("PROFIT_TARGET", 0.0)      # 0 disables
CIRCUIT_STOP_LOSS = env_float("CIRCUIT_STOP_LOSS", 0.0)  # 0 disables; set 4000 for -4000

POLL_INTERVAL_SEC     = env_int("POLL_INTERVAL_SEC", 1)
SNAPSHOT_INTERVAL_SEC = env_int("SNAPSHOT_INTERVAL_SEC", 60)

ATR_PERIOD = env_int("ATR_PERIOD", 14)
NSE_HOLIDAYS = parse_dates_csv("NSE_HOLIDAYS", default="")

# Optional override: if you want to hardcode a FUT tradingsymbol (rarely needed)
FUT_TRADINGSYMBOL_OVERRIDE = os.getenv("FUT_TRADINGSYMBOL", "").strip()  # e.g., "NIFTY26JANFUT"

TABLE_NAME = safe_ident(os.getenv("TABLE_NAME", "live_ss_nifty_fut"), "live_ss_nifty_fut")
FLAG_TABLE = safe_ident(os.getenv("FLAG_TABLE", "trade_flag"), "trade_flag")
FLAG_COL   = safe_ident(os.getenv("FLAG_COL", "live_ss_nifty_fut"), "live_ss_nifty_fut")

LIVE_MODE = env_bool("LIVE_MODE", False)

KITE_API_KEY      = os.getenv("KITE_API_KEY", "").strip()
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()

STOCKO_BASE_URL     = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in").strip()
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN", "").strip()
STOCKO_CLIENT_ID    = os.getenv("STOCKO_CLIENT_ID", "").strip()

RECHECK_FLAG_ON_ENTRY = env_bool("RECHECK_FLAG_ON_ENTRY", False)

# =========================================================
# DB
# =========================================================

def db_connect():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL env var missing.")
    return psycopg2.connect(url, sslmode="require", cursor_factory=RealDictCursor)

def ensure_table(conn):
    """Create strategy table and ensure new columns exist (idempotent)."""
    with conn.cursor() as c:
        c.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {t} (
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

                -- For backward compatibility, we keep "spot" but store FUT underlying price here
                spot NUMERIC,

                atr NUMERIC,
                unreal_pnl NUMERIC,
                total_pnl NUMERIC,
                ce_entry_price NUMERIC,
                pe_entry_price NUMERIC,
                ce_ltp NUMERIC,
                pe_ltp NUMERIC,
                ce_exit_price NUMERIC,
                pe_exit_price NUMERIC
            );
        """).format(t=sql.Identifier(TABLE_NAME)))

        for col, coltype in [
            ("ce_entry_price", "NUMERIC"),
            ("pe_entry_price", "NUMERIC"),
            ("ce_ltp", "NUMERIC"),
            ("pe_ltp", "NUMERIC"),
            ("ce_exit_price", "NUMERIC"),
            ("pe_exit_price", "NUMERIC"),
        ]:
            c.execute(
                sql.SQL("ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {c} " + coltype).format(
                    t=sql.Identifier(TABLE_NAME),
                    c=sql.Identifier(col)
                )
            )
    conn.commit()

def ensure_trade_flag(conn):
    """
    Ensures:
    - FLAG_TABLE exists
    - FLAG_COL exists
    - exactly one row exists (id=1)
    - default value = TRUE
    """
    with conn.cursor() as c:
        # Table exists
        c.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {tbl} (
                id INT PRIMARY KEY
            )
        """).format(tbl=sql.Identifier(FLAG_TABLE)))

        # Column exists
        c.execute(sql.SQL("""
            ALTER TABLE {tbl}
            ADD COLUMN IF NOT EXISTS {col} BOOLEAN DEFAULT TRUE
        """).format(
            tbl=sql.Identifier(FLAG_TABLE),
            col=sql.Identifier(FLAG_COL)
        ))

        # Ensure one row
        c.execute(sql.SQL("SELECT COUNT(*) AS cnt FROM {tbl}").format(
            tbl=sql.Identifier(FLAG_TABLE)
        ))
        cnt = int(c.fetchone()["cnt"])

        if cnt == 0:
            c.execute(sql.SQL("""
                INSERT INTO {tbl} (id, {col})
                VALUES (1, TRUE)
            """).format(
                tbl=sql.Identifier(FLAG_TABLE),
                col=sql.Identifier(FLAG_COL)
            ))
        elif cnt > 1:
            c.execute(sql.SQL("""
                DELETE FROM {tbl} WHERE id <> 1
            """).format(tbl=sql.Identifier(FLAG_TABLE)))

        # If id=1 exists but NULL, set TRUE
        c.execute(sql.SQL("""
            UPDATE {tbl}
            SET {col} = COALESCE({col}, TRUE)
            WHERE id = 1
        """).format(
            tbl=sql.Identifier(FLAG_TABLE),
            col=sql.Identifier(FLAG_COL)
        ))

    conn.commit()

def log_db(
    conn,
    *,
    event,
    reason,
    symbol,
    side,
    qty,
    price,
    underlying_fut,
    atr,
    unreal,
    total,
    ce_entry=None,
    pe_entry=None,
    ce_ltp=None,
    pe_ltp=None,
    ce_exit=None,
    pe_exit=None,
):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
            INSERT INTO {t}
            (
              ts, bot_name, mode, event, reason,
              symbol, side, qty, price,
              spot, atr, unreal_pnl, total_pnl,
              ce_entry_price, pe_entry_price,
              ce_ltp, pe_ltp,
              ce_exit_price, pe_exit_price
            )
            VALUES
            (NOW(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """).format(t=sql.Identifier(TABLE_NAME)), (
            BOT_NAME,
            "LIVE" if LIVE_MODE else "PAPER",
            str(event),
            str(reason),
            str(symbol),
            str(side),
            int(qty),
            float(price) if price is not None else 0.0,
            float(underlying_fut) if underlying_fut is not None else 0.0,
            float(atr) if atr is not None else None,
            float(unreal) if unreal is not None else 0.0,
            float(total) if total is not None else 0.0,
            float(ce_entry) if ce_entry is not None else None,
            float(pe_entry) if pe_entry is not None else None,
            float(ce_ltp) if ce_ltp is not None else None,
            float(pe_ltp) if pe_ltp is not None else None,
            float(ce_exit) if ce_exit is not None else None,
            float(pe_exit) if pe_exit is not None else None,
        ))
    conn.commit()

def read_trade_flag(conn) -> bool:
    """
    Returns flag value.
    If anything unexpected happens, returns False (safe).
    """
    try:
        with conn.cursor() as c:
            c.execute(sql.SQL("SELECT {col} FROM {tbl} WHERE id=1 LIMIT 1").format(
                col=sql.Identifier(FLAG_COL),
                tbl=sql.Identifier(FLAG_TABLE),
            ))
            r = c.fetchone()
            if not r:
                return False
            return bool(r.get(FLAG_COL))
    except Exception:
        return False

# =========================================================
# KITE / MARKET
# =========================================================

def kite_connect() -> KiteConnect:
    if not KITE_API_KEY or not KITE_ACCESS_TOKEN:
        raise RuntimeError("KITE_API_KEY / KITE_ACCESS_TOKEN missing.")
    k = KiteConnect(api_key=KITE_API_KEY)
    k.set_access_token(KITE_ACCESS_TOKEN)
    return k

def safe_ltp_many(kite: KiteConnect, instruments: list[str]) -> dict[str, float]:
    out = {i: float("nan") for i in instruments}
    try:
        q = kite.ltp(instruments)
        for i in instruments:
            if i in q and "last_price" in q[i]:
                out[i] = float(q[i]["last_price"])
    except Exception:
        pass
    return out

# =========================================================
# STOCKO (LIVE)
# =========================================================

def _stocko_headers():
    return {"Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}", "Content-Type": "application/json"}

def stocko_search_token(keyword: str) -> int:
    url = f"{STOCKO_BASE_URL}/api/v1/search"
    r = requests.get(url, params={"key": keyword}, headers=_stocko_headers(), timeout=10)
    r.raise_for_status()
    data = r.json()
    result = data.get("result") or data.get("data", {}).get("result", [])
    for rec in result:
        if rec.get("exchange") == "NFO":
            return int(rec["token"])
    raise RuntimeError(f"Stocko search: no NFO token found for {keyword}")

def _gen_user_order_id(offset=0) -> str:
    base = int(time.time() * 1000) + int(offset)
    return str(base)[-15:]

def stocko_place_order_token(token: int, side: str, qty: int, offset=0):
    url = f"{STOCKO_BASE_URL}/api/v1/orders"
    payload = {
        "exchange": "NFO",
        "order_type": "MARKET",
        "instrument_token": int(token),
        "quantity": int(qty),
        "disclosed_quantity": 0,
        "order_side": side.upper(),
        "price": 0,
        "trigger_price": 0,
        "validity": "DAY",
        "product": "NRML",
        "client_id": STOCKO_CLIENT_ID,
        "user_order_id": _gen_user_order_id(offset),
        "market_protection_percentage": 0,
        "device": "WEB",
    }
    r = requests.post(url, json=payload, headers=_stocko_headers(), timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"Stocko order failed: {r.status_code} {r.text}")
    return r.json()

def stocko_place_by_tradingsymbol(tradingsymbol: str, side: str, qty: int, offset=0):
    if not LIVE_MODE:
        return {"simulated": True}
    if not STOCKO_ACCESS_TOKEN or not STOCKO_CLIENT_ID:
        raise RuntimeError("LIVE_MODE=True but STOCKO creds missing.")
    tok = stocko_search_token(tradingsymbol)
    return stocko_place_order_token(tok, side, qty, offset=offset)

# =========================================================
# SESSION GUARDS
# =========================================================

def is_weekend(d: date) -> bool:
    return d.weekday() >= 5

def is_holiday(d: date) -> bool:
    return d in NSE_HOLIDAYS

def market_session_state(now: datetime) -> tuple[bool, str]:
    d = now.date()
    t = now.time()

    reasons = []
    if is_weekend(d):
        reasons.append("WEEKEND")
    if is_holiday(d):
        reasons.append("HOLIDAY")
    if t < MARKET_OPEN_TIME:
        reasons.append("BEFORE_OPEN")
    if t > MARKET_CLOSE_TIME:
        reasons.append("AFTER_CLOSE")

    if reasons:
        return False, ",".join(reasons)
    return True, "OPEN"

# =========================================================
# INSTRUMENT RESOLUTION (Options + FUT)
# =========================================================

def load_instruments_once(kite: KiteConnect) -> pd.DataFrame:
    nfo = pd.DataFrame(kite.instruments("NFO"))
    nfo = nfo[(nfo["name"] == "NIFTY") & (nfo["segment"].isin(["NFO-OPT", "NFO-FUT"]))].copy()
    return nfo

def get_nearest_nifty_fut_symbol(nfo_df: pd.DataFrame) -> str:
    fut = nfo_df[nfo_df["instrument_type"] == "FUT"].copy()
    if fut.empty:
        raise RuntimeError("No NIFTY FUT found in instruments.")
    fut["expiry"] = pd.to_datetime(fut["expiry"])
    fut = fut.sort_values("expiry").iloc[0]   # nearest expiry contract
    return str(fut["tradingsymbol"])

def resolve_atm_ce_pe(nfo_df: pd.DataFrame, atm: int, today: date) -> tuple[str, str, str]:
    """
    Picks NEXT IMMEDIATE option expiry: nearest expiry date >= today.
    """
    opt = nfo_df[nfo_df["instrument_type"].isin(["CE", "PE"])].copy()
    if opt.empty:
        raise RuntimeError("No NIFTY options found in instruments.")
    opt["expiry"] = pd.to_datetime(opt["expiry"]).dt.date

    expiries = sorted({e for e in opt["expiry"].unique() if e >= today})
    if not expiries:
        raise RuntimeError("No upcoming option expiries found.")
    expiry = expiries[0]  # next immediate expiry

    ce = opt[(opt["expiry"] == expiry) & (opt["instrument_type"] == "CE") & (opt["strike"].astype(int) == int(atm))]
    pe = opt[(opt["expiry"] == expiry) & (opt["instrument_type"] == "PE") & (opt["strike"].astype(int) == int(atm))]

    if ce.empty or pe.empty:
        raise RuntimeError(f"Could not resolve CE/PE for strike={atm} expiry={expiry}")

    return str(ce.iloc[0]["tradingsymbol"]), str(pe.iloc[0]["tradingsymbol"]), str(expiry)

# =========================================================
# ATR BUILDER (FUT minute candle builder)
# =========================================================

class FutAtrBuilder:
    def __init__(self, atr_period: int):
        self.atr_period = atr_period
        self.tr_history = []

        self.last_minute_key = None
        self.minute_high = None
        self.minute_low = None
        self.minute_close = None

        self.prev_close = None
        self.atr_val = None

    def update(self, now: datetime, fut_ltp: float) -> float | None:
        if fut_ltp is None or math.isnan(fut_ltp):
            return self.atr_val

        minute_key = now.replace(second=0, microsecond=0)

        if self.last_minute_key is None:
            self.last_minute_key = minute_key
            self.minute_high = fut_ltp
            self.minute_low = fut_ltp
            self.minute_close = fut_ltp
            self.prev_close = fut_ltp
            return self.atr_val

        if minute_key == self.last_minute_key:
            self.minute_high = max(self.minute_high, fut_ltp)
            self.minute_low = min(self.minute_low, fut_ltp)
            self.minute_close = fut_ltp
            return self.atr_val

        tr = max(
            self.minute_high - self.minute_low,
            abs(self.minute_high - self.prev_close),
            abs(self.minute_low - self.prev_close),
        )
        self.tr_history.append(tr)

        if len(self.tr_history) >= self.atr_period:
            self.atr_val = round(sum(self.tr_history[-self.atr_period:]) / self.atr_period, 2)
        else:
            self.atr_val = None

        self.prev_close = self.minute_close
        self.last_minute_key = minute_key
        self.minute_high = fut_ltp
        self.minute_low = fut_ltp
        self.minute_close = fut_ltp

        return self.atr_val

# =========================================================
# PNL / POSITIONS
# =========================================================

def compute_unreal_m2m(kite: KiteConnect, positions: dict, ce_inst: str, pe_inst: str) -> float:
    if not positions:
        return 0.0
    ltps = safe_ltp_many(kite, [ce_inst, pe_inst])
    unreal = 0.0

    for leg, inst in [("CE", ce_inst), ("PE", pe_inst)]:
        if leg not in positions:
            continue
        ltp = ltps.get(inst, float("nan"))
        if math.isnan(ltp):
            continue
        entry = float(positions[leg]["entry"])
        qty = int(positions[leg]["qty"])
        if positions[leg]["side"].upper() == "SELL":
            unreal += (entry - ltp) * qty
        else:
            unreal += (ltp - entry) * qty

    return float(unreal)

def square_off(conn, kite: KiteConnect, positions: dict, ce_ts: str, pe_ts: str,
               underlying_fut: float, atr: float | None, reason: str, event: str):
    """Exit both legs and log exit prices + include entry prices."""
    if not positions:
        return

    ce_inst = f"NFO:{ce_ts}"
    pe_inst = f"NFO:{pe_ts}"
    ltps = safe_ltp_many(kite, [ce_inst, pe_inst])

    ce_entry = positions.get("CE", {}).get("entry")
    pe_entry = positions.get("PE", {}).get("entry")

    for leg, tsym, inst, offset in [("CE", ce_ts, ce_inst, 0), ("PE", pe_ts, pe_inst, 1)]:
        if leg not in positions:
            continue
        qty = int(positions[leg]["qty"])
        exit_side = "BUY" if positions[leg]["side"].upper() == "SELL" else "SELL"
        exit_price = ltps.get(inst, float("nan"))
        if math.isnan(exit_price):
            exit_price = 0.0

        if LIVE_MODE:
            stocko_place_by_tradingsymbol(tsym, exit_side, qty, offset=100 + offset)

        unreal = compute_unreal_m2m(kite, positions, ce_inst, pe_inst)

        log_db(
            conn,
            event="EXIT",
            reason=reason,
            symbol=inst,
            side=exit_side,
            qty=qty,
            price=exit_price,
            underlying_fut=underlying_fut,
            atr=atr,
            unreal=unreal,
            total=unreal,
            ce_entry=ce_entry,
            pe_entry=pe_entry,
            ce_ltp=ltps.get(ce_inst) if not math.isnan(ltps.get(ce_inst, float("nan"))) else None,
            pe_ltp=ltps.get(pe_inst) if not math.isnan(ltps.get(pe_inst, float("nan"))) else None,
            ce_exit=exit_price if leg == "CE" else None,
            pe_exit=exit_price if leg == "PE" else None,
        )

    log_db(
        conn,
        event=event,
        reason=reason,
        symbol="ALL",
        side="NA",
        qty=0,
        price=0,
        underlying_fut=underlying_fut,
        atr=atr,
        unreal=0.0,
        total=0.0,
        ce_entry=ce_entry,
        pe_entry=pe_entry,
        ce_exit=ltps.get(ce_inst) if not math.isnan(ltps.get(ce_inst, float("nan"))) else None,
        pe_exit=ltps.get(pe_inst) if not math.isnan(ltps.get(pe_inst, float("nan"))) else None,
    )

    positions.clear()

# =========================================================
# STRATEGY (ENTRY SIGNAL) – FUT ONLY
# =========================================================

def should_enter(now: datetime, underlying_fut: float, positions: dict) -> tuple[bool, str, int, float]:
    if positions:
        return False, "POSITION_OPEN", 0, 0.0
    if now.time() < ENTRY_START_TIME:
        return False, f"BEFORE_ENTRY_START_{ENTRY_START_TIME.strftime('%H:%M')}", 0, 0.0

    atm = int(round(underlying_fut / STRIKE_STEP) * STRIKE_STEP)
    diff = abs(underlying_fut - atm)
    if diff <= ENTRY_TOL:
        return True, f"ATM_WITHIN_TOL_{ENTRY_TOL}_ATM_{atm}_DIFF_{diff:.2f}", atm, diff
    return False, f"NO_ENTRY_DIFF_{diff:.2f}_GT_{ENTRY_TOL}", atm, diff

# =========================================================
# MAIN
# =========================================================

def main():
    conn = db_connect()
    ensure_table(conn)
    ensure_trade_flag(conn)  # ✅ creates trade_flag + live_ss_nifty_fut column + row id=1
    kite = kite_connect()

    nfo_df = load_instruments_once(kite)

    fut_ts = FUT_TRADINGSYMBOL_OVERRIDE or get_nearest_nifty_fut_symbol(nfo_df)
    fut_inst = f"NFO:{fut_ts}"

    atr_builder = FutAtrBuilder(ATR_PERIOD)

    positions = {}
    ce_ts = pe_ts = ""
    trading_halted = False
    trade_allowed_cached = False

    last_snapshot_ts = 0.0
    last_atr = None
    last_market_status = None

    print(f"[START] {BOT_NAME} | LIVE_MODE={LIVE_MODE} | TABLE={TABLE_NAME}")
    print(f"[CFG] ENTRY_START_TIME={ENTRY_START_TIME.strftime('%H:%M')} ENTRY_TOL={ENTRY_TOL} STRIKE_STEP={STRIKE_STEP} QTY={QTY_PER_LEG}")
    print(f"[CFG] PROFIT_TARGET={PROFIT_TARGET} CIRCUIT_STOP_LOSS={CIRCUIT_STOP_LOSS} SQUARE_OFF_TIME={SQUARE_OFF_TIME.strftime('%H:%M')}")
    print(f"[CFG] FUT_UNDERLYING={fut_inst} ATR_PERIOD={ATR_PERIOD}")
    print(f"[CFG] FLAG_TABLE={FLAG_TABLE} FLAG_COL={FLAG_COL}")
    if FUT_TRADINGSYMBOL_OVERRIDE:
        print(f"[CFG] FUT_TRADINGSYMBOL override enabled: {FUT_TRADINGSYMBOL_OVERRIDE}")
    if not NSE_HOLIDAYS:
        print("[WARN] NSE_HOLIDAYS not set. Weekends blocked, but holidays won’t be blocked until you set NSE_HOLIDAYS.")

    while True:
        now = datetime.now(MARKET_TZ)

        # ---- Outside session: NO API calls, NO DB writes ----
        is_open, reason = market_session_state(now)
        if not is_open:
            if last_market_status != reason:
                print(f"[INFO] Market CLOSED ({reason}). Bot idle (no API/no DB).")
                last_market_status = reason
            time.sleep(30)
            continue
        else:
            if last_market_status is not None:
                print("[INFO] Market OPEN. Bot active.")
                last_market_status = None

        # ---- Fetch FUT LTP only ----
        ltps = safe_ltp_many(kite, [fut_inst])
        fut_ltp = ltps.get(fut_inst, float("nan"))
        if math.isnan(fut_ltp):
            time.sleep(POLL_INTERVAL_SEC)
            continue

        underlying = float(fut_ltp)

        # ---- Update ATR every tick (FUT-based) ----
        last_atr = atr_builder.update(now, underlying)

        # ---- Forced square-off at 15:25 ----
        if now.time() >= SQUARE_OFF_TIME:
            if positions and ce_ts and pe_ts:
                square_off(conn, kite, positions, ce_ts, pe_ts, underlying, last_atr, "TIME_1525", "TIME_EXIT")
            else:
                log_db(conn, event="TIME_EXIT", reason="TIME_1525_NO_POS",
                       symbol="ALL", side="NA", qty=0, price=0,
                       underlying_fut=underlying, atr=last_atr, unreal=0.0, total=0.0)
            print("[STOP] 15:25 square-off executed. Exiting process.")
            return

        # ---- Minute snapshot: flag + CE/PE details + M2M ----
        if time.time() - last_snapshot_ts >= SNAPSHOT_INTERVAL_SEC:
            trade_allowed_cached = read_trade_flag(conn)

            ce_entry = positions.get("CE", {}).get("entry") if positions else None
            pe_entry = positions.get("PE", {}).get("entry") if positions else None

            ce_inst = f"NFO:{ce_ts}" if ce_ts else None
            pe_inst = f"NFO:{pe_ts}" if pe_ts else None

            ce_ltp = pe_ltp = None
            unreal = 0.0

            if positions and ce_inst and pe_inst:
                leg_ltps = safe_ltp_many(kite, [ce_inst, pe_inst])
                _ce = leg_ltps.get(ce_inst, float("nan"))
                _pe = leg_ltps.get(pe_inst, float("nan"))
                ce_ltp = None if math.isnan(_ce) else float(_ce)
                pe_ltp = None if math.isnan(_pe) else float(_pe)
                unreal = compute_unreal_m2m(kite, positions, ce_inst, pe_inst)

            log_db(
                conn,
                event="SNAPSHOT",
                reason=f"FLAG={trade_allowed_cached},HALT={trading_halted}",
                symbol="NIFTY_FUT",
                side="NA",
                qty=0,
                price=0,
                underlying_fut=underlying,
                atr=last_atr,
                unreal=unreal,
                total=unreal,
                ce_entry=ce_entry,
                pe_entry=pe_entry,
                ce_ltp=ce_ltp,
                pe_ltp=pe_ltp,
            )

            last_snapshot_ts = time.time()

            # Kill-switch enforcement
            if (not trade_allowed_cached) and positions and ce_ts and pe_ts:
                square_off(conn, kite, positions, ce_ts, pe_ts, underlying, last_atr, "FLAG_FALSE", "FLAG_EXIT")
                trading_halted = True

            # Resume logic
            if trade_allowed_cached and trading_halted and not positions:
                trading_halted = False
                log_db(conn, event="RESUME", reason="FLAG_TRUE_RESUME",
                       symbol="ALL", side="NA", qty=0, price=0,
                       underlying_fut=underlying, atr=last_atr, unreal=0.0, total=0.0)

        # ---- If halted, do not enter ----
        if trading_halted:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        # ---- Risk checks ----
        if positions and ce_ts and pe_ts:
            ce_inst = f"NFO:{ce_ts}"
            pe_inst = f"NFO:{pe_ts}"
            unreal = compute_unreal_m2m(kite, positions, ce_inst, pe_inst)

            if CIRCUIT_STOP_LOSS > 0 and unreal <= -abs(CIRCUIT_STOP_LOSS):
                square_off(conn, kite, positions, ce_ts, pe_ts, underlying, last_atr, "CIRCUIT_SL", "CIRCUIT_SL")
                trading_halted = True
                time.sleep(POLL_INTERVAL_SEC)
                continue

            if PROFIT_TARGET > 0 and unreal >= PROFIT_TARGET:
                square_off(conn, kite, positions, ce_ts, pe_ts, underlying, last_atr, "PROFIT_TARGET", "PROFIT_EXIT")
                trading_halted = True
                time.sleep(POLL_INTERVAL_SEC)
                continue

        # ---- Entry logic (FUT-based ATM) ----
        if not positions:
            enter, why, atm, diff = should_enter(now, underlying, positions)

            trade_allowed_now = trade_allowed_cached
            if RECHECK_FLAG_ON_ENTRY:
                trade_allowed_now = read_trade_flag(conn)

            if enter and trade_allowed_now:
                ce_ts, pe_ts, expiry = resolve_atm_ce_pe(nfo_df, atm, now.date())
                ce_inst = f"NFO:{ce_ts}"
                pe_inst = f"NFO:{pe_ts}"

                leg_ltps = safe_ltp_many(kite, [ce_inst, pe_inst])
                ce_price = leg_ltps.get(ce_inst, float("nan"))
                pe_price = leg_ltps.get(pe_inst, float("nan"))
                if math.isnan(ce_price) or math.isnan(pe_price):
                    time.sleep(POLL_INTERVAL_SEC)
                    continue

                if LIVE_MODE:
                    stocko_place_by_tradingsymbol(ce_ts, "SELL", QTY_PER_LEG, offset=1)
                    stocko_place_by_tradingsymbol(pe_ts, "SELL", QTY_PER_LEG, offset=2)

                positions["CE"] = {"side": "SELL", "qty": QTY_PER_LEG, "entry": float(ce_price), "strike": atm, "expiry": expiry}
                positions["PE"] = {"side": "SELL", "qty": QTY_PER_LEG, "entry": float(pe_price), "strike": atm, "expiry": expiry}

                unreal = compute_unreal_m2m(kite, positions, ce_inst, pe_inst)

                log_db(conn, event="ENTRY", reason=why, symbol=ce_inst, side="SELL",
                       qty=QTY_PER_LEG, price=ce_price,
                       underlying_fut=underlying, atr=last_atr, unreal=unreal, total=unreal,
                       ce_entry=ce_price, pe_entry=pe_price, ce_ltp=float(ce_price), pe_ltp=float(pe_price))

                log_db(conn, event="ENTRY", reason=why, symbol=pe_inst, side="SELL",
                       qty=QTY_PER_LEG, price=pe_price,
                       underlying_fut=underlying, atr=last_atr, unreal=unreal, total=unreal,
                       ce_entry=ce_price, pe_entry=pe_price, ce_ltp=float(ce_price), pe_ltp=float(pe_price))

                log_db(conn, event="ENTRY_ALL", reason=f"{why},EXP={expiry}", symbol="ALL", side="NA",
                       qty=0, price=0,
                       underlying_fut=underlying, atr=last_atr, unreal=unreal, total=unreal,
                       ce_entry=ce_price, pe_entry=pe_price, ce_ltp=float(ce_price), pe_ltp=float(pe_price))

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main()
