#!/usr/bin/env python3
"""
Checkpoint Validator — Stock Analysis Agent v2
Runs automatically on day 5, 10, and 28 after each analysis.
Fetches current prices and fills outcome fields in validation_log.json.

Usage:
    python checkpoint_validator.py

Outcomes:
    CORRECT  — price moved in predicted direction >3%
    WRONG    — price moved against prediction >3%
    NEUTRAL  — price moved <3% either way
    UNKNOWN  — price data unavailable
"""

import json
import yfinance as yf
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

LOG_PATH = Path("validation_log.json")

def fmt_inr(value):
    if value is None: return "NULL"
    return f"Rs.{value:,.2f}"

def check_outcome(direction, entry_price, current_price):
    if not entry_price or not current_price:
        return "UNKNOWN"
    pct = ((current_price - entry_price) / entry_price) * 100
    if abs(pct) < 3:
        return f"NEUTRAL ({pct:+.1f}%)"
    if direction == "BULLISH" and pct > 0:
        return f"CORRECT ({pct:+.1f}%)"
    if direction == "BEARISH" and pct < 0:
        return f"CORRECT ({pct:+.1f}%)"
    if direction == "NEUTRAL":
        return f"N/A — direction was NEUTRAL"
    return f"WRONG ({pct:+.1f}%)"

# ─────────────────────────────────────────────
# CELL 6 — Validation Checker
# Run this cell on day 5, 10, and 28.
# Fetches current prices and fills outcome fields.
# CORRECT  = price moved in predicted direction >3%
# WRONG    = price moved against prediction >3%
# NEUTRAL  = price moved <3% either way
# ─────────────────────────────────────────────

def check_outcome(direction, entry_price, current_price):
    """Determine outcome given predicted direction and price change."""
    if entry_price is None or current_price is None:
        return "UNKNOWN"
    pct_change = ((current_price - entry_price) / entry_price) * 100
    if abs(pct_change) < 3:
        return f"NEUTRAL ({pct_change:+.1f}%)"
    if direction == "BULLISH" and pct_change > 0:
        return f"CORRECT ({pct_change:+.1f}%)"
    if direction == "BEARISH" and pct_change < 0:
        return f"CORRECT ({pct_change:+.1f}%)"
    if direction == "NEUTRAL":
        return f"N/A — direction was NEUTRAL"
    return f"WRONG ({pct_change:+.1f}%)"

print("\n" + "─"*62 + "\n  CHECKPOINT VALIDATOR\n" + "─"*62)
today_str = datetime.today().strftime("%Y-%m-%d")
print(f"\n  Running validation check on: {today_str}")

if not LOG_PATH.exists():
    print("  No validation_log.json found. Run Cells 1-5 first.")
else:
    with open(LOG_PATH, "r") as f:
        log = json.load(f)

    updated = 0
    for record in log:
        ticker_ns = record["stock"] + ".NS"
        direction = record.get("direction")

        for cp_key, cp in record.get("checkpoints", {}).items():
            if cp["date"] != today_str or cp["outcome"] is not None:
                continue

            # Fetch current price
            try:
                info  = yf.Ticker(ticker_ns).fast_info
                price = round(float(info.last_price), 2)
            except Exception:
                price = None

            # Entry price = price at analysis date
            entry = record.get("entry_price")
            if entry is None:
                try:
                    hist  = yf.Ticker(ticker_ns).history(period="1d", start=record["analysis_date"])
                    entry = round(float(hist["Close"].iloc[0]), 2) if not hist.empty else None
                    record["entry_price"] = entry
                except Exception:
                    entry = None

            outcome        = check_outcome(direction, entry, price)
            cp["price"]    = price
            cp["outcome"]  = outcome
            updated       += 1

            print(f"  {record['stock']:<16} {cp_key}  entry={fmt_inr(entry)}  now={fmt_inr(price)}  → {outcome}")

    if updated == 0:
        print(f"  No checkpoints due today. Check again on a checkpoint date.")
        print(f"  Upcoming checkpoints:")
        shown = set()
        for record in log:
            for cp_key, cp in record.get("checkpoints", {}).items():
                if cp["outcome"] is None and cp["date"] not in shown:
                    print(f"    {cp['date']} ({cp_key})")
                    shown.add(cp["date"])
    else:
        with open(LOG_PATH, "w") as f:
            json.dump(log, f, indent=2)
        print(f"\n  {updated} checkpoint(s) updated and saved to {LOG_PATH.resolve()}")

print(f"\n  This is a data-driven signal, not financial advice.")
print(f"  All signals require human validation before acting.")
print(f"  Investments are subject to market risk.")
print(f"\n  {'═'*60}")
print(f"  Run complete: {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Log: {LOG_PATH.resolve()}")
print(f"  This is a data-driven signal, not financial advice.")
