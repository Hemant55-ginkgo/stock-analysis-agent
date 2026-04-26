#!/usr/bin/env python3
"""
Stock Analysis Agent v2 — NSE/BSE Large Cap Multi-Stock
Event-driven signal pipeline. Runs automatically via GitHub Actions
every Sunday 9am IST. Results saved to validation_log.json.

Usage:
    python stock_analysis_agent_v2.py

Requirements:
    pip install yfinance pandas python-dotenv anthropic requests

Environment variables (set in .env or GitHub Secrets):
    ANTHROPIC_API_KEY

This is a data-driven signal tool, not financial advice.
All signals require human validation before acting.
Investments are subject to market risk.
"""

# ─────────────────────────────────────────────
# CELL 1 — Setup: install yfinance and helpers
# Run once per session.
# ─────────────────────────────────────────────
# Dependencies: pip install yfinance pandas python-dotenv anthropic requests
import json
from dotenv import load_dotenv
load_dotenv()  # loads .env file if present
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

LOG_PATH = Path("validation_log.json")

def fmt_inr(value, decimals=2):
    if value is None: return "NULL"
    return f"Rs.{value:,.{decimals}f}"

def fmt_mcap(crore):
    if crore is None: return "NULL"
    if crore >= 100_000: return f"Rs.{crore/100_000:.2f} lakh crore"
    return f"Rs.{crore:,.0f} crore"

def null_or(value, formatter=None):
    if value is None: return "NULL"
    return formatter(value) if formatter else value

def section(title):
    bar = "─" * 62
    print(f"\n{bar}\n  {title}\n{bar}")

def field(label, value, width=32):
    display = "NULL" if value is None else str(value)
    print(f"  {label:<{width}} {display}")

def months_ago(date_str):
    dt = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
    return (datetime.today() - dt).days / 30.44



# ─────────────────────────────────────────────
# CELL 2 — Configuration
# Define your stock basket and settings here.
# NSE tickers need .NS suffix for yfinance.
# ─────────────────────────────────────────────

STOCK_BASKET = [
    {"ticker": "RELIANCE.NS",  "name": "Reliance Industries",  "sector": "Energy/Retail/Telecom"},
    {"ticker": "TCS.NS",       "name": "Tata Consultancy Services", "sector": "IT"},
    {"ticker": "HDFCBANK.NS",  "name": "HDFC Bank",            "sector": "Banking"},
    {"ticker": "INFY.NS",      "name": "Infosys",              "sector": "IT"},
    {"ticker": "ICICIBANK.NS", "name": "ICICI Bank",           "sector": "Banking"},
    {"ticker": "SUNPHARMA.NS", "name": "Sun Pharma",           "sector": "Pharma"},
    {"ticker": "TATAMOTORS.NS","name": "Tata Motors",          "sector": "Auto"},
    {"ticker": "WIPRO.NS",     "name": "Wipro",                "sector": "IT"},
]

# History window for event detection
HISTORY_YEARS   = 5

# Event detection threshold
EVENT_THRESHOLD = 5.0   # % move in window to count as event
JUMP_THRESHOLD  = 8.0   # % move in window to count as jump
WINDOW_DAYS     = 3     # rolling window for move detection

# Validation checkpoints (days from analysis date)
CHECKPOINTS = [5, 10, 28]

# Sector momentum — set manually until auto-compute is built
# BUY / WATCH / AVOID
SECTOR_MOMENTUM = {
    "Energy/Retail/Telecom": "BUY",
    "IT":                    "WATCH",
    "Banking":               "BUY",
    "Pharma":                "WATCH",
    "Auto":                  "BUY",
}

# Global conditions — update daily
GLOBAL = {
    "crude_trend":     "rising",    # rising / falling / stable
    "usd_inr":         83.42,
    "usd_inr_trend":   "stable",    # appreciating / depreciating / stable
    "us_market_trend": "positive",  # positive / negative / neutral
    "india_vix":       16.4,
    "policy_event_within_5d": False,
}

print(f"[CONFIG] Basket: {len(STOCK_BASKET)} stocks — " + ", ".join(s["ticker"] for s in STOCK_BASKET))
# Earnings days away overrides — update each quarter
# Q1 results ~Aug, Q2 ~Nov, Q3 ~Feb, Q4 ~May
EARNINGS_OVERRIDES = {
    "RELIANCE":   12,
    "TCS":        90,
    "HDFCBANK":   10,
    "INFY":       90,
    "ICICIBANK":  14,
    "SUNPHARMA":  28,
    "WIPRO":      90,
}


# ─────────────────────────────────────────────
# CELL 3 — yfinance Data Fetcher
# Pulls live data for every stock in the basket.
# Computes: price, MAs, volume, 52w range,
# RSI, market cap, and event history.
# ─────────────────────────────────────────────

def compute_rsi(series, period=14):
    """Compute RSI from a price series."""
    delta  = series.diff()
    gain   = delta.clip(lower=0).rolling(period).mean()
    loss   = (-delta.clip(upper=0)).rolling(period).mean()
    rs     = gain / loss
    return round(float((100 - (100 / (1 + rs))).iloc[-1]), 2)

def detect_events(hist, threshold=5.0, window=3):
    """Detect price moves > threshold% in any rolling window."""
    events = []
    closes = hist["Close"]
    for i in range(window, len(closes)):
        start_price = float(closes.iloc[i - window])
        end_price   = float(closes.iloc[i])
        if start_price == 0:
            continue
        pct = ((end_price - start_price) / start_price) * 100
        if abs(pct) >= threshold:
            date_str = str(closes.index[i])[:10]
            # Avoid duplicate events within 2 days
            if events and (datetime.strptime(date_str, "%Y-%m-%d") -
                           datetime.strptime(events[-1]["date"], "%Y-%m-%d")).days < 2:
                continue
            events.append({
                "date":           date_str,
                "magnitude_pct":  round(abs(pct), 2),
                "direction":      "UP" if pct > 0 else "DOWN",
                "window_days":    window,
                "cause_category": "UNKNOWN",
                "news_headline":  None,
                "news_date":      None,
                "news_source":    None
            })
    return events

def compute_earnings_days_away(nse_ticker):
    """
    Fetch next earnings date from Screener.in.
    Returns days until next quarterly result, or None if unavailable.
    """
    import re
    try:
        url = f'https://www.screener.in/company/{nse_ticker}/consolidated/'
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': 'text/html'
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode('utf-8', errors='ignore')

        raw_dates = re.findall(
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(20\d{2})',
            html
        )
        today = datetime.today()
        upcoming = []
        seen = set()
        for month, year in raw_dates:
            key = f'{month}{year}'
            if key in seen:
                continue
            seen.add(key)
            try:
                dt = datetime.strptime(f'15 {month} {year}', '%d %b %Y')
                if dt >= today:
                    upcoming.append(dt)
            except Exception:
                continue

        if not upcoming:
            # Estimate: last known quarter + 90 days
            all_dates = []
            for month, year in raw_dates:
                try:
                    dt = datetime.strptime(f'15 {month} {year}', '%d %b %Y')
                    all_dates.append(dt)
                except Exception:
                    continue
            if all_dates:
                next_est = max(all_dates) + timedelta(days=90)
                return max(0, (next_est - today).days)
            return None

        return max(0, (min(upcoming) - today).days)

    except Exception:
        return None

def fetch_stock_data(stock_config):
    """Fetch and compute all fields for one stock."""
    ticker  = stock_config["ticker"]
    name    = stock_config["name"]
    sector  = stock_config["sector"]

    print(f"  Fetching {ticker}...", end=" ")

    try:
        yf_ticker = yf.Ticker(ticker)
        end_date  = datetime.today()
        start_date = end_date - timedelta(days=HISTORY_YEARS * 365 + 30)

        hist = yf_ticker.history(start=start_date, end=end_date)

        if hist.empty or len(hist) < 100:
            print("FAILED — insufficient history")
            return None

        closes = hist["Close"]
        volumes = hist["Volume"]

        current_price = round(float(closes.iloc[-1]), 2)
        week52_high   = round(float(closes.tail(252).max()), 2)
        week52_low    = round(float(closes.tail(252).min()), 2)
        ma_50         = round(float(closes.tail(50).mean()), 2)
        ma_200        = round(float(closes.tail(200).mean()), 2) if len(closes) >= 200 else None
        vol_5d_avg    = int(volumes.tail(5).mean())
        vol_30d_avg   = int(volumes.tail(30).mean())
        rsi_14        = compute_rsi(closes)
        history_days  = len(hist)
        gaps_filled   = int(hist["Close"].isna().sum())

        # Market cap in crore (yfinance gives in base currency)
        info = yf_ticker.fast_info
        try:
            market_cap_cr = round(info.market_cap / 1e7, 0) if info.market_cap else None
        except Exception:
            market_cap_cr = None

        # Detect events
        events = detect_events(hist, threshold=EVENT_THRESHOLD, window=WINDOW_DAYS)

        # Quality checks
        quality_fail   = gaps_filled >= 10 or history_days < 500
        fail_reason    = None
        if gaps_filled >= 10:   fail_reason = f"price_gaps_filled={gaps_filled} exceeds max of 10"
        elif history_days < 500: fail_reason = f"history_days={history_days} below minimum of 500"

        print(f"OK — {history_days} days, {len(events)} events, price Rs.{current_price}")

        return {
            # Step 1
            "stock_name":         name,
            "ticker":             ticker.replace(".NS", ""),
            "exchange":           "NSE",
            "sector":             sector,
            "market_cap_cr":      market_cap_cr,
            "current_price":      current_price,
            "week52_high":        week52_high,
            "week52_low":         week52_low,
            "history_days":       history_days,
            "price_gaps_filled":  gaps_filled,
            "news_items_found":   0,  # news fetch not yet implemented
            "sector_momentum":    SECTOR_MOMENTUM.get(sector),
            "data_quality_fail":  quality_fail,
            "fail_reason":        fail_reason,
            # Step 2
            "events":             events,
            # Step 3
            "rsi_14":             rsi_14,
            "ma_50":              ma_50,
            "ma_200":             ma_200,
            "vol_5d_avg":         vol_5d_avg,
            "vol_30d_avg":        vol_30d_avg,
            "earnings_days_away": compute_earnings_days_away(ticker.replace('.NS','')),
            "fii_flow_10d_cr":    None,  # requires NSDL/CDSL data
            "promoter_pledging_change": None,  # requires BSE filings
            # Global (shared across all stocks)
            **GLOBAL
        }

    except Exception as e:
        print(f"ERROR — {e}")
        return None

section("STEP 0 — FETCHING LIVE DATA")
print()

ALL_DATA = {}
for stock in STOCK_BASKET:
    data = fetch_stock_data(stock)
    if data:
        ALL_DATA[data["ticker"]] = data

print(f"\nFetch complete: {len(ALL_DATA)} of {len(STOCK_BASKET)} stocks loaded successfully.")
FAILED = [s["ticker"].replace(".NS","") for s in STOCK_BASKET if s["ticker"].replace(".NS","") not in ALL_DATA]
if FAILED:
    print(f"Failed tickers: {FAILED}")

# ─────────────────────────────────────────────
# CELL 3B v2 — Earnings + Announcements Tagger
#
# Three-layer event tagging, free and automatic:
#
# Layer 1: Screener.in quarterly results
#          → tags all earnings events by date
#          → 4 events/year × 5 years = 20 per stock
#
# Layer 2: NSE corporate announcements API
#          → tags promoter, AGM, buyback, mergers
#          → official NSE filings, highly accurate
#
# Layer 3: LLM via keyword cache
#          → handles remaining untagged events
#          → switchable: anthropic / openai / none
#
# Events older than 4 weeks with no match → UNKNOWN
# (acceptable — these are usually noise events)
# ─────────────────────────────────────────────

import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import json
import time
import os
from pathlib import Path
from datetime import datetime, timedelta

# ── LLM provider config ──────────────────────
LLM_PROVIDER = 'anthropic'           # 'anthropic' / 'openai' / 'none'
LLM_MODEL    = 'claude-haiku-4-5-20251001'
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', 'YOUR_KEY_HERE')
OPENAI_API_KEY    = os.environ.get('OPENAI_API_KEY', '')

CACHE_PATH = Path('news_cache.json')
CATEGORIES = ['earnings','fii','promoter','macro','policy','global','company-specific','UNKNOWN']

# ── NSE ticker map ───────────────────────────
# Screener.in and NSE use plain ticker (no .NS suffix)
# Add any additional stocks here as needed
NSE_TICKER_MAP = {
    'RELIANCE':   'RELIANCE',
    'TCS':        'TCS',
    'HDFCBANK':   'HDFCBANK',
    'INFY':       'INFY',
    'ICICIBANK':  'ICICIBANK',
    'SUNPHARMA':  'SUNPHARMA',
    'TATAMOTORS': 'TATAMOTORS',
    'WIPRO':      'WIPRO',
}

# ── Keyword cache ────────────────────────────
SEED_KEYWORDS = {
    'earnings':         ['profit','quarterly','results','revenue','eps','net income',
                          'q1','q2','q3','q4','fy','beats','misses','guidance',
                          'ebitda','pat','net profit','consolidated','standalone'],
    'fii':              ['fii','fpi','foreign investor','foreign institutional',
                          'outflow','inflow','foreign buying','foreign selling',
                          'foreign portfolio'],
    'promoter':         ['promoter','pledging','stake','buyback','insider',
                          'founder','rights issue','block deal','bulk deal'],
    'macro':            ['rbi','repo rate','inflation','cpi','gdp','fed',
                          'interest rate','monetary policy','federal reserve',
                          'rate hike','rate cut','bond yield'],
    'policy':           ['sebi','budget','government policy','regulation','tax',
                          'gst','ministry','import duty','export ban','windfall',
                          'subsidy','divestment'],
    'global':           ['crude','oil','china','us market','nasdaq','dow','war',
                          'sanctions','global selloff','recession','ukraine',
                          'middle east','opec','dollar index'],
    'company-specific': ['agm','merger','acquisition','joint venture','jv',
                          'partnership','contract','order win','expansion',
                          'capex','ceo','management','demerger','restructuring',
                          'plant','launch','5g','gigafactory'],
}

def load_cache():
    if CACHE_PATH.exists():
        with open(CACHE_PATH) as f:
            return json.load(f)
    return {
        'keywords':   SEED_KEYWORDS,
        'classified': {},
        'earnings_dates': {},   # ticker → [date strings]
        'stats': {'cache_hits': 0, 'llm_calls': 0,
                  'screener_hits': 0, 'nse_hits': 0}
    }

def save_cache(cache):
    with open(CACHE_PATH, 'w') as f:
        json.dump(cache, f, indent=2)

# ── Layer 1: Screener.in earnings dates ──────
def fetch_screener_earnings(ticker, cache):
    """
    Fetch quarterly result dates from Screener.in.
    Returns list of date strings YYYY-MM-DD.
    Caches results — only fetches once per ticker per session.

    Screener.in company page structure:
    https://www.screener.in/company/RELIANCE/consolidated/
    The quarterly results table has dates in <th> tags
    under the div id='quarters'.
    We parse the HTML table directly.
    """
    if ticker in cache.get('earnings_dates', {}):
        return cache['earnings_dates'][ticker]

    dates = []
    try:
        url = f'https://www.screener.in/company/{ticker}/consolidated/'
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml'
            }
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode('utf-8', errors='ignore')

        # Find the quarters section — Screener uses 'id="quarters"'
        # Dates appear as 'MMM YYYY' in <th> elements e.g. 'Sep 2024'
        import re
        quarters_section = re.search(
            r'id=["\']quarters["\'].*?</section>',
            html, re.DOTALL
        )
        if quarters_section:
            # Extract month-year patterns like 'Sep 2024', 'Mar 2024'
            raw_dates = re.findall(
                r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(20\d{2})',
                quarters_section.group(0)
            )
            for month, year in raw_dates:
                try:
                    # Screener shows result announcement month
                    # Convert to last day of that month as approximate date
                    dt = datetime.strptime(f'15 {month} {year}', '%d %b %Y')
                    dates.append(dt.strftime('%Y-%m-%d'))
                except Exception:
                    continue

        cache.setdefault('earnings_dates', {})[ticker] = dates
        time.sleep(1.0)  # polite delay for Screener

    except Exception as e:
        print(f'      Screener fetch failed for {ticker}: {e}')
        cache.setdefault('earnings_dates', {})[ticker] = []

    return dates

def is_earnings_event(event_date, earnings_dates, window_days=14):
    """
    Returns True if event_date falls within window_days
    of any known earnings announcement date.
    """
    try:
        ev_dt = datetime.strptime(event_date, '%Y-%m-%d')
    except Exception:
        return False, None
    for ed in earnings_dates:
        try:
            earn_dt = datetime.strptime(ed, '%Y-%m-%d')
            if abs((ev_dt - earn_dt).days) <= window_days:
                return True, ed
        except Exception:
            continue
    return False, None

# ── Layer 2: NSE corporate announcements ─────
def fetch_nse_announcements(ticker):
    """
    Fetch corporate announcements from NSE India public API.
    Endpoint: https://www.nseindia.com/api/corp-announcements
    Returns list of dicts with 'date' and 'subject' keys.

    NSE requires a session cookie — we first hit the main page
    to get cookies, then call the API.

    Subject keywords map to cause categories:
      'board meeting', 'results'       → earnings
      'buyback', 'pledg', 'promoter'   → promoter
      'agm', 'merger', 'acquisition'   → company-specific
      'dividend'                        → company-specific
    """
    announcements = []
    try:
        opener = urllib.request.build_opener()
        opener.addheaders = [
            ('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'),
            ('Accept', 'application/json, text/javascript, */*'),
            ('Accept-Language', 'en-US,en;q=0.9'),
            ('Referer', 'https://www.nseindia.com/')
        ]

        # Step 1: get session cookie from NSE homepage
        opener.open('https://www.nseindia.com/', timeout=10)
        time.sleep(0.5)

        # Step 2: call announcements API
        api_url = f'https://www.nseindia.com/api/corp-announcements?index=equities&symbol={ticker}'
        with opener.open(api_url, timeout=10) as resp:
            data = json.loads(resp.read())

        # NSE returns list of announcement objects
        # Key fields: 'bflag' (date), 'subject'
        for item in data if isinstance(data, list) else data.get('data', []):
            raw_date = item.get('bflag') or item.get('date') or item.get('an_dt', '')
            subject  = item.get('subject', '') or item.get('desc', '')
            if raw_date and subject:
                # NSE dates come as 'DD-Mon-YYYY' e.g. '17-Oct-2024'
                try:
                    dt = datetime.strptime(raw_date[:11], '%d-%b-%Y')
                    announcements.append({
                        'date':    dt.strftime('%Y-%m-%d'),
                        'subject': subject.strip()
                    })
                except Exception:
                    try:
                        dt = datetime.strptime(raw_date[:10], '%Y-%m-%d')
                        announcements.append({
                            'date':    dt.strftime('%Y-%m-%d'),
                            'subject': subject.strip()
                        })
                    except Exception:
                        continue

    except Exception as e:
        pass  # NSE sometimes blocks — silently continue

    return announcements

# Subject → category keyword map for NSE announcements
NSE_SUBJECT_MAP = [
    (['board meeting','financial results','quarterly results',
      'annual results','unaudited'],                              'earnings'),
    (['buyback','buy-back','promoter','pledg','creati','revok'],  'promoter'),
    (['agm','annual general','extraordinary general','egm',
      'merger','amalgam','acqui','demerger','scheme'],            'company-specific'),
    (['dividend','interim dividend','final dividend'],            'company-specific'),
    (['rights issue','rights entitlement'],                       'promoter'),
    (['capex','expansion','new plant','order','contract',
      'joint venture','collaboration','mou'],                     'company-specific'),
]

def classify_nse_subject(subject):
    s = subject.lower()
    for keywords, category in NSE_SUBJECT_MAP:
        if any(k in s for k in keywords):
            return category
    return None

def find_nse_announcement(event_date, announcements, window_days=7):
    try:
        ev_dt = datetime.strptime(event_date, '%Y-%m-%d')
    except Exception:
        return None, None, None
    for ann in announcements:
        try:
            ann_dt = datetime.strptime(ann['date'], '%Y-%m-%d')
            if abs((ev_dt - ann_dt).days) <= window_days:
                cat = classify_nse_subject(ann['subject'])
                if cat:
                    return cat, ann['subject'], ann['date']
        except Exception:
            continue
    return None, None, None

# ── Layer 3: keyword cache + LLM ─────────────
def keyword_classify(headline, keywords):
    h = headline.lower()
    scores = {}
    for cat, words in keywords.items():
        score = sum(1 for w in words if w.lower() in h)
        if score > 0:
            scores[cat] = score
    if not scores:
        return None
    top  = max(scores, key=scores.get)
    vals = sorted(scores.values(), reverse=True)
    if len(vals) == 1 or vals[0] >= vals[1] * 1.5:
        return top
    return None

MINI_PROMPT = ('Classify this Indian stock news headline into exactly one category.\n'
               'Categories: earnings, fii, promoter, macro, policy, global, company-specific, UNKNOWN\n'
               'Reply with the category name only. No explanation.\n'
               'Headline: "{headline}"')

def llm_classify_anthropic(headline):
    body = json.dumps({
        'model': LLM_MODEL, 'max_tokens': 10,
        'messages': [{'role': 'user',
                      'content': MINI_PROMPT.format(headline=headline)}]
    }).encode()
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages', data=body,
        headers={'Content-Type': 'application/json',
                 'x-api-key': ANTHROPIC_API_KEY,
                 'anthropic-version': '2023-06-01'}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())['content'][0]['text'].strip().lower()

def llm_classify_openai(headline):
    body = json.dumps({
        'model': 'gpt-4o-mini', 'max_tokens': 10,
        'messages': [{'role': 'user',
                      'content': MINI_PROMPT.format(headline=headline)}]
    }).encode()
    req = urllib.request.Request(
        'https://api.openai.com/v1/chat/completions', data=body,
        headers={'Content-Type': 'application/json',
                 'Authorization': f'Bearer {OPENAI_API_KEY}'}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())['choices'][0]['message']['content'].strip().lower()

def llm_classify(headline):
    try:
        if LLM_PROVIDER == 'anthropic': return llm_classify_anthropic(headline)
        if LLM_PROVIDER == 'openai':    return llm_classify_openai(headline)
        return 'UNKNOWN'
    except Exception as e:
        print(f'      LLM error: {e}')
        return 'UNKNOWN'

def classify_via_cache_or_llm(headline, cache):
    if not headline:
        return 'UNKNOWN'
    # Previously classified
    if headline in cache['classified']:
        cache['stats']['cache_hits'] += 1
        return cache['classified'][headline]
    # Keyword match
    kw = keyword_classify(headline, cache['keywords'])
    if kw:
        cache['stats']['cache_hits'] += 1
        cache['classified'][headline] = kw
        return kw
    # LLM fallback
    if LLM_PROVIDER == 'none':
        return 'UNKNOWN'
    cache['stats']['llm_calls'] += 1
    result = llm_classify(headline)
    result = result if result in CATEGORIES else 'UNKNOWN'
    cache['classified'][headline] = result
    if result != 'UNKNOWN':
        words = [w.strip('.,') for w in headline.lower().split() if len(w) > 4]
        existing = [w.lower() for w in cache['keywords'].get(result, [])]
        new_words = [w for w in words if w not in existing and w.isalpha()]
        cache['keywords'].setdefault(result, []).extend(new_words[:3])
    time.sleep(0.2)
    return result

# ── Google News RSS (Layer 3 fallback) ───────
def fetch_google_rss(company_name, event_date, window_days=7):
    query   = urllib.parse.quote(f'{company_name} NSE India')
    url     = f'https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en'
    ev_dt   = datetime.strptime(event_date, '%Y-%m-%d')
    results = []
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            root = ET.fromstring(r.read())
        for item in root.findall('.//item'):
            title   = item.findtext('title', '').strip()
            pub_str = item.findtext('pubDate', '').strip()
            if not title or not pub_str:
                continue
            try:
                pub_dt = datetime.strptime(pub_str[:25], '%a, %d %b %Y %H:%M:%S')
                if abs((pub_dt - ev_dt).days) <= window_days:
                    results.append((title, pub_dt.strftime('%Y-%m-%d')))
            except Exception:
                continue
    except Exception:
        pass
    return results

# ── Main tagging loop ─────────────────────────
cache = load_cache()
# Patch: ensure new stat keys exist in older cache files
cache['stats'].setdefault('screener_hits', 0)
cache['stats'].setdefault('nse_hits', 0)

llm_start = cache['stats']['llm_calls']

total_events = 0
tagged_screener = 0
tagged_nse      = 0
tagged_rss      = 0
tagged_cache    = 0
still_unknown   = 0

section('STEP 0B — NEWS TAGGING')
print(f'\n  LLM provider  : {LLM_PROVIDER}')
print(f'  Cache loaded  : {len(cache["classified"])} previously classified')
print()

for ticker, d in ALL_DATA.items():
    company_name = d.get('stock_name', ticker)
    nse_ticker   = NSE_TICKER_MAP.get(ticker, ticker)
    events       = d.get('events', [])

    print(f'  {ticker:<16} {len(events)} events')

    # Fetch Layer 1: Screener earnings dates (cached after first fetch)
    earnings_dates = fetch_screener_earnings(nse_ticker, cache)
    print(f'    Screener     : {len(earnings_dates)} earnings dates found')

    # Fetch Layer 2: NSE announcements
    nse_announcements = fetch_nse_announcements(nse_ticker)
    print(f'    NSE ann.     : {len(nse_announcements)} announcements found')

    for event in events:
        total_events += 1

        # Skip already tagged
        if (event.get('cause_category') not in (None, 'UNKNOWN')
                and event.get('news_headline')):
            tagged_cache += 1
            continue

        event_date = event['date']

        # ── Layer 1: Screener earnings match ─────
        is_earn, earn_date = is_earnings_event(event_date, earnings_dates, window_days=14)
        if is_earn:
            event['cause_category'] = 'earnings'
            event['news_headline']  = f'Quarterly results announcement (Screener.in)'
            event['news_date']      = earn_date
            event['news_source']    = 'Screener.in'
            tagged_screener += 1
            cache['stats']['screener_hits'] += 1
            continue

        # ── Layer 2: NSE announcement match ──────
        nse_cat, nse_subject, nse_date = find_nse_announcement(
            event_date, nse_announcements
        )
        if nse_cat:
            event['cause_category'] = nse_cat
            event['news_headline']  = nse_subject
            event['news_date']      = nse_date
            event['news_source']    = 'NSE India'
            tagged_nse += 1
            cache['stats']['nse_hits'] += 1
            continue

        # ── Layer 3: Google RSS + cache/LLM ──────
        rss_headlines = fetch_google_rss(company_name, event_date)
        best_headline = None
        best_category = 'UNKNOWN'
        best_date     = None

        for headline, pub_date in rss_headlines:
            cat = classify_via_cache_or_llm(headline, cache)
            if cat != 'UNKNOWN':
                best_headline = headline
                best_category = cat
                best_date     = pub_date
                break

        event['cause_category'] = best_category
        event['news_headline']  = best_headline
        event['news_date']      = best_date
        event['news_source']    = 'Google News RSS' if best_headline else None

        if best_category != 'UNKNOWN':
            tagged_rss += 1
        else:
            still_unknown += 1

    time.sleep(0.5)  # polite delay between companies

save_cache(cache)

total_tagged = tagged_screener + tagged_nse + tagged_rss + tagged_cache
tag_rate     = round(total_tagged / total_events * 100) if total_events else 0
llm_calls    = cache['stats']['llm_calls'] - llm_start

print()
print(f'  {"-"*50}')
print(f'  Total events         : {total_events}')
print(f'  Tagged via Screener  : {tagged_screener}  (earnings, free)')
print(f'  Tagged via NSE       : {tagged_nse}  (announcements, free)')
print(f'  Tagged via RSS+LLM   : {tagged_rss}  (recent events)')
print(f'  Already cached       : {tagged_cache}')
print(f'  Still UNKNOWN        : {still_unknown}  (acceptable — likely noise)')
print(f'  Overall tag rate     : {tag_rate}%')
print(f'  LLM calls this run   : {llm_calls}')
print(f'  Cache saved to       : {CACHE_PATH.resolve()}')
print(f'\n  Proceed to Cell 4 to run the pipeline.')


# ─────────────────────────────────────────────
# CELL 4 — Full 8-Step Pipeline (all stocks)
# Runs Steps 1-7 for every stock in ALL_DATA.
# Stores results in RESULTS dict.
# ─────────────────────────────────────────────

# ── Signal evaluation ────────────────────────
def eval_signals(d):
    results = {}
    rsi = d.get("rsi_14")
    if rsi is None:       results["rsi"] = "NULL"
    elif rsi < 30:        results["rsi"] = "RED"
    elif rsi <= 55:       results["rsi"] = "GREEN"
    elif rsi <= 70:       results["rsi"] = "AMBER"
    else:                 results["rsi"] = "RED"

    p, ma50, ma200 = d.get("current_price"), d.get("ma_50"), d.get("ma_200")
    results["above50"]  = "NULL" if not p or not ma50  else ("GREEN" if p > ma50  else "RED")
    results["above200"] = "NULL" if not p or not ma200 else ("GREEN" if p > ma200 else "RED")

    v5, v30 = d.get("vol_5d_avg"), d.get("vol_30d_avg")
    results["volume"] = "NULL" if not v5 or not v30 else ("GREEN" if v5 > v30 else "RED")

    ed = d.get("earnings_days_away")
    results["earnings"] = "NULL" if ed is None else ("RED" if ed < 7 else ("AMBER" if ed <= 21 else "GREEN"))

    fii = d.get("fii_flow_10d_cr")
    results["fii"] = "NULL" if fii is None else ("GREEN" if fii > 0 else "RED")

    pp = d.get("promoter_pledging_change")
    results["pledging"] = {"no_change": "GREEN", "increase": "AMBER", "major_increase": "RED"}.get(pp, "NULL")

    crude, usdinr_t, usm = d.get("crude_trend"), d.get("usd_inr_trend"), d.get("us_market_trend")
    neg = sum([crude=="rising", usdinr_t=="depreciating", usm=="negative"])
    pos = sum([crude=="falling", usdinr_t=="appreciating", usm=="positive"])
    results["global"] = "NULL" if not any([crude, usdinr_t, usm]) else ("RED" if neg>=2 else ("GREEN" if pos>=2 else "AMBER"))

    sm = d.get("sector_momentum")
    results["sector"] = {"BUY": "GREEN", "WATCH": "AMBER", "AVOID": "RED"}.get(sm, "NULL")

    return results

# ── Similarity scoring ───────────────────────
def compute_similarity(cause_category, direction, sigs):
    """
    Score 0-100: how well current conditions match a historical event type.
    GREEN = full credit, AMBER = half credit, RED/NULL = 0.
    Works symmetrically for BULLISH and BEARISH patterns.
    """
    score = 0
    cat   = (cause_category or '').lower()

    def sig(key):
        v = sigs.get(key)
        if v == 'GREEN': return 1.0
        if v == 'AMBER': return 0.5
        return 0.0

    def bear(key):
        v = sigs.get(key)
        if v == 'RED':   return 1.0
        if v == 'AMBER': return 0.5
        return 0.0

    if direction == 'UP':
        score += sig('rsi')      * 10
        score += sig('above50')  * 8
        score += sig('above200') * 6
        score += sig('volume')   * 10
        score += sig('sector')   * 10
        score += sig('pledging') * 8
        score += sig('earnings') * 10
        score += sig('fii')      * 10
    else:
        score += bear('rsi')      * 10
        score += bear('above50')  * 8
        score += bear('above200') * 6
        score += bear('volume')   * 10
        score += bear('global')   * 12
        score += bear('fii')      * 10
        score += bear('earnings') * 6

    if cat == 'earnings':
        score += sig('earnings') * 15
    if cat == 'fii'              and sigs.get('fii')      != 'NULL': score += 8
    if cat == 'promoter'         and sigs.get('pledging') != 'NULL': score += 8
    if cat == 'macro'            and sigs.get('global')   != 'NULL': score += 8
    if cat == 'global'           and sigs.get('global')   == 'RED':  score += 10
    if cat == 'company-specific':                                      score += 8
    if cat == 'unknown':                                               score -= 15

    return min(100, max(0, round(score)))

def _is_insufficient(evs):
    """
    Minimum sample threshold.
    High-confidence sources (Screener, NSE): minimum 2 events.
    RSS/LLM tagged events: minimum 3 events.
    """
    high_conf = [e for e in evs
                 if e.get('news_source') in ('Screener.in', 'NSE India')]
    if len(high_conf) >= 2:
        return False
    return len(evs) < 3

def run_pattern_match(d, sigs):
    valid_events = [e for e in d["events"]
                    if e.get("cause_category") != "UNKNOWN"
                    and e.get("news_headline") is not None]
    by_cat = {}
    for e in valid_events:
        by_cat.setdefault(e["cause_category"], []).append(e)

    results = []
    for cat, evs in by_cat.items():
        recent = [e for e in evs if months_ago(e["date"]) <= 24]
        older  = [e for e in evs if months_ago(e["date"]) >  24]
        ws = ([(compute_similarity(cat, e["direction"], sigs), 2) for e in recent] +
              [(compute_similarity(cat, e["direction"], sigs), 1) for e in older])
        tw = sum(w for _, w in ws)
        score = round(sum(s*w for s,w in ws)/tw) if tw else 0
        avg_mag  = round(sum(abs(e["magnitude_pct"]) for e in evs)/len(evs), 1)
        hit_rate = round(sum(1 for e in evs if abs(e["magnitude_pct"])>=5)/len(evs)*100)
        ups      = sum(1 for e in evs if e["direction"]=="UP")
        bias     = "BULLISH" if ups > len(evs)/2 else "BEARISH" if ups < len(evs)/2 else "MIXED"
        results.append({"cat": cat, "score": score, "avg_mag": avg_mag,
                         "hit_rate": hit_rate, "bias": bias,
                         "insufficient": _is_insufficient(evs), "evs": evs})
    return results

# ── Jump detection ───────────────────────────
def run_jump_detection(d, sigs):
    jump_evs = [e for e in d["events"]
                if abs(e["magnitude_pct"]) >= JUMP_THRESHOLD
                and e.get("window_days", 99) <= 5]
    aligned = []
    for e in jump_evs:
        score = compute_similarity(e["cause_category"], e["direction"], sigs)
        w     = 2 if months_ago(e["date"]) <= 24 else 1
        wscore = min(100, round(score * (1 + 0.1*(w-1))))
        if wscore > 65:
            aligned.append({"event": e, "score": wscore})
    if len(aligned) >= 2:  status = "JUMP SETUP DETECTED"
    elif len(aligned) == 1: status = "JUMP WATCH"
    else:                   status = "NO JUMP SETUP"
    trigger = aligned[0]["event"]["cause_category"] if aligned else None
    return status, aligned, trigger

# ── Outlook ──────────────────────────────────
def run_outlook(d, pattern_results, jump_status, jump_trigger, signal_counts):
    ed     = d.get("earnings_days_away")
    vix    = d.get("india_vix")
    policy = d.get("policy_event_within_5d", False)
    blackouts = []
    if ed is not None and ed < 7:  blackouts.append(f"Earnings <7d")
    if vix is not None and vix > 22: blackouts.append(f"VIX>{vix}")
    if policy: blackouts.append("Policy event")

    if blackouts:
        return {"direction": "NEUTRAL", "probability": None, "magnitude": None,
                "trigger": None, "horizon": None, "confidence": "LOW",
                "reason": "Blackout: " + ", ".join(blackouts)}

    eligible = [p for p in pattern_results if not p["insufficient"] and p["score"] >= 35]
    if not eligible:
        return {"direction": "NEUTRAL", "probability": None, "magnitude": None,
                "trigger": None, "horizon": None, "confidence": "LOW",
                "reason": "No pattern match >= 50 with sufficient samples"}

    top       = max(eligible, key=lambda p: p["score"])
    direction = "BULLISH" if top["bias"]=="BULLISH" else "BEARISH" if top["bias"]=="BEARISH" else "NEUTRAL"
    prob      = min(80, round((top["score"]/100)*top["hit_rate"]))
    avg_mag   = top["avg_mag"]
    magnitude = "LOW (<5%)" if avg_mag < 5 else "MID (5-10%)" if avg_mag <= 10 else "HIGH (>10%)"
    trigger   = jump_trigger or top["cat"]
    horizon   = "weeks 1-2" if top["score"] >= 75 else "weeks 2-4"
    null_c    = signal_counts.get("NULL", 0)
    if   top["score"] >= 75 and null_c <= 1: confidence, conf_r = "HIGH",   "Strong similarity, few NULLs"
    elif top["score"] >= 35 and null_c <= 3: confidence, conf_r = "MEDIUM", "Partial match, some NULLs"
    else:                                     confidence, conf_r = "LOW",    f"{null_c} NULL signals"

    return {"direction": direction, "probability": prob, "magnitude": magnitude,
            "trigger": trigger, "horizon": horizon, "confidence": confidence, "reason": conf_r}

# ── Main pipeline loop ───────────────────────
RESULTS = {}

for ticker, d in ALL_DATA.items():
    section(f"STOCK: {ticker} — {d['stock_name']}")

    # Quality gate
    if d.get("data_quality_fail"):
        print(f"  DATA QUALITY FAIL: {d.get('fail_reason')}")
        print(f"  Analysis skipped.")
        RESULTS[ticker] = {"quality_pass": False, "data": d}
        continue

    # Step 1 — Overview
    lo, hi, cur = d.get("week52_low"), d.get("week52_high"), d.get("current_price")
    pct_from_low = round(((cur-lo)/(hi-lo))*100, 1) if lo and hi and hi!=lo else None
    print(f"  Price    : {fmt_inr(cur)}  |  52w: {fmt_inr(lo)} — {fmt_inr(hi)}  ({null_or(pct_from_low)}% from low)")
    print(f"  Mkt cap  : {fmt_mcap(d.get('market_cap_cr'))}  |  Sector: {d.get('sector')}  |  Momentum: {null_or(d.get('sector_momentum'))}")
    print(f"  History  : {d.get('history_days')} days  |  Events detected: {len(d.get('events', []))}")

    # Step 3 — Signals
    sigs   = eval_signals(d)
    counts = {s: sum(1 for v in sigs.values() if v==s) for s in ["GREEN","AMBER","RED","NULL"]}
    print(f"  Signals  : GREEN={counts['GREEN']} AMBER={counts['AMBER']} RED={counts['RED']} NULL={counts['NULL']}")
    print(f"  RSI={d.get('rsi_14')} | vs50MA={'above' if sigs['above50']=='GREEN' else 'below'} | vs200MA={'above' if sigs['above200']=='GREEN' else 'below'} | Vol={sigs['volume']}")

    # Steps 4-5 — Pattern + Jump
    patterns                    = run_pattern_match(d, sigs)
    jump_status, aligned, jtrig = run_jump_detection(d, sigs)
    eligible_patterns           = [p for p in patterns if not p["insufficient"] and p["score"] >= 35]
    top_pattern                 = max(eligible_patterns, key=lambda p: p["score"]) if eligible_patterns else None

    if top_pattern:
        print(f"  Pattern  : {top_pattern['cat']} — score {top_pattern['score']}/100 ({top_pattern['bias']})")
    else:
        print(f"  Pattern  : NO PATTERN MATCH")
    print(f"  Jump     : {jump_status}")

    # Step 6 — Outlook
    outlook = run_outlook(d, patterns, jump_status, jtrig, counts)

    # Step 7 — Auto-downgrade if LOW confidence
    if outlook["confidence"] == "LOW":
        outlook["direction"]   = "NEUTRAL"
        outlook["probability"] = None

    prob_str = f"{outlook['probability']}%" if outlook["probability"] else "NULL"
    print(f"  Outlook  : {outlook['direction']}  |  Prob: {prob_str}  |  Confidence: {outlook['confidence']}")
    if outlook.get("magnitude"):  print(f"  Magnitude: {outlook['magnitude']}  |  Horizon: {outlook.get('horizon')}  |  Trigger: {outlook.get('trigger')}")

    RESULTS[ticker] = {
        "quality_pass":  True,
        "data":          d,
        "sigs":          sigs,
        "signal_counts": counts,
        "patterns":      patterns,
        "jump_status":   jump_status,
        "jump_trigger":  jtrig,
        "notify":        jump_status == "JUMP SETUP DETECTED",
        "outlook":       outlook
    }

print(f"\nPipeline complete: {sum(1 for r in RESULTS.values() if r['quality_pass'])} stocks analysed successfully.")

# ─────────────────────────────────────────────
# CELL 5 — Save Validation Records
# Saves one record per stock to validation_log.json.
# Three checkpoints: day 5, 10, and 28.
# outcome fields filled on each checkpoint date.
# ─────────────────────────────────────────────

analysis_date = datetime.today().strftime("%Y-%m-%d")
today_dt      = datetime.today()

# Load existing log
if LOG_PATH.exists():
    with open(LOG_PATH, "r") as f:
        log = json.load(f)
else:
    log = []

new_records = []
for ticker, result in RESULTS.items():
    if not result["quality_pass"]:
        continue
    outlook = result["outlook"]
    record  = {
        "stock":           ticker,
        "analysis_date":   analysis_date,
        "direction":       outlook.get("direction"),
        "probability":     outlook.get("probability"),
        "jump_detected":   result["jump_status"] == "JUMP SETUP DETECTED",
        "key_trigger":     outlook.get("trigger"),
        "checkpoints": {
            "day_5":  {"date": (today_dt + timedelta(days=5)).strftime("%Y-%m-%d"),  "price": None, "outcome": None},
            "day_10": {"date": (today_dt + timedelta(days=10)).strftime("%Y-%m-%d"), "price": None, "outcome": None},
            "day_28": {"date": (today_dt + timedelta(days=28)).strftime("%Y-%m-%d"), "price": None, "outcome": None}
        },
        "notify":          result["notify"],
        "confidence":      outlook.get("confidence"),
        "model_version":   "2.0"
    }
    log.append(record)
    new_records.append(record)

with open(LOG_PATH, "w") as f:
    json.dump(log, f, indent=2)

section("CELL 5 — VALIDATION RECORDS SAVED")
print(f"\n  Records saved today  : {len(new_records)}")
print(f"  Total in log         : {len(log)}")
print(f"  Log file             : {LOG_PATH.resolve()}")
print()
print(f"  {'Stock':<16} {'Direction':<12} {'Prob':<8} {'Jump':<22} {'Notify'}")
print(f"  {'─'*16} {'─'*12} {'─'*8} {'─'*22} {'─'*6}")
for r in new_records:
    prob = f"{r['probability']}%" if r["probability"] else "NULL"
    print(f"  {r['stock']:<16} {r['direction']:<12} {prob:<8} {r['jump_detected'] and 'JUMP SETUP DETECTED' or 'No':<22} {r['notify']}")

print(f"\n  Checkpoint dates (from today {analysis_date}):")
for cp in CHECKPOINTS:
    print(f"    Day {cp:<3}: {(today_dt + timedelta(days=cp)).strftime('%Y-%m-%d')}")
if __name__ == "__main__":
    print(f"\n{'═'*62}")
    print(f"  Run complete: {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Validation log: {LOG_PATH.resolve()}")
    print(f"{'═'*62}")
    print("  This is a data-driven signal, not financial advice.")
    print("  All signals require human validation before acting.")
    print("  Investments are subject to market risk.")
