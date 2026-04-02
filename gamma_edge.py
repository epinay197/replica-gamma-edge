"""
GammaEdge GEX Dashboard -- Tradier Production (Tastytrade + Massive Fallback)
==============================================================================
Run:  python gamma_edge.py
Open: dashboard.html

Data sources:
  Primary:    Tradier (spot, expirations, full option chains with greeks)
  Fallback 1: Tastytrade / dxFeed (used automatically when Tradier fails)
  Fallback 2: Massive.com (used automatically when both Tradier and Tastytrade fail)
"""

import os, sys, json, time, math, threading, http.server, socketserver, traceback
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    print("\n[ERROR] 'requests' not installed.  Fix: pip install requests\n")
    sys.exit(1)

try:
    import websocket
except ImportError:
    print("\n[ERROR] 'websocket-client' not installed.  Fix: pip install websocket-client\n")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────
CREDS_FILE   = os.path.join(os.path.dirname(__file__), "credentials.env")
API_BASE     = os.environ.get("TRADIER_API_BASE", "https://sandbox.tradier.com/v1")
PORT         = int(os.environ.get("PORT", 5050))
MAX_EXPIRIES = 20
MAX_WORKERS  = 8
INDEX_ROOTS  = {"SPX","SPXW","NDX","NDXW","VIX","RUT","RUTW","XSP","MXEA","MXEF"}
HTML_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")

# Massive API constants
MASSIVE_API_BASE   = os.environ.get("MASSIVE_API_BASE", "https://api.massive.com")

# Tastytrade API constants
TT_API_BASE        = "https://api.tastytrade.com"
TT_DXFEED_WS       = "wss://tasty-openapi-ws.dxfeed.com/realtime"
TT_ACCESS_TTL      = 14 * 60        # 15 min expiry, refresh at 14 min
TT_STREAMER_TTL    = 20 * 3600      # streamer token valid ~24h, cache 20h

# ── Credentials ──────────────────────────────────────────────────
def load_creds():
    creds = {}
    if os.path.exists(CREDS_FILE):
        with open(CREDS_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"): continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    creds[k.strip()] = v.strip()
    return creds

def get_token():
    # Check environment variable first (used in cloud deployment)
    token = os.environ.get("TRADIER_TOKEN", "").strip()
    if not token:
        token = load_creds().get("TRADIER_TOKEN", "").strip()
    if not token or token.startswith("YOUR_TOKEN"):
        print("\n[WARN] No valid TRADIER_TOKEN found -- will rely on Tastytrade fallback.\n")
        return None
    return token

def get_massive_key():
    """Load Massive API key from env vars or credentials.env."""
    key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if not key:
        key = load_creds().get("MASSIVE_API_KEY", "").strip()
    return key or None

def get_default_ticker():
    return load_creds().get("TICKER","SPX").strip().upper()

# ── API helpers ──────────────────────────────────────────────────
def _hdr(token):
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def get_spot(token, symbol):
    """
    Get spot price from option chain data.
    Sandbox returns dummy prices for quotes/underlying fields,
    so we derive spot from the ATM strike cluster in the chain.
    """
    sym = symbol.upper()
    is_index = sym in INDEX_ROOTS or any(sym.startswith(r) for r in INDEX_ROOTS)

    # ── Equities: try quotes endpoint (works on sandbox) ─────────
    if not is_index:
        try:
            r = requests.get(f"{API_BASE}/markets/quotes",
                headers=_hdr(token),
                params={"symbols": sym, "greeks": "false"}, timeout=10)
            if r.status_code == 200:
                q = r.json().get("quotes", {}).get("quote")
                if q:
                    if isinstance(q, list): q = q[0]
                    price = _extract_price(q)
                    if price and price > 10:
                        print(f"  [spot] quotes/{sym} = ${price:,.2f}")
                        return price
        except Exception as e:
            print(f"  [spot] quotes exception: {e}")

    # ── All symbols: derive ATM from option chain strikes ─────────
    # The sandbox underlying field is unreliable (~117 for SPX).
    # Instead, we find the strike with highest total OI -- that cluster
    # sits right around the true current spot level.
    roots = ["SPXW", "SPX"] if sym in ("SPX","SPXW") else [sym]
    for root in roots:
        try:
            exps = get_expirations(token, root)
            if not exps:
                continue
            # Use nearest expiry -- has the most meaningful OI clustering
            r = requests.get(f"{API_BASE}/markets/options/chains",
                headers=_hdr(token),
                params={"symbol": root, "expiration": exps[0], "greeks": "true"},
                timeout=15)
            print(f"  [spot] chain/{root}/{exps[0]} -> {r.status_code}")
            if r.status_code != 200:
                continue
            opts  = r.json().get("options") or {}
            chain = opts.get("option", [])
            if isinstance(chain, dict): chain = [chain]
            if not chain:
                continue

            # Debug: print all fields of first contract once
            print(f"  [spot] {root} first contract keys: {list(chain[0].keys())}")
            print(f"  [spot] {root} first contract underlying={chain[0].get('underlying')} last={chain[0].get('last')} close={chain[0].get('close')}")

            # Method A: 'underlying' field -- Tradier puts current price here on production,
            # symbol string on sandbox. Try to parse it as a number.
            for c in chain:
                for field in ("underlying", "underlying_price"):
                    val = c.get(field)
                    if val is not None:
                        try:
                            p = float(val)
                            if 500 < p < 100000:   # must look like a real index price
                                print(f"  [spot] {root} {field}={p:,.2f} OK")
                                return p
                            else:
                                print(f"  [spot] {root} {field}={val} (not a valid price, skipping)")
                                break  # same value for all contracts, no need to loop
                        except (ValueError, TypeError):
                            print(f"  [spot] {root} {field}='{val}' (string, not a price)")
                            break  # same value for all contracts, no need to loop

            # Method B: weighted average of strikes by OI -- clusters around spot
            oi_by_strike = {}
            for c in chain:
                st = c.get("strike")
                oi = c.get("open_interest") or 0
                if st and oi:
                    oi_by_strike[float(st)] = oi_by_strike.get(float(st), 0) + oi

            if oi_by_strike:
                total_oi = sum(oi_by_strike.values())
                if total_oi > 0:
                    # Weighted average strike by OI -- good approximation of spot
                    wt_avg = sum(k * v for k, v in oi_by_strike.items()) / total_oi
                    if wt_avg > 500:
                        print(f"  [spot] {root} OI-weighted avg strike = ${wt_avg:,.2f}")
                        return round(wt_avg, 2)
                # Fallback: peak OI strike
                peak = max(oi_by_strike, key=oi_by_strike.get)
                if peak > 500:
                    print(f"  [spot] {root} peak OI strike = ${peak:,.2f}")
                    return peak

        except Exception as e:
            print(f"  [spot] chain/{root} exception: {e}")

    return None

def _extract_price(q):
    try:
        bid  = q.get("bid");  ask  = q.get("ask")
        last = q.get("last") or q.get("prevclose") or q.get("close")
        if bid and ask and float(bid)>0 and float(ask)>0:
            return (float(bid)+float(ask))/2
        if last and float(last)>0: return float(last)
    except: pass
    return None

def get_expirations(token, symbol):
    r = requests.get(f"{API_BASE}/markets/options/expirations",
        headers=_hdr(token),
        params={"symbol": symbol, "includeAllRoots": "true"},
        timeout=15)
    if r.status_code != 200:
        print(f"  [exp] HTTP {r.status_code}: {r.text[:200]}")
        r.raise_for_status()
    data = r.json().get("expirations") or {}
    dates = data.get("date", [])
    if isinstance(dates, str): dates = [dates]
    today = date.today().isoformat()
    return sorted(d for d in dates if d >= today)

def get_chain(token, symbol, expiry):
    try:
        r = requests.get(f"{API_BASE}/markets/options/chains",
            headers=_hdr(token),
            params={"symbol": symbol, "expiration": expiry, "greeks": "true"},
            timeout=20)
        if r.status_code != 200:
            print(f"  [chain] {symbol} {expiry} -> HTTP {r.status_code}")
            return []
        opts = r.json().get("options") or {}
        chain = opts.get("option", [])
        if chain is None: return []
        return [chain] if isinstance(chain, dict) else chain
    except Exception as e:
        print(f"  [chain] {symbol} {expiry} -> {e}")
        return []


# ══════════════════════════════════════════════════════════════════
# TASTYTRADE FALLBACK
# ══════════════════════════════════════════════════════════════════

# Token cache (module-level, thread-safe via lock)
_tt_lock           = threading.Lock()
_tt_access_token   = None
_tt_access_ts      = 0.0
_tt_streamer_token = None
_tt_streamer_ts    = 0.0


def _get_tt_creds():
    """Load Tastytrade credentials from env vars or credentials.env."""
    creds = load_creds()
    client_id     = os.environ.get("TT_CLIENT_ID",     creds.get("TT_CLIENT_ID",     "")).strip()
    client_secret = os.environ.get("TT_CLIENT_SECRET", creds.get("TT_CLIENT_SECRET", "")).strip()
    refresh_token = os.environ.get("TT_REFRESH_TOKEN", creds.get("TT_REFRESH_TOKEN", "")).strip()
    return client_id, client_secret, refresh_token


def _tt_get_access_token():
    """Return a valid Tastytrade OAuth access token, refreshing if expired."""
    global _tt_access_token, _tt_access_ts
    with _tt_lock:
        now = time.time()
        if _tt_access_token and (now - _tt_access_ts) < TT_ACCESS_TTL:
            return _tt_access_token

        client_id, client_secret, refresh_token = _get_tt_creds()
        if not (client_id and client_secret and refresh_token):
            raise RuntimeError("[TT] Tastytrade credentials not configured")

        resp = requests.post(
            f"{TT_API_BASE}/oauth/token",
            data={
                "grant_type":    "refresh_token",
                "refresh_token": refresh_token,
                "client_id":     client_id,
                "client_secret": client_secret,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"[TT] OAuth token refresh failed: {resp.status_code} {resp.text[:300]}")

        _tt_access_token = resp.json()["access_token"]
        _tt_access_ts    = now
        print("[TT] OAuth access token refreshed OK")
        return _tt_access_token


def _tt_get_streamer_token():
    """Return a valid dxFeed streamer token, refreshing if expired."""
    global _tt_streamer_token, _tt_streamer_ts
    with _tt_lock:
        now = time.time()
        if _tt_streamer_token and (now - _tt_streamer_ts) < TT_STREAMER_TTL:
            return _tt_streamer_token

        access = _tt_get_access_token()
        resp = requests.get(
            f"{TT_API_BASE}/api-quote-tokens",
            headers={"Authorization": f"Bearer {access}", "Accept": "application/json"},
            timeout=15,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"[TT] Streamer token fetch failed: {resp.status_code} {resp.text[:300]}")

        _tt_streamer_token = resp.json()["data"]["token"]
        _tt_streamer_ts    = now
        print("[TT] dxFeed streamer token refreshed OK")
        return _tt_streamer_token


# ── dxFeed WebSocket helpers ──────────────────────────────────────

def _dxfeed_fetch(streamer_token, subscriptions, timeout=20):
    """
    Open a dxFeed WebSocket, subscribe to `subscriptions`, collect all
    messages until no new data arrives for `timeout` seconds, then return
    a dict keyed by event-type -> symbol -> {field: value}.

    subscriptions: list of {"type": "Greeks", "symbol": [".SPXW241220C4500"]}
    """
    collected = {}   # {event_type: {symbol: {field: value}}}
    done      = threading.Event()
    last_msg  = [time.time()]

    def on_message(ws, message):
        last_msg[0] = time.time()
        try:
            data = json.loads(message)
        except Exception:
            return

        # dxFeed feed messages look like:
        # {"type":"Greeks","eventSymbol":".SPXW241220C4500","gamma":0.001,...}
        # or wrapped in {"channel":1,"type":"DATA","data":[...]}
        events = []
        if isinstance(data, list):
            events = data
        elif isinstance(data, dict):
            if data.get("type") == "DATA" and isinstance(data.get("data"), list):
                events = data["data"]
            else:
                events = [data]

        for ev in events:
            if not isinstance(ev, dict): continue
            etype  = ev.get("eventType") or ev.get("type")
            symbol = ev.get("eventSymbol") or ev.get("symbol")
            if not etype or not symbol: continue
            if etype not in collected:
                collected[etype] = {}
            if symbol not in collected[etype]:
                collected[etype][symbol] = {}
            collected[etype][symbol].update(ev)

    def on_error(ws, error):
        print(f"[TT/dxFeed] WS error: {error}")

    def on_close(ws, code, msg):
        done.set()

    def on_open(ws):
        # dxFeed FEED setup sequence
        # 1. SETUP
        ws.send(json.dumps({"type": "SETUP", "channel": 0, "keepaliveTimeout": 60,
                             "acceptKeepaliveTimeout": 60, "version": "0.1"}))
        # 2. AUTH
        ws.send(json.dumps({"type": "AUTH", "channel": 0, "token": streamer_token}))
        # 3. Open a data channel
        ws.send(json.dumps({"type": "CHANNEL_REQUEST", "channel": 1,
                             "service": "FEED", "parameters": {"contract": "AUTO"}}))
        # 4. FEED_SETUP on channel 1
        feed_setup = {
            "type":         "FEED_SETUP",
            "channel":      1,
            "acceptAggregationPeriod": 0,
            "acceptDataFormat":        "FULL",
            "acceptEventFields":       {
                "Greeks":  ["eventType","eventSymbol","volatility","delta","gamma",
                            "theta","rho","vega"],
                "Summary": ["eventType","eventSymbol","openInterest","dayOpenInterest"],
                "Trade":   ["eventType","eventSymbol","price","dayVolume"],
                "Quote":   ["eventType","eventSymbol","bidPrice","askPrice"],
            }
        }
        ws.send(json.dumps(feed_setup))
        # 5. FEED_SUBSCRIPTION
        ws.send(json.dumps({
            "type":          "FEED_SUBSCRIPTION",
            "channel":       1,
            "reset":         True,
            "add":           subscriptions,
        }))

    ws_app = websocket.WebSocketApp(
        TT_DXFEED_WS,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    t = threading.Thread(target=ws_app.run_forever, daemon=True)
    t.start()

    # Wait until silence for `timeout` seconds
    deadline = time.time() + timeout + 5   # hard outer cap
    while time.time() < deadline:
        time.sleep(0.5)
        if done.is_set():
            break
        if (time.time() - last_msg[0]) >= timeout:
            break

    ws_app.close()
    return collected


# ── Tastytrade public data functions ──────────────────────────────

def get_spot_tastytrade(symbol):
    """
    Fetch spot price for `symbol` via dxFeed WebSocket (Trade/Quote event).
    Returns float or None.
    """
    sym = symbol.upper()
    # Map option-root symbols to their underlying dxFeed ticker
    dx_map = {
        "SPX": "$SPX.X", "SPXW": "$SPX.X",
        "NDX": "$NDX.X", "NDXW": "$NDX.X",
        "RUT": "$RUT.X", "RUTW": "$RUT.X",
        "VIX": "$VIX.X",
    }
    dx_sym = dx_map.get(sym, sym)

    print(f"  [TT/spot] Fetching via dxFeed: {dx_sym}")
    try:
        streamer = _tt_get_streamer_token()
        subs = [
            {"type": "Trade", "symbol": [dx_sym]},
            {"type": "Quote", "symbol": [dx_sym]},
        ]
        data = _dxfeed_fetch(streamer, subs, timeout=10)

        # Prefer Trade.price, fallback to Quote mid
        trade = (data.get("Trade") or {}).get(dx_sym, {})
        quote = (data.get("Quote") or {}).get(dx_sym, {})

        price = None
        t_price = trade.get("price")
        if t_price and float(t_price) > 10:
            price = float(t_price)
        else:
            bid = quote.get("bidPrice")
            ask = quote.get("askPrice")
            if bid and ask and float(bid) > 0 and float(ask) > 0:
                price = (float(bid) + float(ask)) / 2

        if price:
            print(f"  [TT/spot] {dx_sym} = ${price:,.2f}")
        else:
            print(f"  [TT/spot] No price received for {dx_sym}")
        return price

    except Exception as e:
        print(f"  [TT/spot] Exception: {e}")
        return None


def get_expirations_tastytrade(symbol):
    """
    Fetch option expirations for `symbol` via Tastytrade REST API.
    Returns sorted list of 'YYYY-MM-DD' strings >= today.
    """
    sym = symbol.upper()
    print(f"  [TT/exp] Fetching expirations for {sym}")
    try:
        access = _tt_get_access_token()
        # Tastytrade uses '/SPXW' prefix for index options
        tt_sym = f"/{sym}" if sym in INDEX_ROOTS else sym

        resp = requests.get(
            f"{TT_API_BASE}/instruments/equity-options",
            headers={"Authorization": f"Bearer {access}", "Accept": "application/json"},
            params={"symbol[]": tt_sym, "active": "true"},
            timeout=20,
        )
        if resp.status_code != 200:
            print(f"  [TT/exp] HTTP {resp.status_code}: {resp.text[:200]}")
            return []

        items = resp.json().get("data", {}).get("items", [])
        today = date.today().isoformat()
        exps  = set()
        for item in items:
            exp = item.get("expiration-date")
            if exp and exp >= today:
                exps.add(exp)

        result = sorted(exps)
        print(f"  [TT/exp] Found {len(result)} expirations for {sym}")
        return result

    except Exception as e:
        print(f"  [TT/exp] Exception: {e}")
        return []


def _build_option_symbol_dxfeed(root, expiry, otype, strike):
    """
    Build a dxFeed option symbol string.
    Format: .{ROOT}{YY}{MM}{DD}{C|P}{strike_integer}
    Example: .SPXW241220C4500
    """
    dt   = datetime.strptime(expiry, "%Y-%m-%d")
    yy   = dt.strftime("%y")
    mm   = dt.strftime("%m")
    dd   = dt.strftime("%d")
    cp   = "C" if otype.lower() == "call" else "P"
    # Strike: no decimal if whole number, else include decimal
    s    = float(strike)
    s_str = str(int(s)) if s == int(s) else str(s)
    return f".{root}{yy}{mm}{dd}{cp}{s_str}"


def get_chain_tastytrade(symbol, expiry):
    """
    Fetch a full option chain for (symbol, expiry) via Tastytrade REST for
    contract metadata + dxFeed WebSocket for greeks and OI.

    Returns a list of contract dicts in the same format expected by compute_gex():
      {
        "strike":          float,
        "option_type":     "call" | "put",
        "expiration_date": "YYYY-MM-DD",
        "contract_size":   100,
        "open_interest":   int,
        "greeks": {
            "gamma":  float,
            "delta":  float,
            "mid_iv": float,
        }
      }
    """
    sym = symbol.upper()
    tt_sym = f"/{sym}" if sym in INDEX_ROOTS else sym

    print(f"  [TT/chain] Fetching {sym} {expiry}")
    try:
        access = _tt_get_access_token()

        # Step 1: Get option symbols for this expiry from REST
        resp = requests.get(
            f"{TT_API_BASE}/instruments/equity-options",
            headers={"Authorization": f"Bearer {access}", "Accept": "application/json"},
            params={"symbol[]": tt_sym, "expiration-date": expiry, "active": "true"},
            timeout=20,
        )
        if resp.status_code != 200:
            print(f"  [TT/chain] REST {resp.status_code}: {resp.text[:200]}")
            return []

        items = resp.json().get("data", {}).get("items", [])
        if not items:
            print(f"  [TT/chain] No contracts from REST for {sym} {expiry}")
            return []

        # Build map: dxfeed_symbol -> contract_info
        contracts_meta = {}
        dx_symbols     = []
        for item in items:
            dx_sym = item.get("streamer-symbol")
            if not dx_sym:
                # Build it manually as fallback
                strike = item.get("strike-price") or item.get("strike")
                otype  = item.get("option-type", "").lower()
                if strike and otype in ("call", "put"):
                    root_key = sym.replace("/", "")
                    dx_sym = _build_option_symbol_dxfeed(root_key, expiry, otype, strike)
                else:
                    continue

            strike = float(item.get("strike-price") or item.get("strike") or 0)
            otype  = (item.get("option-type") or "").lower()
            if not strike or otype not in ("call", "put"):
                continue

            contracts_meta[dx_sym] = {
                "strike":          strike,
                "option_type":     otype,
                "expiration_date": expiry,
                "contract_size":   100,
            }
            dx_symbols.append(dx_sym)

        if not dx_symbols:
            print(f"  [TT/chain] No valid dxFeed symbols for {sym} {expiry}")
            return []

        print(f"  [TT/chain] Subscribing to {len(dx_symbols)} dxFeed symbols for {sym} {expiry}")

        # Step 2: Fetch Greeks + Summary + Trade via dxFeed WebSocket
        # Batch into chunks to avoid oversized WS messages
        CHUNK = 200
        all_data = {"Greeks": {}, "Summary": {}, "Trade": {}}
        streamer = _tt_get_streamer_token()

        for i in range(0, len(dx_symbols), CHUNK):
            chunk = dx_symbols[i:i+CHUNK]
            subs  = [
                {"type": "Greeks",  "symbol": chunk},
                {"type": "Summary", "symbol": chunk},
                {"type": "Trade",   "symbol": chunk},
            ]
            chunk_data = _dxfeed_fetch(streamer, subs, timeout=12)
            for etype in ("Greeks", "Summary", "Trade"):
                all_data[etype].update(chunk_data.get(etype, {}))

        # Step 3: Assemble contracts
        result = []
        for dx_sym, meta in contracts_meta.items():
            greeks_ev  = all_data["Greeks"].get(dx_sym,  {})
            summary_ev = all_data["Summary"].get(dx_sym, {})
            trade_ev   = all_data["Trade"].get(dx_sym,   {})

            # Open interest: Summary.openInterest or Summary.dayOpenInterest
            oi = 0
            for field in ("openInterest", "dayOpenInterest"):
                v = summary_ev.get(field)
                if v is not None:
                    try:
                        oi = int(float(v))
                        if oi > 0: break
                    except (ValueError, TypeError):
                        pass

            # Volume from Trade
            vol = 0
            tv = trade_ev.get("dayVolume")
            if tv is not None:
                try: vol = int(float(tv))
                except (ValueError, TypeError): pass

            gamma = None
            delta = None
            iv    = None
            for field, target in (("gamma","gamma"), ("delta","delta"), ("volatility","iv")):
                v = greeks_ev.get(field)
                if v is not None:
                    try:
                        fv = float(v)
                        if target == "gamma": gamma = fv
                        elif target == "delta": delta = fv
                        elif target == "iv": iv = fv
                    except (ValueError, TypeError):
                        pass

            result.append({
                "strike":          meta["strike"],
                "option_type":     meta["option_type"],
                "expiration_date": meta["expiration_date"],
                "contract_size":   meta["contract_size"],
                "open_interest":   oi,
                "volume":          vol,
                "greeks": {
                    "gamma":   gamma,
                    "delta":   delta,
                    "mid_iv":  iv,
                },
            })

        filled = sum(1 for c in result if c["greeks"]["gamma"] is not None)
        print(f"  [TT/chain] {sym} {expiry}: {len(result)} contracts, "
              f"{filled} with greeks, total OI={sum(c['open_interest'] for c in result):,}")
        return result

    except Exception as e:
        traceback.print_exc()
        print(f"  [TT/chain] Exception for {sym} {expiry}: {e}")
        return []


# ── Tastytrade full fetch (spot + expirations + chains) ───────────

def fetch_gex_tastytrade(ticker):
    """
    Full GEX data fetch using Tastytrade as the data source.
    Returns the same structure as fetch_gex() so the rest of the pipeline
    (compute_gex, key_levels, HTTP handler) works unchanged.
    """
    ticker = ticker.upper().strip()
    t0     = time.time()
    print(f"\n{'─'*55}")
    print(f"[GammaEdge/TT] Fetching {ticker} via Tastytrade...")
    print(f"{'─'*55}")

    # Spot price
    spot = get_spot_tastytrade(ticker)
    if spot is None:
        return {"error": f"[TT] Cannot fetch spot price for {ticker}"}
    print(f"  Spot: ${spot:,.2f}")

    # Expirations
    exps = get_expirations_tastytrade(ticker)
    if not exps and ticker == "SPX":
        print("  [TT/exp] No SPX exps, trying SPXW...")
        exps = get_expirations_tastytrade("SPXW")
    if not exps:
        return {"error": f"[TT] No expirations found for {ticker}"}
    print(f"  Expirations: {len(exps)} -- first: {exps[0]}, last: {exps[-1]}")

    exps = exps[:MAX_EXPIRIES]

    # Option chains
    all_contracts = []
    syms = [ticker] + (["SPXW"] if ticker == "SPX" else [])
    print(f"  Fetching chains for: {syms} x {len(exps)} expirations...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(get_chain_tastytrade, sym, exp): (sym, exp)
                for sym in syms for exp in exps}
        for f in as_completed(futs):
            all_contracts.extend(f.result())

    print(f"  Raw contracts returned: {len(all_contracts):,}")

    if not all_contracts:
        return {"error": "[TT] No option contracts returned from Tastytrade"}

    # GEX computation (unchanged -- same contract format)
    strikes = compute_gex(all_contracts, spot, ticker)
    kl      = key_levels(strikes, spot)

    # Summary by expiry
    exp_summary = {}
    for s in strikes:
        for exp, gex in s["expirations"].items():
            if exp not in exp_summary:
                exp_summary[exp] = {"gex": 0, "dte": dte(exp)}
            exp_summary[exp]["gex"] += gex

    elapsed = round(time.time() - t0, 1)
    print(f"  Done in {elapsed}s -- {len(strikes)} strikes, "
          f"call_wall={kl.get('call_wall')}, put_wall={kl.get('put_wall')}, "
          f"total_gex={kl.get('total_gex')}")

    return {
        "ticker":      ticker,
        "spot":        spot,
        "strikes":     strikes,
        "key_levels":  kl,
        "expirations": {k: {"gex": round(v["gex"], 3), "dte": v["dte"]}
                        for k, v in sorted(exp_summary.items())},
        "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed":     elapsed,
        "data_source": "tastytrade",
    }


# ══════════════════════════════════════════════════════════════════
# MASSIVE.COM FALLBACK (third tier)
# ══════════════════════════════════════════════════════════════════

def _massive_hdr():
    """Return auth header for Massive API."""
    key = get_massive_key()
    if not key:
        raise RuntimeError("[Massive] No MASSIVE_API_KEY configured")
    return {"Authorization": f"Bearer {key}", "Accept": "application/json"}


def get_spot_massive(ticker):
    """
    Fetch spot price for `ticker` via Massive last-trade endpoint.
    GET /v2/last/trade/{ticker} -> results.p
    Returns float or None.
    """
    sym = ticker.upper()
    print(f"  [Massive/spot] Fetching last trade for {sym}")
    try:
        r = requests.get(
            f"{MASSIVE_API_BASE}/v2/last/trade/{sym}",
            headers=_massive_hdr(),
            timeout=15,
        )
        if r.status_code != 200:
            print(f"  [Massive/spot] HTTP {r.status_code}: {r.text[:200]}")
            return None
        data = r.json()
        results = data.get("results", {})
        price = results.get("p")
        if price is not None:
            price = float(price)
            if price > 0:
                print(f"  [Massive/spot] {sym} = ${price:,.2f}")
                return price
        print(f"  [Massive/spot] No valid price in response for {sym}")
        return None
    except Exception as e:
        print(f"  [Massive/spot] Exception: {e}")
        return None


def get_chain_massive(ticker, expiration):
    """
    Fetch options chain snapshot for (ticker, expiration) via Massive API.
    GET /v3/snapshot/options/{ticker}?expiration_date=YYYY-MM-DD&limit=250
    Follows next_url for pagination.

    Returns list of contract dicts in the same format expected by compute_gex():
      {
        "strike":          float,
        "option_type":     "call" | "put",
        "expiration_date": "YYYY-MM-DD",
        "contract_size":   100,
        "open_interest":   int,
        "greeks": {
            "gamma":  float,
            "delta":  float,
            "mid_iv": float,
        }
      }
    """
    sym = ticker.upper()
    print(f"  [Massive/chain] Fetching {sym} {expiration}")
    try:
        headers = _massive_hdr()
        url = f"{MASSIVE_API_BASE}/v3/snapshot/options/{sym}"
        params = {"expiration_date": expiration, "limit": 250}
        all_results = []

        while url:
            r = requests.get(url, headers=headers, params=params, timeout=20)
            if r.status_code != 200:
                print(f"  [Massive/chain] HTTP {r.status_code}: {r.text[:200]}")
                break
            data = r.json()
            results = data.get("results", [])
            all_results.extend(results)

            # Pagination: follow next_url if present
            next_url = data.get("next_url")
            if next_url:
                url = next_url
                params = {}  # next_url includes query params already
            else:
                url = None

        # Convert to standard contract format
        contracts = []
        for item in all_results:
            details = item.get("details", {})
            greeks  = item.get("greeks", {})

            strike     = details.get("strike_price")
            exp_date   = details.get("expiration_date")
            ctype_raw  = (details.get("contract_type") or "").lower()
            oi         = item.get("open_interest") or 0
            iv         = item.get("implied_volatility")

            if not strike or ctype_raw not in ("call", "put"):
                continue

            gamma_val = greeks.get("gamma")
            delta_val = greeks.get("delta")

            contracts.append({
                "strike":          float(strike),
                "option_type":     ctype_raw,
                "expiration_date": exp_date or expiration,
                "contract_size":   100,
                "open_interest":   int(oi),
                "greeks": {
                    "gamma":  float(gamma_val) if gamma_val is not None else None,
                    "delta":  float(delta_val) if delta_val is not None else None,
                    "mid_iv": float(iv) if iv is not None else None,
                },
            })

        filled = sum(1 for c in contracts if c["greeks"]["gamma"] is not None)
        print(f"  [Massive/chain] {sym} {expiration}: {len(contracts)} contracts, "
              f"{filled} with greeks, total OI={sum(c['open_interest'] for c in contracts):,}")
        return contracts

    except Exception as e:
        traceback.print_exc()
        print(f"  [Massive/chain] Exception for {sym} {expiration}: {e}")
        return []


def _get_expirations_massive(ticker):
    """
    Derive available expirations from a Massive options snapshot.
    We fetch a broad snapshot (no expiration filter) and collect unique expiration dates.
    Returns sorted list of 'YYYY-MM-DD' strings >= today.
    """
    sym = ticker.upper()
    print(f"  [Massive/exp] Fetching expirations for {sym}")
    try:
        headers = _massive_hdr()
        url = f"{MASSIVE_API_BASE}/v3/snapshot/options/{sym}"
        params = {"limit": 250}
        exps = set()

        # Fetch first page(s) to discover expirations
        pages = 0
        while url and pages < 5:
            r = requests.get(url, headers=headers, params=params, timeout=20)
            if r.status_code != 200:
                print(f"  [Massive/exp] HTTP {r.status_code}: {r.text[:200]}")
                break
            data = r.json()
            for item in data.get("results", []):
                details = item.get("details", {})
                exp = details.get("expiration_date")
                if exp:
                    exps.add(exp)
            next_url = data.get("next_url")
            if next_url:
                url = next_url
                params = {}
            else:
                url = None
            pages += 1

        today = date.today().isoformat()
        result = sorted(e for e in exps if e >= today)
        print(f"  [Massive/exp] Found {len(result)} expirations for {sym}")
        return result

    except Exception as e:
        print(f"  [Massive/exp] Exception: {e}")
        return []


def fetch_gex_massive(ticker):
    """
    Full GEX data fetch using Massive.com as the data source.
    Returns the same structure as fetch_gex() so the rest of the pipeline
    (compute_gex, key_levels, HTTP handler) works unchanged.
    """
    ticker = ticker.upper().strip()
    t0     = time.time()
    print(f"\n{'='*55}")
    print(f"[GammaEdge/Massive] Fetching {ticker} via Massive.com...")
    print(f"{'='*55}")

    # Spot price
    spot = get_spot_massive(ticker)
    if spot is None:
        return {"error": f"[Massive] Cannot fetch spot price for {ticker}"}
    print(f"  Spot: ${spot:,.2f}")

    # Expirations
    exps = _get_expirations_massive(ticker)
    if not exps and ticker == "SPX":
        print("  [Massive/exp] No SPX exps, trying SPXW...")
        exps = _get_expirations_massive("SPXW")
    if not exps:
        return {"error": f"[Massive] No expirations found for {ticker}"}
    print(f"  Expirations: {len(exps)} -- first: {exps[0]}, last: {exps[-1]}")

    exps = exps[:MAX_EXPIRIES]

    # Option chains
    all_contracts = []
    syms = [ticker] + (["SPXW"] if ticker == "SPX" else [])
    print(f"  Fetching chains for: {syms} x {len(exps)} expirations...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(get_chain_massive, sym, exp): (sym, exp)
                for sym in syms for exp in exps}
        for f in as_completed(futs):
            all_contracts.extend(f.result())

    print(f"  Raw contracts returned: {len(all_contracts):,}")

    if not all_contracts:
        return {"error": "[Massive] No option contracts returned from Massive"}

    # GEX computation (unchanged -- same contract format)
    strikes = compute_gex(all_contracts, spot, ticker)
    kl      = key_levels(strikes, spot)

    # Summary by expiry
    exp_summary = {}
    for s in strikes:
        for exp, gex in s["expirations"].items():
            if exp not in exp_summary:
                exp_summary[exp] = {"gex": 0, "dte": dte(exp)}
            exp_summary[exp]["gex"] += gex

    elapsed = round(time.time() - t0, 1)
    print(f"  Done in {elapsed}s -- {len(strikes)} strikes, "
          f"call_wall={kl.get('call_wall')}, put_wall={kl.get('put_wall')}, "
          f"total_gex={kl.get('total_gex')}")

    return {
        "ticker":      ticker,
        "spot":        spot,
        "strikes":     strikes,
        "key_levels":  kl,
        "expirations": {k: {"gex": round(v["gex"], 3), "dte": v["dte"]}
                        for k, v in sorted(exp_summary.items())},
        "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed":     elapsed,
        "data_source": "massive",
    }


# ── Black-Scholes gamma ──────────────────────────────────────────
def _npdf(x): return math.exp(-0.5*x*x) / math.sqrt(2*math.pi)

def bs_gamma(S, K, T, r=0.045, sigma=0.20):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0: return 0.0
    try:
        d1 = (math.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
        return _npdf(d1) / (S * sigma * math.sqrt(T))
    except: return 0.0

# ── GEX computation ──────────────────────────────────────────────
def dte(expiry_str):
    return (date.fromisoformat(expiry_str) - date.today()).days

def compute_gex(contracts, spot, ticker):
    is_index = ticker.upper() in INDEX_ROOTS or any(
        ticker.upper().startswith(r) for r in INDEX_ROOTS)
    sm = {}
    skipped_no_oi = 0
    skipped_no_type = 0
    bs_used = 0
    real_gamma = 0

    for c in contracts:
        strike = c.get("strike")
        oi     = c.get("open_interest") or 0

        # Normalise option_type: Tradier uses "call"/"put"
        raw = (c.get("option_type") or "").strip().lower()
        if raw in ("call", "c"):
            otype = "call"
        elif raw in ("put", "p"):
            otype = "put"
        else:
            skipped_no_type += 1
            continue

        if not strike or oi == 0:
            skipped_no_oi += 1
            continue

        expiry = c.get("expiration_date", "")
        size   = int(c.get("contract_size") or 100)
        g      = c.get("greeks") or {}
        gamma  = g.get("gamma")
        iv     = g.get("mid_iv") or g.get("smv_vol") or g.get("ask_iv") or g.get("bid_iv")

        if gamma is None or gamma == 0:
            T = max(dte(expiry), 0.5) / 365.0
            sigma = float(iv) if iv else 0.20
            gamma = bs_gamma(spot, float(strike), T, 0.045, sigma)
            bs_used += 1
        else:
            real_gamma += 1

        gamma = abs(float(gamma))
        raw_gex = gamma * oi * (spot ** 2) * size / 1e9

        # Index: dealers SHORT gamma on both calls and puts -> negative GEX
        # Equity/ETF: dealers long calls (positive), short puts (negative)
        if is_index:
            call_contrib = -raw_gex if otype == "call" else 0
            put_contrib  = -raw_gex if otype == "put"  else 0
        else:
            call_contrib = +raw_gex if otype == "call" else 0
            put_contrib  = -raw_gex if otype == "put"  else 0

        signed = call_contrib + put_contrib
        delta  = float(g.get("delta") or 0)

        if strike not in sm:
            sm[strike] = {
                "strike": strike,
                "call_gex": 0., "put_gex": 0.,
                "call_oi": 0,   "put_oi": 0,
                "call_delta": 0., "put_delta": 0.,
                "exps_call": {}, "exps_put": {}
            }
        d = sm[strike]
        if otype == "call":
            d["call_gex"]   += call_contrib
            d["call_oi"]    += oi
            d["call_delta"] += delta * oi * size
            d["exps_call"][expiry] = d["exps_call"].get(expiry, 0) + call_contrib
        else:
            d["put_gex"]    += put_contrib
            d["put_oi"]     += oi
            d["put_delta"]  += delta * oi * size
            d["exps_put"][expiry] = d["exps_put"].get(expiry, 0) + put_contrib

    print(f"  [gex] contracts processed: {len(contracts)}, "
          f"real_gamma={real_gamma}, bs_fallback={bs_used}, "
          f"skipped_no_oi={skipped_no_oi}, skipped_no_type={skipped_no_type}")

    result = []
    for s, d in sorted(sm.items()):
        all_exps = set(list(d["exps_call"]) + list(d["exps_put"]))
        exps_combined = {
            e: round(d["exps_call"].get(e, 0) + d["exps_put"].get(e, 0), 4)
            for e in all_exps
        }
        result.append({
            "strike":      s,
            "net_gex":     round(d["call_gex"] + d["put_gex"], 4),
            "call_gex":    round(d["call_gex"], 4),
            "put_gex":     round(d["put_gex"],  4),
            "net_delta":   round(d["call_delta"] + d["put_delta"], 0),
            "net_oi":      d["call_oi"] + d["put_oi"],
            "call_oi":     d["call_oi"],
            "put_oi":      d["put_oi"],
            "pct_spot":    round((s / spot - 1) * 100, 2) if spot else 0,
            "expirations": exps_combined,
        })
    return result

def key_levels(strikes, spot):
    if not strikes: return {}
    near = [s for s in strikes if abs(s["strike"] / spot - 1) <= 0.20]
    if not near: near = strikes
    call_wall = max(near, key=lambda x: x["call_gex"])["strike"]
    put_wall  = min(near, key=lambda x: x["put_gex"])["strike"]
    max_pain  = max(near, key=lambda x: x["net_oi"])["strike"]
    gamma_flip = None
    cum = 0
    for s in sorted(near, key=lambda x: x["strike"]):
        if s["strike"] < spot: continue
        cum += s["net_gex"]
        if cum > 0: gamma_flip = s["strike"]; break
    return {
        "call_wall":   call_wall,
        "put_wall":    put_wall,
        "max_pain":    max_pain,
        "gamma_flip":  gamma_flip,
        "total_gex":   round(sum(s["net_gex"]  for s in near), 2),
        "total_calls": round(sum(s["call_gex"] for s in near), 2),
        "total_puts":  round(sum(s["put_gex"]  for s in near), 2),
    }

# ── Main fetch pipeline ──────────────────────────────────────────
def fetch_gex(token, ticker):
    ticker = ticker.upper().strip()
    t0 = time.time()
    print(f"\n{'─'*55}")
    print(f"[GammaEdge] Fetching {ticker}...")
    print(f"{'─'*55}")

    # Spot price
    spot = get_spot(token, ticker)
    if spot is None:
        return {"error": f"Cannot fetch spot price for {ticker}. Check your Tradier token and internet connection."}
    print(f"  Spot: ${spot:,.2f}")

    # Expirations
    try:
        exps = get_expirations(token, ticker)
        if not exps and ticker == "SPX":
            print("  [exp] No SPX exps, trying SPXW...")
            exps = get_expirations(token, "SPXW")
        if not exps:
            return {"error": f"No expirations found for {ticker}"}
        print(f"  Expirations: {len(exps)} -- first: {exps[0]}, last: {exps[-1]}")
    except Exception as e:
        return {"error": f"Expirations error: {e}"}

    exps = exps[:MAX_EXPIRIES]

    # Option chains -- fetch SPX + SPXW in parallel
    all_contracts = []
    syms = [ticker] + (["SPXW"] if ticker == "SPX" else [])
    print(f"  Fetching chains for: {syms} x {len(exps)} expirations...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(get_chain, token, sym, exp): (sym, exp)
                for sym in syms for exp in exps}
        for f in as_completed(futs):
            all_contracts.extend(f.result())

    print(f"  Raw contracts returned: {len(all_contracts):,}")

    if not all_contracts:
        return {"error": "No option contracts returned -- check Tradier token and market hours."}

    # GEX computation
    strikes = compute_gex(all_contracts, spot, ticker)
    kl      = key_levels(strikes, spot)

    # Summary by expiry
    exp_summary = {}
    for s in strikes:
        for exp, gex in s["expirations"].items():
            if exp not in exp_summary:
                exp_summary[exp] = {"gex": 0, "dte": dte(exp)}
            exp_summary[exp]["gex"] += gex

    elapsed = round(time.time() - t0, 1)
    print(f"  Done in {elapsed}s -- {len(strikes)} strikes, "
          f"call_wall={kl.get('call_wall')}, put_wall={kl.get('put_wall')}, "
          f"total_gex={kl.get('total_gex')}")

    return {
        "ticker":      ticker,
        "spot":        spot,
        "strikes":     strikes,
        "key_levels":  kl,
        "expirations": {k: {"gex": round(v["gex"], 3), "dte": v["dte"]}
                        for k, v in sorted(exp_summary.items())},
        "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed":     elapsed,
        "data_source": "tradier",
    }


# ── Unified fetch with Tastytrade fallback ───────────────────────
def fetch_gex_with_fallback(token, ticker):
    """
    Try Tradier first. If it fails for any reason (bad token, HTTP error,
    empty result, exception), fall back to Tastytrade. If Tastytrade also
    fails, fall back to Massive.com as third tier.
    """
    tradier_ok = token is not None

    if tradier_ok:
        try:
            result = fetch_gex(token, ticker)
            if "error" not in result:
                return result
            print(f"\n[GammaEdge] Tradier returned error: {result['error']}")
            print("[GammaEdge] Switching to Tastytrade fallback...\n")
        except Exception as e:
            print(f"\n[GammaEdge] Tradier fetch raised exception: {e}")
            print("[GammaEdge] Switching to Tastytrade fallback...\n")
    else:
        print("[GammaEdge] No Tradier token -- using Tastytrade directly\n")

    # Tastytrade fallback (second tier)
    try:
        result = fetch_gex_tastytrade(ticker)
        if "error" not in result:
            return result
        print(f"\n[GammaEdge] Tastytrade returned error: {result.get('error')}")
        print("[GammaEdge] Switching to Massive fallback...\n")
    except Exception as e:
        print(f"\n[GammaEdge] Tastytrade fallback raised exception: {e}")
        print("[GammaEdge] Switching to Massive fallback...\n")

    # Massive fallback (third tier)
    try:
        return fetch_gex_massive(ticker)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[ERROR] Massive fallback also failed:\n{tb}")
        return {"error": f"All data sources failed (Tradier, Tastytrade, Massive). Last error: {e}"}


# ── Cache + HTTP server ──────────────────────────────────────────
_cache = {}
_lock  = threading.Lock()
TTL    = 120

def get_cached(token, ticker):
    with _lock:
        if ticker in _cache:
            d, ts = _cache[ticker]
            if time.time() - ts < TTL:
                return d
    try:
        d = fetch_gex_with_fallback(token, ticker)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[ERROR] fetch_gex_with_fallback crashed:\n{tb}")
        d = {"error": f"Internal error: {e}"}
    with _lock:
        _cache[ticker] = (d, time.time())
    return d

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        path   = self.path.split("?")[0].rstrip("/") or "/"
        params = {}
        if "?" in self.path:
            for p in self.path.split("?", 1)[1].split("&"):
                if "=" in p: k, v = p.split("=", 1); params[k] = v

        token  = self.server.token
        ticker = params.get("ticker", self.server.default_ticker).upper()

        # ── Serve dashboard HTML ─────────────────────────────────
        if path == "/":
            try:
                with open(HTML_FILE, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except FileNotFoundError:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"dashboard.html not found")
            return

        # ── JSON API endpoints ───────────────────────────────────
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        if path == "/gex":
            self.wfile.write(json.dumps(get_cached(token, ticker)).encode())
        elif path == "/health":
            self.wfile.write(b'{"status":"ok"}')
        elif path == "/debug":
            result = {}
            if token:
                for sym in ["SPX", "$SPX.X", "SPY"]:
                    try:
                        r = requests.get(f"{API_BASE}/markets/quotes",
                            headers=_hdr(token),
                            params={"symbols": sym, "greeks": "false"}, timeout=8)
                        result[sym] = {"status": r.status_code, "body": r.json()}
                    except Exception as e:
                        result[sym] = {"error": str(e)}
            else:
                result["tradier"] = "no token configured"
            self.wfile.write(json.dumps(result, indent=2).encode())
        else:
            self.wfile.write(b'{"error":"unknown endpoint"}')

    def log_message(self, fmt, *args):
        # Only log non-200 responses
        if len(args) > 1 and "200" not in str(args[1]):
            super().log_message(fmt, *args)

def run_server(token, ticker):
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("0.0.0.0", PORT), Handler) as httpd:
        httpd.token = token
        httpd.default_ticker = ticker
        sep = "=" * 55
        src = "Tradier" if token else "Tastytrade (no Tradier token)"
        print(f"\n{sep}")
        print(f"  GammaEdge is RUNNING")
        print(f"  Data source: {src} (Tastytrade + Massive fallback enabled)")
        print(f"  Dashboard: http://localhost:{PORT}/")
        print(f"  GEX API:   http://localhost:{PORT}/gex?ticker=SPX")
        print(f"  Health:    http://localhost:{PORT}/health")
        print(f"  Stop: Ctrl+C")
        print(f"{sep}\n")
        httpd.serve_forever()

if __name__ == "__main__":
    token  = get_token()
    ticker = get_default_ticker()
    src    = "Tradier" if token else "Tastytrade"
    print(f"[GammaEdge] Starting with {src}, pre-loading {ticker}...")
    threading.Thread(target=get_cached, args=(token, ticker), daemon=True).start()
    run_server(token, ticker)
