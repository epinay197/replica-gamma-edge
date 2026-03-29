"""
GammaEdge GEX Dashboard — Tradier Production
=============================================
Run:  python gamma_edge.py
Open: dashboard.html
"""

import os, sys, json, time, math, threading, http.server, socketserver, traceback
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    print("\n[ERROR] 'requests' not installed.  Fix: pip install requests\n")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────
CREDS_FILE   = os.path.join(os.path.dirname(__file__), "credentials.env")
API_BASE     = os.environ.get("TRADIER_API_BASE", "https://sandbox.tradier.com/v1")
PORT         = int(os.environ.get("PORT", 5050))
MAX_EXPIRIES = 20
MAX_WORKERS  = 8
INDEX_ROOTS  = {"SPX","SPXW","NDX","NDXW","VIX","RUT","RUTW","XSP","MXEA","MXEF"}
HTML_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")

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
        print("\n[ERROR] No TRADIER_TOKEN found. Set env var TRADIER_TOKEN or add to credentials.env\n")
        sys.exit(1)
    return token

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
    # Instead, we find the strike with highest total OI — that cluster
    # sits right around the true current spot level.
    roots = ["SPXW", "SPX"] if sym in ("SPX","SPXW") else [sym]
    for root in roots:
        try:
            exps = get_expirations(token, root)
            if not exps:
                continue
            # Use nearest expiry — has the most meaningful OI clustering
            r = requests.get(f"{API_BASE}/markets/options/chains",
                headers=_hdr(token),
                params={"symbol": root, "expiration": exps[0], "greeks": "true"},
                timeout=15)
            print(f"  [spot] chain/{root}/{exps[0]} → {r.status_code}")
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

            # Method A: 'underlying' field — Tradier puts current price here on production,
            # symbol string on sandbox. Try to parse it as a number.
            for c in chain:
                for field in ("underlying", "underlying_price"):
                    val = c.get(field)
                    if val is not None:
                        try:
                            p = float(val)
                            if 500 < p < 100000:   # must look like a real index price
                                print(f"  [spot] {root} {field}={p:,.2f} ✓")
                                return p
                            else:
                                print(f"  [spot] {root} {field}={val} (not a valid price, skipping)")
                                break  # same value for all contracts, no need to loop
                        except (ValueError, TypeError):
                            print(f"  [spot] {root} {field}='{val}' (string, not a price)")
                            break  # same value for all contracts, no need to loop

            # Method B: weighted average of strikes by OI — clusters around spot
            oi_by_strike = {}
            for c in chain:
                st = c.get("strike")
                oi = c.get("open_interest") or 0
                if st and oi:
                    oi_by_strike[float(st)] = oi_by_strike.get(float(st), 0) + oi

            if oi_by_strike:
                total_oi = sum(oi_by_strike.values())
                if total_oi > 0:
                    # Weighted average strike by OI — good approximation of spot
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
            print(f"  [chain] {symbol} {expiry} → HTTP {r.status_code}")
            return []
        opts = r.json().get("options") or {}
        chain = opts.get("option", [])
        if chain is None: return []
        return [chain] if isinstance(chain, dict) else chain
    except Exception as e:
        print(f"  [chain] {symbol} {expiry} → {e}")
        return []


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

        # Index: dealers SHORT gamma on both calls and puts → negative GEX
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
        print(f"  Expirations: {len(exps)} — first: {exps[0]}, last: {exps[-1]}")
    except Exception as e:
        return {"error": f"Expirations error: {e}"}

    exps = exps[:MAX_EXPIRIES]

    # Option chains — fetch SPX + SPXW in parallel
    all_contracts = []
    syms = [ticker] + (["SPXW"] if ticker == "SPX" else [])
    print(f"  Fetching chains for: {syms} × {len(exps)} expirations...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(get_chain, token, sym, exp): (sym, exp)
                for sym in syms for exp in exps}
        for f in as_completed(futs):
            all_contracts.extend(f.result())

    print(f"  Raw contracts returned: {len(all_contracts):,}")

    if not all_contracts:
        return {"error": "No option contracts returned — check Tradier token and market hours."}

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
    print(f"  Done in {elapsed}s — {len(strikes)} strikes, "
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
    }

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
        d = fetch_gex(token, ticker)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[ERROR] fetch_gex crashed:\n{tb}")
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
            for sym in ["SPX", "$SPX.X", "SPY"]:
                try:
                    r = requests.get(f"{API_BASE}/markets/quotes",
                        headers=_hdr(token),
                        params={"symbols": sym, "greeks": "false"}, timeout=8)
                    result[sym] = {"status": r.status_code, "body": r.json()}
                except Exception as e:
                    result[sym] = {"error": str(e)}
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
        print(f"\n{sep}")
        print(f"  GammaEdge is RUNNING")
        print(f"  Dashboard: http://localhost:{PORT}/")
        print(f"  GEX API:   http://localhost:{PORT}/gex?ticker=SPX")
        print(f"  Health:    http://localhost:{PORT}/health")
        print(f"  Stop: Ctrl+C")
        print(f"{sep}\n")
        httpd.serve_forever()

if __name__ == "__main__":
    token  = get_token()
    ticker = get_default_ticker()
    print(f"[GammaEdge] Token loaded, pre-loading {ticker}...")
    threading.Thread(target=get_cached, args=(token, ticker), daemon=True).start()
    run_server(token, ticker)
