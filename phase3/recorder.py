"""phase3/recorder.py — καταγραφή βιβλίου εντολών + συναλλαγών για τα 5λεπτα BTC Up/Down (PREREG_phase3.md §1).

Τρέχει συνεχώς σε VPS (systemd). Μία σύνδεση websocket, κυλιόμενη συνδρομή: κάθε αγορά T από T−600 s έως T+360 s.
Αποθηκεύει (συμπιεσμένα CSV ανά ώρα UTC λήψης, στο --data/rec/YYYY-MM-DD/HH.csv.gz):
  B,recv_ms,ts_ms,T,side,levels           snapshot βιβλίου Up, N καλύτερα επίπεδα ("p@s|p@s|...")
  P,recv_ms,ts_ms,T,side,price,size,bb,ba  price_change Up που αγγίζει τα N καλύτερα επίπεδα (πριν ή μετά)
  X,recv_ms,ts_ms,T,UD,side,price,size,fee_bps,tx   κάθε συναλλαγή (Up και Down)
  K,recv_ms,ts_ms,T,UD,old,new             tick size change
  R,recv_ms,ts_ms,T,winning_outcome        market_resolved
  S/U,recv_ms,,T,UD,asset                  συνδρομή / απεγγραφή
  G,recv_ms,,reason                        κενό (αποσύνδεση κλπ.)
Το βιβλίο του Down δεν αποθηκεύεται (καθρέφτης του Up — probe: 118/118). Το size του price_change = νέο σύνολο επιπέδου (probe: 96,7%).
Κατάσταση κάθε λεπτό σε --data/status/latest.json και ανά ώρα σε --data/status/hour=YYYY-MM-DDTHH.json.
"""
import argparse, asyncio, gzip, json, os, sys, time, urllib.request
from collections import Counter

WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA = "https://gamma-api.polymarket.com/events?slug=btc-updown-5m-{T}"
PRE, POST = 600, 360


def http_json(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "polymarket-5m-meter/phase3-recorder"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def market_tokens(T):
    ev = http_json(GAMMA.format(T=T)); m = ev[0]["markets"][0]
    toks = json.loads(m["clobTokenIds"]); outs = [o.lower() for o in json.loads(m["outcomes"])]
    return toks[outs.index("up")], toks[outs.index("down")]


class Book:
    def __init__(self): self.bids, self.asks = {}, {}

    def snap(self, bids, asks):
        self.bids = {float(x["price"]): float(x["size"]) for x in bids}
        self.asks = {float(x["price"]): float(x["size"]) for x in asks}

    def side(self, s): return self.bids if s == "BUY" else self.asks

    def top(self, s, n):
        d = self.side(s)
        return sorted(d, reverse=(s == "BUY"))[:n]

    def apply(self, s, price, size):
        d = self.side(s); v = float(size); price = float(price)
        if v <= 1e-12: d.pop(price, None)
        else: d[price] = v


class Rec:
    """Λογική χωρίς δίκτυο (δοκιμάζεται με συνθετικά μηνύματα)."""
    def __init__(self, levels=10):
        self.N = levels
        self.assets = {}          # asset_id -> (T, "U"/"D")
        self.books = {}           # Up asset_id -> Book
        self.cnt = Counter()

    def add_market(self, T, up, dn):
        self.assets[up] = (T, "U"); self.assets[dn] = (T, "D"); self.books[up] = Book()

    def drop_market(self, up, dn):
        self.assets.pop(up, None); self.assets.pop(dn, None); self.books.pop(up, None)

    def handle(self, ev, recv_ms):
        et = ev.get("event_type"); rows = []; self.cnt[et] += 1
        ts = ev.get("timestamp", "")
        if et == "book":
            a = ev.get("asset_id"); info = self.assets.get(a)
            if info and info[1] == "U":
                b = self.books[a]; b.snap(ev.get("bids", []), ev.get("asks", []))
                for s, key in (("BUY", "bids"), ("SELL", "asks")):
                    lv = "|".join(f"{p:g}@{b.side(s)[p]:g}" for p in b.top(s, self.N))
                    rows.append(["B", recv_ms, ts, info[0], s, lv])
        elif et == "price_change":
            for pc in ev.get("price_changes", []):
                a = pc.get("asset_id"); info = self.assets.get(a)
                if not info or info[1] != "U": continue
                b = self.books[a]; s = pc["side"]; p = float(pc["price"])
                before = p in b.top(s, self.N)
                b.apply(s, p, pc["size"])
                after = p in b.top(s, self.N)
                if before or after:
                    rows.append(["P", recv_ms, ts, info[0], s, pc["price"], pc["size"], pc.get("best_bid", ""), pc.get("best_ask", "")])
                else:
                    self.cnt["P_dropped"] += 1
        elif et == "last_trade_price":
            info = self.assets.get(ev.get("asset_id"))
            if info:
                rows.append(["X", recv_ms, ts, info[0], info[1], ev.get("side", ""), ev.get("price", ""), ev.get("size", ""),
                             ev.get("fee_rate_bps", ""), ev.get("transaction_hash", "")])
        elif et == "tick_size_change":
            info = self.assets.get(ev.get("asset_id"))
            if info: rows.append(["K", recv_ms, ts, info[0], info[1], ev.get("old_tick_size", ""), ev.get("new_tick_size", "")])
        elif et == "market_resolved":
            ids = ev.get("assets_ids") or []
            Ts = {self.assets[i][0] for i in ids if i in self.assets}
            for T in Ts: rows.append(["R", recv_ms, ts, T, ev.get("winning_outcome", "")])
        return rows


class Writer:
    def __init__(self, root):
        self.root = root; self.key = None; self.f = None; self.bytes = 0; self.rows = 0

    def write(self, rows, now):
        if not rows: return
        key = time.strftime("%Y-%m-%d/%H", time.gmtime(now))
        if key != self.key:
            if self.f: self.f.close()
            d = os.path.join(self.root, "rec", key[:10]); os.makedirs(d, exist_ok=True)
            self.f = gzip.open(os.path.join(d, key[11:] + ".csv.gz"), "at"); self.key = key
        for r in rows:
            line = ",".join(str(x) for x in r) + "\n"; self.f.write(line); self.bytes += len(line); self.rows += 1

    def flush(self):
        if self.f: self.f.flush()


async def run(a):
    import websockets
    W = Writer(a.data); st_dir = os.path.join(a.data, "status"); os.makedirs(st_dir, exist_ok=True)
    started = time.time(); gaps = []; reconnects = 0
    while True:
        R = Rec(a.levels); live = {}   # T -> (up, dn)
        try:
            async with websockets.connect(WS, ping_interval=None, max_size=2**25, open_timeout=30, close_timeout=5) as ws:
                await ws.send(json.dumps({"assets_ids": [], "type": "market", "custom_feature_enabled": True}))

                async def pinger():
                    while True:
                        await asyncio.sleep(10); await ws.send("PING")

                async def roller():
                    while True:
                        now = time.time(); now_ms = int(now * 1000); T0 = int(now) - int(now) % 300
                        want = [T for T in range(T0 - 300, T0 + PRE + 301, 300) if T - PRE <= now < T + POST]
                        for T in want:
                            if T in live: continue
                            try: up, dn = await asyncio.to_thread(market_tokens, T)
                            except Exception as e:
                                W.write([["G", now_ms, "", f"gamma_fail T={T} {repr(e)[:80]}"]], now); continue
                            live[T] = (up, dn); R.add_market(T, up, dn)
                            await ws.send(json.dumps({"assets_ids": [up, dn], "operation": "subscribe", "custom_feature_enabled": True}))
                            W.write([["S", now_ms, "", T, "U", up], ["S", now_ms, "", T, "D", dn]], now)
                        for T in [T for T in live if now >= T + POST]:
                            up, dn = live.pop(T)
                            await ws.send(json.dumps({"assets_ids": [up, dn], "operation": "unsubscribe"}))
                            R.drop_market(up, dn); W.write([["U", now_ms, "", T, "U", up], ["U", now_ms, "", T, "D", dn]], now)
                        await asyncio.sleep(5)

                async def statuser():
                    last_hour = None
                    while True:
                        await asyncio.sleep(60); W.flush()
                        now = time.time()
                        st = {"now": now, "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)), "uptime_h": (now - started) / 3600,
                              "reconnects": reconnects, "gaps": gaps[-50:], "rows": W.rows, "bytes_uncompressed": W.bytes,
                              "live_markets": sorted(live), "counts": dict(R.cnt), "levels": a.levels}
                        json.dump(st, open(os.path.join(st_dir, "latest.json"), "w"))
                        h = time.strftime("%Y-%m-%dT%H", time.gmtime(now))
                        if h != last_hour:
                            json.dump(st, open(os.path.join(st_dir, f"hour={h}.json"), "w")); last_hour = h

                tasks = [asyncio.create_task(f()) for f in (pinger, roller, statuser)]
                try:
                    async for raw in ws:
                        now = time.time()
                        if raw == "PONG": continue
                        try: data = json.loads(raw)
                        except Exception: R.cnt["nonjson"] += 1; continue
                        rows = []
                        for ev in (data if isinstance(data, list) else [data]): rows += R.handle(ev, int(now * 1000))
                        W.write(rows, now)
                finally:
                    for t in tasks: t.cancel()
        except Exception as e:
            err = repr(e)[:120]
        else:
            err = "closed"
        reconnects += 1; now = time.time()
        gaps.append([int(now), err]); W.write([["G", int(now * 1000), "", "reconnect " + err.replace(",", ";")]], now); W.flush()
        await asyncio.sleep(min(30, 2 + reconnects))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="/data"); ap.add_argument("--levels", type=int, default=10)
    asyncio.run(run(ap.parse_args()))
