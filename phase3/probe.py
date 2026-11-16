"""phase3/probe.py — δοκιμή οργάνου (ΟΧΙ μέτρηση): 15' websocket του CLOB για τα 5λεπτα BTC Up/Down.

Απαντά, πριν γραφτεί το PREREG_phase3:
 1. Δουλεύει το wss://ws-subscriptions-clob.polymarket.com/ws/market από GitHub runner;
 2. Το `size` του price_change είναι νέο σύνολο του επιπέδου (A) ή αλλαγή (B); — σύγκριση με REST /book κάθε 20 s.
 3. Είναι τα βιβλία Up/Down καθρέφτες (best_bid_Up + best_ask_Down = 1);
 4. Πόσα μηνύματα/bytes ανά λεπτό (για να κριθεί αν 14 μέρες χωράνε).
 5. Τα last_trade_price έχουν size/side/transaction_hash (για ένωση με τα on-chain fills);
Έξοδος: phase3/probe/summary.json + sample.jsonl.gz (πρώτα 3 λεπτά raw). Κανένα secret.
"""
import argparse, asyncio, gzip, json, os, sys, time, urllib.request
from collections import Counter, defaultdict

WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA = "https://gamma-api.polymarket.com/events?slug=btc-updown-5m-{T}"
BOOK = "https://clob.polymarket.com/book?token_id={tid}"
HERE = os.path.dirname(os.path.abspath(__file__))


def http_json(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "polymarket-5m-meter/phase3-probe"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def market_tokens(T):
    try:
        ev = http_json(GAMMA.format(T=T))
        m = ev[0]["markets"][0]
        return {"T": T, "tokens": json.loads(m["clobTokenIds"]), "outcomes": json.loads(m["outcomes"])}
    except Exception as e:
        return {"T": T, "error": repr(e)[:200]}


class Book:
    def __init__(self):
        self.bids, self.asks = {}, {}

    def snap(self, bids, asks):
        self.bids = {float(x["price"]): float(x["size"]) for x in bids}
        self.asks = {float(x["price"]): float(x["size"]) for x in asks}

    def apply(self, side, price, size, mode):
        d = self.bids if side == "BUY" else self.asks
        p, s = float(price), float(size)
        v = s if mode == "A" else d.get(p, 0.0) + s
        if v <= 1e-9: d.pop(p, None)
        else: d[p] = v

    def top(self):
        return (max(self.bids) if self.bids else None, min(self.asks) if self.asks else None)


def cmp(book, rest):
    """Σύγκριση με REST /book: ποσοστό επιπέδων με ίδιο size (ανοχή 1e-6), στα 10 καλύτερα κάθε πλευράς."""
    rb = {float(x["price"]): float(x["size"]) for x in rest.get("bids", [])}
    ra = {float(x["price"]): float(x["size"]) for x in rest.get("asks", [])}
    ok = tot = 0
    for mine, theirs, rev in ((book.bids, rb, True), (book.asks, ra, False)):
        keys = sorted(set(mine) | set(theirs), reverse=rev)[:10]
        for k in keys:
            tot += 1; ok += abs(mine.get(k, 0.0) - theirs.get(k, 0.0)) < 1e-6
    return ok, tot


async def main_async(minutes, outdir):
    import websockets
    os.makedirs(outdir, exist_ok=True)
    t_end = time.time() + minutes * 60
    S = {"started": time.time(), "minutes": minutes, "errors": [], "connect": None, "markets": [], "types": Counter(), "bytes": 0,
         "msgs": 0, "per_min": [], "rest_cmp": {"A": [0, 0], "B": [0, 0]}, "rest_errors": 0, "mirror": [], "trade_fields": Counter(),
         "trade_has_hash": 0, "trades": 0, "price_change_entries": 0, "ts_lag_ms": []}
    books = {"A": defaultdict(Book), "B": defaultdict(Book)}
    pairs = []          # (T, upTok, downTok)
    subscribed = set()
    sample = gzip.open(os.path.join(outdir, "sample.jsonl.gz"), "wt")
    sample_until = time.time() + 180
    minute_bytes = 0; minute_start = time.time()

    def want_markets():
        now = int(time.time()); T0 = now - now % 300
        out = []
        for T in (T0, T0 + 300, T0 + 600):
            if T in {p[0] for p in pairs}: continue
            m = market_tokens(T); S["markets"].append({k: v for k, v in m.items() if k != "tokens"} | {"ok": "tokens" in m})
            if "tokens" in m:
                o = [x.lower() for x in m["outcomes"]]; up = m["tokens"][o.index("up")]; dn = m["tokens"][o.index("down")]
                pairs.append((T, up, dn)); out += [up, dn]
        return out

    try:
        async with websockets.connect(WS, ping_interval=None, max_size=2**24, open_timeout=30) as ws:
            S["connect"] = "ok"
            first = want_markets(); subscribed.update(first)
            await ws.send(json.dumps({"assets_ids": first, "type": "market", "custom_feature_enabled": True}))

            async def pinger():
                while time.time() < t_end:
                    await asyncio.sleep(10)
                    try: await ws.send("PING")
                    except Exception: return

            async def rester():
                while time.time() < t_end:
                    await asyncio.sleep(20)
                    for tid in list(subscribed)[-4:]:
                        try:
                            rest = await asyncio.to_thread(http_json, BOOK.format(tid=tid))
                        except Exception:
                            S["rest_errors"] += 1; continue
                        for mode in ("A", "B"):
                            ok, tot = cmp(books[mode][tid], rest); S["rest_cmp"][mode][0] += ok; S["rest_cmp"][mode][1] += tot
                    for (T, up, dn) in pairs[-3:]:
                        bu, au = books["A"][up].top(); bd, ad = books["A"][dn].top()
                        if bu is not None and ad is not None: S["mirror"].append(round(bu + ad, 6))

            async def roller():
                while time.time() < t_end:
                    await asyncio.sleep(30)
                    new = [t for t in await asyncio.to_thread(want_markets) if t not in subscribed]
                    if new:
                        subscribed.update(new)
                        await ws.send(json.dumps({"assets_ids": new, "operation": "subscribe", "custom_feature_enabled": True}))

            tasks = [asyncio.create_task(f()) for f in (pinger, rester, roller)]
            while time.time() < t_end:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    S["errors"].append(["recv_timeout", time.time()]); continue
                n = len(raw); S["bytes"] += n; minute_bytes += n; S["msgs"] += 1
                if time.time() - minute_start >= 60:
                    S["per_min"].append(minute_bytes); minute_bytes = 0; minute_start = time.time()
                if raw == "PONG": S["types"]["PONG"] += 1; continue
                if time.time() < sample_until: sample.write(raw + "\n")
                try: data = json.loads(raw)
                except Exception: S["types"]["nonjson"] += 1; continue
                for ev in (data if isinstance(data, list) else [data]):
                    et = ev.get("event_type", "?"); S["types"][et] += 1
                    if ev.get("timestamp"):
                        try: S["ts_lag_ms"].append(int(time.time() * 1000) - int(ev["timestamp"]))
                        except Exception: pass
                    if et == "book":
                        for mode in ("A", "B"): books[mode][ev["asset_id"]].snap(ev.get("bids", []), ev.get("asks", []))
                    elif et == "price_change":
                        for pc in ev.get("price_changes", []):
                            S["price_change_entries"] += 1
                            for mode in ("A", "B"): books[mode][pc["asset_id"]].apply(pc["side"], pc["price"], pc["size"], mode)
                    elif et == "last_trade_price":
                        S["trades"] += 1; S["trade_has_hash"] += bool(ev.get("transaction_hash"))
                        for k in ev: S["trade_fields"][k] += 1
            for t in tasks: t.cancel()
    except Exception as e:
        S["errors"].append(["fatal", repr(e)[:300]])
    sample.close()
    lag = sorted(S["ts_lag_ms"]); S["ts_lag_ms"] = {"n": len(lag), "p50": lag[len(lag) // 2] if lag else None, "p90": lag[int(len(lag) * 0.9)] if lag else None}
    mir = S["mirror"]; S["mirror"] = {"n": len(mir), "share_equal_1": (sum(1 for x in mir if abs(x - 1) < 1e-6) / len(mir)) if mir else None, "sample": mir[:20]}
    for mode in ("A", "B"):
        ok, tot = S["rest_cmp"][mode]; S["rest_cmp"][mode] = {"levels_equal": ok, "levels": tot, "share": ok / tot if tot else None}
    S["types"] = dict(S["types"]); S["trade_fields"] = dict(S["trade_fields"])
    mins = max(1e-9, (time.time() - S["started"]) / 60)
    S["bytes_per_min"] = S["bytes"] / mins; S["est_GB_14_days_raw"] = S["bytes_per_min"] * 60 * 24 * 14 / 1e9
    json.dump(S, open(os.path.join(outdir, "summary.json"), "w"), indent=1, default=str)
    print(json.dumps({k: S[k] for k in ("connect", "errors", "types", "rest_cmp", "mirror", "bytes_per_min", "est_GB_14_days_raw", "trades", "trade_has_hash", "ts_lag_ms")}, default=str)[:3000])


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--minutes", type=float, default=15); ap.add_argument("--out", default=os.path.join(HERE, "probe"))
    a = ap.parse_args(); asyncio.run(main_async(a.minutes, a.out))
