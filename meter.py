"""polymarket-5m-meter: μία μέρα (UTC) αγορών BTC Up/Down 5m -> σύνοψη ανά πορτοφόλι.

Τι κάνει:
 1. gamma-api: για κάθε παράθυρο 5' της μέρας (slug btc-updown-5m-<T>) διαβάζει conditionId, token ids, αποτέλεσμα (outcomePrices), fee schedule.
 2. SQD Portal: stream των OrderFilled (CTF Exchange V2, NegRisk V2 A/B, ExchangeV3) από [μέρα-24h, μέρα+26h).
 3. Κρατά ΜΟΝΟ fills με token id των αγορών της μέρας. Δεν αποθηκεύει raw fills.
 4. Ανά πορτοφόλι: όγκος, maker/taker, fees, εκτιμώμενο rebate, καθαρό αποτέλεσμα μετά την κρίση (payout = outcomePrices).
 5. --verify: ελέγχει 1 αγορά ολόκληρη με data-api v2 και gamma (όγκος, αριθμός fills, fees, αποτέλεσμα).
Κανένα secret στον κώδικα: προαιρετικό SQD_API_KEY από μεταβλητή περιβάλλοντος.
"""
import argparse, csv, gzip, json, os, sys, time, urllib.request, urllib.error
from collections import defaultdict
from datetime import datetime, timezone, timedelta

EXCHANGES = ["0xE111180000d2663C0091e4f400237545B87B996B", "0xe2222d279d744050d28e00520010520000310F59",
             "0xe2222d002000ba0053cef3375333610f64600036", "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"]
TOPIC_FILLED = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"  # OrderFilled V2 (10 πεδία)
SQD = "https://portal.sqd.dev/datasets/polygon-mainnet/stream"
ANCHORS = [(89900000, 1783554995), (91150000, 1785430020), (95000000, 1791205151), (97300000, 1794655469)]
SPB = 1.5017  # δευτ./block (μετρημένο σε 4 σημεία Ιούλ–Νοέ 2026, πάντα 1.5017)
FEE_RATE_DEFAULT = 0.07
HEADERS = {"User-Agent": "polymarket-5m-meter/0.1", "Accept-Encoding": "gzip"}
_last_req = [0.0]

def http(url, data=None, headers=None, timeout=120, tries=6):
    h = dict(HEADERS); h.update(headers or {})
    last = None
    for a in range(tries):
        gap = time.time() - _last_req[0]
        if gap < 0.25: time.sleep(0.25 - gap)   # ≤ 4 req/s, κάτω από το όριο 50 req/10s
        _last_req[0] = time.time()
        try:
            req = urllib.request.Request(url, data=data, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding") == "gzip": raw = gzip.decompress(raw)
                return raw
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            if e.code in (429, 502, 503, 504, 529):
                time.sleep(min(float(e.headers.get("Retry-After") or 10) + a, 45)); continue
            if e.code in (404, 400, 410): raise
            time.sleep(3 + a * 3)
        except Exception as e:
            last = repr(e)[:150]; time.sleep(3 + a * 3)
    raise RuntimeError(f"{url[:80]} failed: {last}")

def sqd_post(frm, to, with_tx=False):
    log_fields = {"address": True, "topics": True, "data": True}
    if with_tx: log_fields["transactionHash"] = True
    body = {"type": "evm", "fromBlock": frm, "toBlock": to,
            "fields": {"block": {"number": True, "timestamp": True}, "log": log_fields},
            "logs": [{"address": EXCHANGES, "topic0": [TOPIC_FILLED]}]}
    h = {"content-type": "application/json"}
    key = os.environ.get("SQD_API_KEY")
    if key: h["Authorization"] = "Bearer " + key
    return http(SQD, json.dumps(body).encode(), h, tries=40)

def find_block(ts):
    """Block με timestamp εντός ±4s από ts, με διόρθωση από πραγματικές κεφαλίδες."""
    a_n, a_t = min(ANCHORS, key=lambda a: abs(a[1] - ts))
    g = a_n + int((ts - a_t) / SPB)
    for _ in range(10):
        lines = [json.loads(l) for l in sqd_post(g, g + 40).splitlines() if l.strip()]
        hd = lines[0]["header"]
        err = ts - hd["timestamp"]
        if abs(err) <= 4: return hd["number"]
        g = hd["number"] + int(err / SPB)
    raise RuntimeError("find_block δεν συνέκλινε")

# ---------- αγορές ----------
def load_markets(day):
    t0 = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    markets, missing = [], []
    for T in range(t0, t0 + 86400, 300):
        try:
            ev = json.loads(http(f"https://gamma-api.polymarket.com/events?slug=btc-updown-5m-{T}", tries=4))
        except Exception as e:
            ev = []
        if not ev or not ev[0].get("markets"): missing.append(T); continue
        e = ev[0]; m = e["markets"][0]
        try:
            toks = json.loads(m["clobTokenIds"]); outs = json.loads(m["outcomes"]); pr = [float(x) for x in json.loads(m["outcomePrices"])]
        except Exception:
            missing.append(T); continue
        fs = m.get("feeSchedule") or {}
        markets.append({"T": T, "slug": e["slug"], "cond": m["conditionId"], "tokens": toks, "outcomes": outs, "payout": pr,
                        "closed": bool(m.get("closed")), "uma": m.get("umaResolutionStatus"),
                        "clean": sorted(pr) == [0.0, 1.0], "volume": float(m.get("volumeNum") or m.get("volume") or 0),
                        "fee_rate": float(fs.get("rate", FEE_RATE_DEFAULT)), "rebate_rate": float(fs.get("rebateRate", 0.2)),
                        "fee_type": m.get("feeType"), "final": (e.get("eventMetadata") or {}).get("finalPrice"),
                        "ptb": (e.get("eventMetadata") or {}).get("priceToBeat")})
    return markets, missing

# ---------- κύριο πέρασμα ----------
def process(day, pre_h, post_h, verify, outdir):
    d0 = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    markets, missing = load_markets(day)
    print(f"markets {len(markets)} missing {len(missing)}", flush=True)
    tokmap = {}
    for i, m in enumerate(markets):
        for k, t in enumerate(m["tokens"]): tokmap[format(int(t), "064x")] = (i, k)
    ts_from, ts_to = d0 - pre_h * 3600, d0 + 86400 + post_h * 3600
    b0, b1 = find_block(ts_from) - 5, find_block(ts_to) + 5
    print(f"blocks {b0}..{b1} ({b1-b0} blocks)", flush=True)
    # ανά (αγορά, πορτοφόλι) -> στατιστικά
    pos = defaultdict(lambda: {"sh": [0.0, 0.0], "cash": 0.0, "fee": 0.0, "mk_fills": 0, "tk_fills": 0, "mk_usd": 0.0, "tk_usd": 0.0,
                               "mk_sh": 0.0, "tk_sh": 0.0, "fe": 0.0, "hours": defaultdict(int)})
    mk_fe = defaultdict(float); tk_fee = defaultdict(float)
    mstat = defaultdict(lambda: {"rows": 0, "sh_tk": 0.0, "usd_tk": 0.0, "sh_mk": 0.0, "usd_mk": 0.0, "fee_words": 0.0, "fee_tk": 0.0, "fee_formula": 0.0, "rel_t": defaultdict(int)})
    vrows = defaultdict(list)   # μόνο για verify: (tx, wallet, token, shares)
    cur, nlogs, nmatch, nreq = b0, 0, 0, 0
    while cur < b1:
        body = sqd_post(cur, b1 - 1, with_tx=verify); nreq += 1
        last = None
        for line in body.splitlines():
            if not line.strip(): continue
            o = json.loads(line); hd = o["header"]; last = hd["number"]
            ts = hd["timestamp"]
            if ts < ts_from or ts >= ts_to: continue
            for lg in o.get("logs", ()):
                nlogs += 1
                dt = lg["data"]
                tk = dt[66:130]
                hit = tokmap.get(tk)
                if hit is None: continue
                nmatch += 1
                mi, ki = hit; m = markets[mi]
                tp = lg["topics"]
                maker = "0x" + tp[2][-40:]; taker = tp[3][-40:]
                is_taker_row = ("0x" + taker) == lg["address"].lower()
                side = int(dt[2:66], 16)
                a1 = int(dt[130:194], 16); a2 = int(dt[194:258], 16); fee = int(dt[258:322], 16) / 1e6
                if side == 0: usd, sh = a1 / 1e6, a2 / 1e6     # BUY: πληρώνει collateral, παίρνει shares
                else: usd, sh = a2 / 1e6, a1 / 1e6             # SELL: δίνει shares, παίρνει collateral
                if sh <= 0: continue
                p = usd / sh
                P = pos[(mi, maker)]
                if side == 0: P["cash"] -= usd; P["sh"][ki] += sh
                else: P["cash"] += usd; P["sh"][ki] -= sh
                P["fee"] += fee
                fe = sh * m["fee_rate"] * p * (1 - p)
                hr = datetime.fromtimestamp(ts, timezone.utc).hour
                P["hours"][hr] += 1
                S = mstat[mi]; S["rows"] += 1; S["fee_words"] += fee; S["rel_t"][int((ts - m["T"]) // 3600)] += 1
                if is_taker_row:
                    P["tk_fills"] += 1; P["tk_usd"] += usd; P["tk_sh"] += sh; S["sh_tk"] += sh; S["usd_tk"] += usd
                    S["fee_formula"] += fe; S["fee_tk"] += fee; tk_fee[mi] += fee
                else:
                    P["mk_fills"] += 1; P["mk_usd"] += usd; P["mk_sh"] += sh; P["fe"] += fe; mk_fe[mi] += fe
                    S["sh_mk"] += sh; S["usd_mk"] += usd
                if verify: vrows[mi].append((lg.get("transactionHash"), maker, tk, round(sh, 6), is_taker_row, side, round(p, 6), fee))
        if last is None: raise RuntimeError(f"κενή απάντηση στο block {cur}")
        cur = last + 1
        if nreq % 10 == 0: print(f"req {nreq} block {cur} logs {nlogs} matched {nmatch}", flush=True)
    print(f"done stream: reqs {nreq}, logs {nlogs}, matched {nmatch}", flush=True)

    # ---------- σύνοψη ανά πορτοφόλι ----------
    W = defaultdict(lambda: {"fills": 0, "mk_fills": 0, "tk_fills": 0, "mk_usd": 0.0, "tk_usd": 0.0, "mk_sh": 0.0, "tk_sh": 0.0,
                             "fee": 0.0, "rebate": 0.0, "gross": 0.0, "markets": 0, "neg_inventory": 0, "hours": [0] * 24})
    for (mi, w), P in pos.items():
        m = markets[mi]
        payout = sum(P["sh"][k] * m["payout"][k] for k in (0, 1))
        gross = P["cash"] + payout
        reb = 0.0
        if mk_fe[mi] > 0: reb = m["rebate_rate"] * tk_fee[mi] * P["fe"] / mk_fe[mi]
        R = W[w]
        R["fills"] += P["mk_fills"] + P["tk_fills"]; R["mk_fills"] += P["mk_fills"]; R["tk_fills"] += P["tk_fills"]
        for k in ("mk_usd", "tk_usd", "mk_sh", "tk_sh", "fee"): R[k] += P[k]
        R["rebate"] += reb; R["gross"] += gross; R["markets"] += 1
        if P["sh"][0] < -1e-6 or P["sh"][1] < -1e-6: R["neg_inventory"] += 1
        for h, c in P["hours"].items(): R["hours"][h] += c
    os.makedirs(os.path.join(outdir, "wallets"), exist_ok=True); os.makedirs(os.path.join(outdir, "markets"), exist_ok=True)
    cols = ["wallet", "fills", "mk_fills", "tk_fills", "markets", "mk_usd", "tk_usd", "mk_sh", "tk_sh", "fee", "rebate_est", "gross", "net", "neg_inv_markets", "hours"]
    with gzip.open(os.path.join(outdir, "wallets", f"day={day}.csv.gz"), "wt", newline="") as f:
        wr = csv.writer(f); wr.writerow(cols)
        for w, R in sorted(W.items()):
            wr.writerow([w, R["fills"], R["mk_fills"], R["tk_fills"], R["markets"], round(R["mk_usd"], 4), round(R["tk_usd"], 4), round(R["mk_sh"], 4), round(R["tk_sh"], 4),
                         round(R["fee"], 6), round(R["rebate"], 6), round(R["gross"], 6), round(R["gross"] - R["fee"] + R["rebate"], 6), R["neg_inventory"], "|".join(map(str, R["hours"]))])
    with open(os.path.join(outdir, "markets", f"day={day}.csv"), "w", newline="") as f:
        wr = csv.writer(f); wr.writerow(["T", "slug", "cond", "clean_result", "winner", "gamma_volume", "sqd_rows", "sqd_usd_taker_rows", "sqd_usd_maker_rows", "sqd_fee_words", "sqd_fee_formula"])
        for i, m in enumerate(markets):
            S = mstat[i]
            wn = m["outcomes"][m["payout"].index(max(m["payout"]))] if m["clean"] else "UNCLEAR"
            wr.writerow([m["T"], m["slug"], m["cond"], m["clean"], wn, m["volume"], S["rows"], round(S["usd_tk"], 4), round(S["usd_mk"], 4), round(S["fee_words"], 6), round(S["fee_formula"], 6)])
    summary = {"day": day, "markets": len(markets), "missing_windows": missing, "unclear_results": [m["slug"] for m in markets if not m["clean"]],
               "stream_requests": nreq, "logs_scanned": nlogs, "logs_matched": nmatch, "wallets": len(W)}
    json.dump(summary, open(os.path.join(outdir, f"summary_{day}.json"), "w"), indent=1)
    if verify: do_verify(day, markets, mstat, vrows, outdir)

def do_verify(day, markets, mstat, vrows, outdir):
    """Έλεγχος 1 αγοράς (διάμεσος όγκος): SQD έναντι data-api v2 έναντι gamma. Μόνο επίπεδο αγοράς, όχι πορτοφολιών."""
    ranked = sorted([i for i in range(len(markets)) if mstat[i]["rows"] > 0], key=lambda i: mstat[i]["usd_tk"])
    i = ranked[len(ranked) // 2]; m = markets[i]; S = mstat[i]
    rep = {"market": m["slug"], "cond": m["cond"], "gamma_volume": m["volume"], "winner_gamma": m["outcomes"][m["payout"].index(max(m["payout"]))],
           "final_price": m["final"], "price_to_beat": m["ptb"],
           "winner_from_prices": ("Up" if (m["final"] is not None and m["ptb"] is not None and m["final"] >= m["ptb"]) else "Down") if m["final"] is not None else None,
           "sqd": {"rows": S["rows"], "shares_taker_rows": round(S["sh_tk"], 4), "usd_taker_rows": round(S["usd_tk"], 4), "shares_maker_rows": round(S["sh_mk"], 4),
                   "usd_maker_rows": round(S["usd_mk"], 4), "fee_words_all_rows": round(S["fee_words"], 6), "fee_words_taker_rows": round(S["fee_tk"], 6), "fee_formula_total_taker_rows": round(S["fee_formula"], 6),
                   "fills_by_hour_rel_window_start": dict(sorted(S["rel_t"].items()))}}
    # data-api v2: όλες οι σελίδες
    rows, cursor = [], None
    for page in range(80):
        url = f"https://data-api.polymarket.com/v2/trades?conditionId={m['cond']}&limit=500" + (f"&cursor={cursor}" if cursor else "")
        try: j = json.loads(http(url, tries=4))
        except Exception as e: rep["data_api_error"] = repr(e)[:200]; break
        rows += j.get("data") or []
        pg = j.get("pagination") or {}
        cursor = pg.get("next_cursor")
        if not cursor or not pg.get("has_more"): break
    rep["data_api"] = {"rows": len(rows), "sum_size": round(sum(r["size"] for r in rows), 4), "sum_size_x_price": round(sum(r["size"] * r["price"] for r in rows), 4), "pages": page + 1}
    # σύγκριση ανά (tx, πορτοφόλι, token): πλήθος γραμμών και άθροισμα shares (ανοχή 0.006/γραμμή για στρογγυλοποίηση του data-api)
    dagg = defaultdict(lambda: [0, 0.0]); sagg = defaultdict(lambda: [0, 0.0])
    for r in rows:
        k = (r["transaction_hash"], r["proxy_wallet"].lower(), format(int(r["token_id"]), "064x")); dagg[k][0] += 1; dagg[k][1] += r["size"]
    for (tx, w, tk, sh, is_tk, side, p, fee) in vrows[i]:
        k = (tx, w, tk); sagg[k][0] += 1; sagg[k][1] += sh
    ok = bad_cnt = bad_sz = 0; ex = []
    for k, (c, z) in dagg.items():
        sc, sz = sagg.get(k, (0, 0.0))
        if sc == c and abs(sz - z) <= 0.006 * c: ok += 1
        elif sc == c: bad_sz += 1; ex.append([k[0][:12], k[1][:10], c, round(z, 4), round(sz, 4)])
        else: bad_cnt += 1; ex.append([k[0][:12], k[1][:10], f"data_api {c} rows vs sqd {sc}", round(z, 4), round(sz, 4)])
    only_sqd = [k for k in sagg if k not in dagg]
    rep["key_match"] = {"data_api_keys": len(dagg), "sqd_keys": len(sagg), "match": ok, "count_differs": bad_cnt, "size_differs": bad_sz, "only_in_sqd": len(only_sqd)}
    rep["sample_mismatch"] = ex[:6]
    rep["sample_only_in_sqd"] = [[k[0][:12], k[1][:10], sagg[k][0], round(sagg[k][1], 4)] for k in only_sqd[:6]]
    json.dump(rep, open(os.path.join(outdir, f"verify_{day}.json"), "w"), indent=1, ensure_ascii=False)
    print(json.dumps(rep, ensure_ascii=False)[:3000], flush=True)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True); ap.add_argument("--out", default="out")
    ap.add_argument("--pre-hours", type=int, default=3); ap.add_argument("--post-hours", type=int, default=2)
    ap.add_argument("--verify", action="store_true")
    a = ap.parse_args()
    process(a.date, a.pre_hours, a.post_hours, a.verify, a.out)
