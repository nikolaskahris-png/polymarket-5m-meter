"""phase2/fills.py — μία μέρα (UTC) αγορών BTC 5m -> συγκεντρωτικά ανά πορτοφόλι της λίστας (PREREG_phase2.md).

Ίδιο stream/αποκωδικοποίηση με meter.py (εισάγεται). Δεν αποθηκεύει raw fills.
Για κάθε fill πορτοφολιού της λίστας (phase2/targets.csv):
  συνεισφορά c = (payout_k − p)·sh (αγορά) ή (p − payout_k)·sh (πώληση)·  Σc = gross της φάσης 1.
  κάδοι: rel (χρόνος από την έναρξη του παραθύρου), price, size (notional $)· ανά ρόλο (mk/tk).
  tracks: all, s50, s100, s200 (fills με notional ≤ όριο), s100_no10 (≤ $100 και rel < 290 s).
     ανά track: net (c − fee + rebate), όγκος, μέγιστο κεφάλαιο σε κίνδυνο μέσα στη μέρα.
  dir: κατευθυντική θέση στην κρίση (Up-ισοδύναμο X = U − D) — track all.
  q6: edge εισόδου έναντι «δίκαιης» πιθανότητας από Binance BTCUSDT 1s (μόνο 0 ≤ rel < 300).
Έλεγχος συνέπειας: phase1_net από out/wallets/day=D.csv.gz.
"""
import argparse, bisect, csv, gzip, hashlib, heapq, io, json, math, os, sys, time, zipfile
from collections import defaultdict
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))

REL_EDGES = [0, 60, 180, 240, 270, 290, 300]
REL_LABELS = ["pre", "0-60", "60-180", "180-240", "240-270", "270-290", "290-300", "post"]
SIZE_EDGES = [10, 50, 100, 500, 2000]
SIZE_LABELS = ["<10", "10-50", "50-100", "100-500", "500-2000", ">2000"]
PRICE_LABELS = [f"{i/10:.1f}-{(i+1)/10:.1f}" for i in range(10)]
TRACKS = ["all", "s50", "s100", "s200", "s100_no10"]


def rel_bin(rel):
    if rel < 0: return "pre"
    for i in range(1, len(REL_EDGES)):
        if rel < REL_EDGES[i]: return REL_LABELS[i]
    return "post"


def size_bin(usd):  # [0,10) [10,50) [50,100) [100,500) [500,2000) [2000,∞)
    return SIZE_LABELS[bisect.bisect_right(SIZE_EDGES, usd)]


def price_bin(p):
    return PRICE_LABELS[min(int(p * 10), 9)] if p >= 0 else PRICE_LABELS[0]


def track_ok(tr, usd, rel):
    if tr == "all": return True
    if tr == "s50": return usd <= 50
    if tr == "s100": return usd <= 100
    if tr == "s200": return usd <= 200
    if tr == "s100_no10": return usd <= 100 and rel < 290
    raise ValueError(tr)


def norm_cdf(z): return 0.5 * math.erfc(-z / math.sqrt(2))


class Spot:
    """Binance BTCUSDT 1s closes σε συνεχές πλέγμα δευτερολέπτων (forward fill), με αθροιστικά για rolling σ."""
    def __init__(self, t0, closes):
        self.t0 = t0; self.c = closes
        r = [0.0] + [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
        self.s1 = [0.0]; self.s2 = [0.0]
        for x in r: self.s1.append(self.s1[-1] + x); self.s2.append(self.s2[-1] + x * x)

    def close_before(self, ts):
        i = ts - 1 - self.t0          # τελευταίο ΟΛΟΚΛΗΡΩΜΕΝΟ δευτερόλεπτο πριν το fill (χωρίς ματιά μπροστά)
        return self.c[i] if 0 <= i < len(self.c) else None

    def sigma(self, ts, win=3600):
        j = ts - 1 - self.t0; i = j - win
        if i < 1 or j >= len(self.c): return None
        n = j - i; s1 = self.s1[j + 1] - self.s1[i + 1]; s2 = self.s2[j + 1] - self.s2[i + 1]
        v = (s2 - s1 * s1 / n) / (n - 1)
        return math.sqrt(v) if v > 0 else None


def fair_up(spot, ts, T, K):
    """P(Up) = Φ(ln(S/K)/(σ√τ)). None αν δεν ορίζεται (προ-ανοίγματος, μετά το τέλος, χωρίς K/σ/S)."""
    if spot is None or K is None or ts < T or ts >= T + 300: return None
    S = spot.close_before(ts); sg = spot.sigma(ts); tau = T + 300 - ts
    if S is None or sg is None or tau <= 0: return None
    p = norm_cdf(math.log(S / K) / (sg * math.sqrt(tau)))
    return min(max(p, 1e-6), 1 - 1e-6)


class Agg:
    """Συσσώρευση χωρίς δίκτυο (δοκιμάζεται με συνθετικά δεδομένα).
    markets[i]: dict με T, payout[2], outcomes[2], fee_rate, rebate_rate, ptb."""
    def __init__(self, markets, targets, spot=None):
        self.m = markets; self.targets = targets; self.spot = spot
        self.mk_fe = defaultdict(float); self.tk_fee = defaultdict(float)   # όλα τα πορτοφόλια (για rebate)
        z5 = lambda: [0.0, 0.0, 0.0, 0.0, 0]  # usd, sh, c, fee, n
        self.bins = defaultdict(z5)            # (w, bt, label, role) -> z5
        self.bin_fe = defaultdict(float)       # (w, mi, bt, label, role) -> fee-equivalent (maker)
        self.tr = defaultdict(lambda: [0.0, 0.0, 0.0, 0, 0.0])  # (w, tr) -> c, fee, usd, n, maxcap
        self.tr_fe = defaultdict(float)        # (w, mi, tr) -> fe
        self.pos = {}                          # (w, mi, tr) -> [cash, shU0, sh1]
        self.cap_open = {}                     # (w, tr) -> {mi: cap}
        self.cap_tot = defaultdict(float)
        self.cap_heap = defaultdict(list)      # (w, tr) -> [(end, mi)]
        self.dirpos = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])  # (w,mi) -> sum_pos_s, sum_pos_sx, sum_neg_s, sum_neg_sx
        self.q6 = defaultdict(lambda: [0.0, 0.0, 0, 0])  # (w, role, relbin) -> Σ edge·sh, Σ sh, n, n_na

    def market_fill(self, mi, is_taker, fee, sh, p):
        """Για ΚΑΘΕ fill της αγοράς (όλα τα πορτοφόλια) — όπως meter.py."""
        if is_taker: self.tk_fee[mi] += fee
        else: self.mk_fe[mi] += sh * self.m[mi]["fee_rate"] * p * (1 - p)

    def _cap_update(self, key, mi, ts, newcap):
        op = self.cap_open.setdefault(key, {})
        h = self.cap_heap[key]
        while h and h[0][0] <= ts:                       # αγορές που έληξαν πριν από αυτό το fill: αποδεσμεύονται
            end, mj = heapq.heappop(h)
            if mj in op: self.cap_tot[key] -= op.pop(mj)
        old = op.get(mi)
        if old is None:
            heapq.heappush(h, (self.m[mi]["T"] + 300, mi)); old = 0.0
        op[mi] = newcap; self.cap_tot[key] += newcap - old
        return self.cap_tot[key]

    def fill(self, w, mi, k, side, usd, sh, fee, ts, is_taker):
        """Fill πορτοφολιού της λίστας. side 0 = αγορά, 1 = πώληση. k = δείκτης token (0/1)."""
        m = self.m[mi]; p = usd / sh; pay = m["payout"][k]
        c = (pay - p) * sh if side == 0 else (p - pay) * sh
        role = "tk" if is_taker else "mk"
        fe = 0.0 if is_taker else sh * m["fee_rate"] * p * (1 - p)
        rel = ts - m["T"]
        labels = {"rel": rel_bin(rel), "price": price_bin(p), "size": size_bin(usd)}
        for bt, lb in labels.items():
            B = self.bins[(w, bt, lb, role)]; B[0] += usd; B[1] += sh; B[2] += c; B[3] += fee; B[4] += 1
            if fe: self.bin_fe[(w, mi, bt, lb, role)] += fe
        for tr in TRACKS:
            if not track_ok(tr, usd, rel): continue
            R = self.tr[(w, tr)]; R[0] += c; R[1] += fee; R[2] += usd; R[3] += 1
            if fe: self.tr_fe[(w, mi, tr)] += fe
            P = self.pos.setdefault((w, mi, tr), [0.0, 0.0, 0.0])
            if side == 0: P[0] -= usd; P[1 + k] += sh
            else: P[0] += usd; P[1 + k] -= sh
            if rel < 300:   # αισιόδοξα: το κεφάλαιο αποδεσμεύεται στο τέλος του παραθύρου (T+300)
                cap = max(0.0, -P[0] + max(0.0, -P[1]) + max(0.0, -P[2]))
                tot = self._cap_update((w, tr), mi, ts, cap)
                if tot > R[4]: R[4] = tot
        # κατεύθυνση (Up-ισοδύναμο): αγορά Up = +sh @p· πώληση Up = −sh @p· αγορά Down = −sh @(1−p)· πώληση Down = +sh @(1−p)
        up = m["outcomes"][k].lower() == "up"
        s = (sh if side == 0 else -sh) * (1 if up else -1)
        x = p if up else 1 - p
        D = self.dirpos[(w, mi)]
        if s > 0: D[0] += s; D[1] += s * x
        else: D[2] += s; D[3] += s * x
        # q6
        if 0 <= rel < 300:
            Q = self.q6[(w, role, labels["rel"])]
            pu = fair_up(self.spot, ts, m["T"], m.get("ptb"))
            if pu is None: Q[3] += 1
            else:
                pk = pu if up else 1 - pu
                e = (pk - p) if side == 0 else (p - pk)
                Q[0] += e * sh; Q[1] += sh; Q[2] += 1

    def rebate_factor(self, mi):
        return self.m[mi]["rebate_rate"] * self.tk_fee[mi] / self.mk_fe[mi] if self.mk_fe[mi] > 0 else 0.0

    def finalize(self):
        out = defaultdict(lambda: {"bins": {}, "tracks": {}, "dir": None, "q6": {}})
        breb = defaultdict(float)
        for (w, mi, bt, lb, role), fe in self.bin_fe.items(): breb[(w, bt, lb, role)] += fe * self.rebate_factor(mi)
        for (w, bt, lb, role), B in self.bins.items():
            out[w]["bins"].setdefault(bt, {}).setdefault(lb, {})[role] = [round(B[0], 6), round(B[1], 6), round(B[2], 6), round(B[3], 6), round(breb[(w, bt, lb, role)], 6), B[4]]
        treb = defaultdict(float)
        for (w, mi, tr), fe in self.tr_fe.items(): treb[(w, tr)] += fe * self.rebate_factor(mi)
        for (w, tr), R in self.tr.items():
            net = R[0] - R[1] + treb[(w, tr)]
            out[w]["tracks"][tr] = {"gross": round(R[0], 6), "fee": round(R[1], 6), "reb": round(treb[(w, tr)], 6), "net": round(net, 6),
                                    "usd": round(R[2], 6), "n": R[3], "maxcap": round(R[4], 6)}
        # κατεύθυνση: X·(pU − VWAP της πλευράς του X)
        dd = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0.0, 0])  # abs X, Σ|s|, win-weighted |X|, dir pnl, gross, markets
        for (w, mi), D in self.dirpos.items():
            m = self.m[mi]; iu = [o.lower() for o in m["outcomes"]].index("up"); pU = m["payout"][iu]
            X = D[0] + D[2]; tot = D[0] - D[2]
            gross = (D[0] * pU - D[1]) + (D[2] * pU - D[3])
            if X > 0: vw = D[1] / D[0]
            elif X < 0: vw = D[3] / D[2]
            else: vw = 0.0
            dpnl = X * (pU - vw)
            win = abs(X) if (X > 0 and pU == 1) or (X < 0 and pU == 0) else 0.0
            R = dd[w]; R[0] += abs(X); R[1] += tot; R[2] += win; R[3] += dpnl; R[4] += gross; R[5] += 1
        for w, R in dd.items():
            out[w]["dir"] = {"absX": round(R[0], 6), "abs_flow": round(R[1], 6), "absX_won": round(R[2], 6), "dir_pnl": round(R[3], 6), "gross": round(R[4], 6), "markets": R[5]}
        for (w, role, lb), Q in self.q6.items():
            out[w]["q6"].setdefault(role, {})[lb] = [round(Q[0], 6), round(Q[1], 6), Q[2], Q[3]]
        return dict(out)


# ---------------- Binance 1s ----------------
def load_spot(day, http):
    """BTCUSDT 1s klines για D−1, D, D+1 από data.binance.vision, με έλεγχο .CHECKSUM και assertion εύρους χρόνου."""
    d = datetime.strptime(day, "%Y-%m-%d").date(); rows = {}; info = []
    for dd in (d - timedelta(days=1), d, d + timedelta(days=1)):
        name = f"BTCUSDT-1s-{dd.isoformat()}.zip"
        url = f"https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s/{name}"
        try:
            raw = http(url, tries=4); chk = http(url + ".CHECKSUM", tries=4).decode().split()[0]
        except Exception as e:
            info.append([dd.isoformat(), "missing", repr(e)[:120]]); continue
        ok = hashlib.sha256(raw).hexdigest() == chk
        info.append([dd.isoformat(), "ok" if ok else "BAD_CHECKSUM"])
        if not ok: continue
        z = zipfile.ZipFile(io.BytesIO(raw))
        for r in csv.reader(io.TextIOWrapper(z.open(z.namelist()[0]))):
            if not r or not r[0][0].isdigit(): continue
            t = int(r[0])
            t = t // 1_000_000 if t > 1e14 else (t // 1000 if t > 1e11 else t)   # µs (2025+) ή ms
            rows[t] = float(r[4])
    if not rows: return None, info
    t0, t1 = min(rows), max(rows)
    lo = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()) - 2 * 86400
    assert lo <= t0 and t1 <= lo + 5 * 86400, f"Binance χρόνος εκτός εύρους: {t0}..{t1}"
    closes = []; last = None
    for t in range(t0, t1 + 1):
        last = rows.get(t, last); closes.append(last)
    info.append(["seconds", len(closes), "filled", len(closes) - len(rows)])
    return Spot(t0, closes), info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True); ap.add_argument("--out", default="out")
    ap.add_argument("--targets", default=os.path.join(HERE, "targets.csv"))
    ap.add_argument("--dest", default=os.path.join(HERE, "out"))
    ap.add_argument("--pre-hours", type=int, default=3); ap.add_argument("--post-hours", type=int, default=2)
    a = ap.parse_args()
    sys.path.insert(0, os.path.join(HERE, ".."))
    import meter as MT
    day = a.date
    targets = {r["wallet"] for r in csv.DictReader(open(a.targets))}
    markets, missing = MT.load_markets(day)
    for m in markets:
        try: m["ptb"] = float(m["ptb"]) if m.get("ptb") is not None else None
        except Exception: m["ptb"] = None
    spot, spot_info = load_spot(day, MT.http)
    print(f"markets {len(markets)} missing {len(missing)} targets {len(targets)} spot {spot_info}", flush=True)
    tokmap = {}
    for i, m in enumerate(markets):
        for k, t in enumerate(m["tokens"]): tokmap[format(int(t), "064x")] = (i, k)
    d0 = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    ts_from, ts_to = d0 - a.pre_hours * 3600, d0 + 86400 + a.post_hours * 3600
    b0, b1 = MT.find_block(ts_from) - 5, MT.find_block(ts_to) + 5
    G = Agg(markets, targets, spot)
    cur, nreq, nmatch, ntarget = b0, 0, 0, 0
    while cur < b1:
        body = MT.sqd_post(cur, b1 - 1); nreq += 1; last = None
        for line in body.splitlines():
            if not line.strip(): continue
            o = json.loads(line); hd = o["header"]; last = hd["number"]; ts = hd["timestamp"]
            if ts < ts_from or ts >= ts_to: continue
            for lg in o.get("logs", ()):
                dt = lg["data"]; hit = tokmap.get(dt[66:130])
                if hit is None: continue
                mi, ki = hit; tp = lg["topics"]
                maker = "0x" + tp[2][-40:]; is_taker = ("0x" + tp[3][-40:]) == lg["address"].lower()
                side = int(dt[2:66], 16); a1 = int(dt[130:194], 16); a2 = int(dt[194:258], 16); fee = int(dt[258:322], 16) / 1e6
                if side == 0: usd, sh = a1 / 1e6, a2 / 1e6
                else: usd, sh = a2 / 1e6, a1 / 1e6
                if sh <= 0: continue
                nmatch += 1
                G.market_fill(mi, is_taker, fee, sh, usd / sh)
                if maker in targets:
                    ntarget += 1; G.fill(maker, mi, ki, side, usd, sh, fee, ts, is_taker)
        if last is None: raise RuntimeError(f"κενή απάντηση στο block {cur}")
        cur = last + 1
        if nreq % 10 == 0: print(f"req {nreq} block {cur} matched {nmatch} target {ntarget}", flush=True)
    res = G.finalize()
    # έλεγχος συνέπειας με τη φάση 1
    p1 = {}
    f1 = os.path.join(a.out, "wallets", f"day={day}.csv.gz")
    if os.path.exists(f1):
        with gzip.open(f1, "rt") as fh:
            for r in csv.DictReader(fh):
                if r["wallet"] in targets: p1[r["wallet"]] = float(r["net"])
    for w in set(res) | set(p1):
        res.setdefault(w, {"bins": {}, "tracks": {}, "dir": None, "q6": {}})["phase1_net"] = p1.get(w)
    os.makedirs(a.dest, exist_ok=True)
    doc = {"day": day, "markets": len(markets), "missing_windows": missing, "spot": spot_info, "stream_requests": nreq,
           "fills_matched": nmatch, "fills_target": ntarget, "wallets": res}
    with gzip.open(os.path.join(a.dest, f"day={day}.json.gz"), "wt") as f: json.dump(doc, f)
    bad = [w for w, r in res.items() if r.get("phase1_net") is not None and "all" in r["tracks"]
           and abs(r["tracks"]["all"]["net"] - r["phase1_net"]) > 0.005 * abs(r["phase1_net"]) + 0.01]
    print(f"done: reqs {nreq} matched {nmatch} target {ntarget} wallets {len(res)} consistency_bad {len(bad)} {bad[:5]}", flush=True)


if __name__ == "__main__":
    main()
