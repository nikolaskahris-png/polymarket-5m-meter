"""Συνθετικά δεδομένα με γνωστή απάντηση για phase2/fills.py (Agg) και phase2/analyze_phase2.py. Τρέχει χωρίς δίκτυο.
python phase2/test_synthetic_phase2.py  ->  all checks passed"""
import csv, gzip, json, math, os, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import fills as F

FAIL = []
def chk(name, got, exp, tol=1e-6):
    ok = (got == exp) if isinstance(exp, str) or exp is None else (got is not None and abs(got - exp) <= tol)
    print(("ok  " if ok else "FAIL"), name, got, exp)
    if not ok: FAIL.append(name)

# 1. κάδοι
for v, e in [(-1, "pre"), (0, "0-60"), (59.9, "0-60"), (60, "60-180"), (289, "270-290"), (290, "290-300"), (299.9, "290-300"), (300, "post")]: chk(f"rel {v}", F.rel_bin(v), e)
for v, e in [(9.99, "<10"), (10, "10-50"), (49.9, "10-50"), (100, "100-500"), (1999, "500-2000"), (2000, ">2000")]: chk(f"size {v}", F.size_bin(v), e)
for v, e in [(0.0, "0.0-0.1"), (0.45, "0.4-0.5"), (0.95, "0.9-1.0"), (1.0, "0.9-1.0")]: chk(f"price {v}", F.price_bin(v), e)

# 2. Agg
M = [{"T": 1000, "outcomes": ["Up", "Down"], "payout": [1.0, 0.0], "fee_rate": 0.07, "rebate_rate": 0.2, "ptb": None},
     {"T": 1300, "outcomes": ["Up", "Down"], "payout": [0.0, 1.0], "fee_rate": 0.07, "rebate_rate": 0.2, "ptb": None}]
G = F.Agg(M, {"A"})
def mf(mi, w, k, side, usd, sh, fee, ts, tk):
    G.market_fill(mi, tk, fee, sh, usd / sh)
    if w == "A": G.fill(w, mi, k, side, usd, sh, fee, ts, tk)
mf(0, "A", 0, 0, 4.0, 10, 0.0, 1010, False)    # A maker: αγορά Up 10 @0.4
mf(0, "A", 1, 0, 5.0, 10, 0.1, 1100, True)     # A taker: αγορά Down 10 @0.5, fee 0.1
mf(0, "B", 0, 0, 5.0, 10, 0.4, 1200, True)     # B taker fee 0.4
mf(0, "C", 1, 1, 5.0, 10, 0.0, 1200, False)    # C maker (αντισυμβαλλόμενος) fe = 10·0.07·0.25 = 0.175
mf(1, "A", 0, 1, 140.0, 200, 0.0, 1310, False) # A maker: πώληση Up 200 @0.7 (από split) -> κεφάλαιο 60
mf(1, "A", 0, 0, 0.9, 1, 0.0, 1595, False)     # A maker: αγορά Up 1 @0.9 στο rel 295
R = G.finalize()["A"]
reb0 = 0.2 * 0.5 * 0.168 / (0.168 + 0.175)
# rebate αγοράς 1: tk_fee = 0 -> 0
chk("all gross", R["tracks"]["all"]["gross"], 1.0 + 140.0 - 0.9)
chk("all fee", R["tracks"]["all"]["fee"], 0.1)
chk("all reb", R["tracks"]["all"]["reb"], round(reb0, 6))
chk("all net", R["tracks"]["all"]["net"], round(141.0 - 0.9 - 0.1 + reb0, 6))
chk("all maxcap", R["tracks"]["all"]["maxcap"], 60.0)
chk("s100 gross", R["tracks"]["s100"]["gross"], 1.0 - 0.9)
chk("s100 maxcap", R["tracks"]["s100"]["maxcap"], 9.0)
chk("s200 maxcap", R["tracks"]["s200"]["maxcap"], 60.0)
chk("s50 gross", R["tracks"]["s50"]["gross"], 1.0 - 0.9)
chk("no10 gross", R["tracks"]["s100_no10"]["gross"], 1.0)
chk("no10 n", R["tracks"]["s100_no10"]["n"], 2)
chk("dir absX", R["dir"]["absX"], 199.0)
chk("dir pnl", R["dir"]["dir_pnl"], 139.3)
chk("dir gross", R["dir"]["gross"], 140.1)   # = Σc: 1 + 139.1
chk("dir won", R["dir"]["absX_won"], 199.0)
chk("rel bin mk 0-60 c", R["bins"]["rel"]["0-60"]["mk"][2], 6.0 + 140.0)  # ts 1010 (αγορά 0) + ts 1310 (αγορά 1)
chk("rel bin mk 0-60 reb", R["bins"]["rel"]["0-60"]["mk"][4], round(reb0, 6))
chk("rel bin tk 60-180 c", R["bins"]["rel"]["60-180"]["tk"][2], -5.0)
chk("rel bin mk 290-300 c", R["bins"]["rel"]["290-300"]["mk"][2], -0.9)
chk("size bin 100-500 c", R["bins"]["size"]["100-500"]["mk"][2], 140.0)
chk("q6 unverified (no spot)", R["q6"]["mk"]["0-60"][3], 2)
# Σ c σε όλους τους κάδους rel = gross
chk("Σ bins = gross", sum(v[2] for L in R["bins"]["rel"].values() for v in L.values()), 140.1)

# 3. δίκαιη πιθανότητα: αποδόσεις ±0,001 -> σ = 0,001·√(n/(n−1)) ~ 0,001
closes = [100.0 * math.exp(0.001 * (i % 2)) for i in range(8000)]
S = F.Spot(0, closes)
ts = 5001; Sx = S.close_before(ts); sg = S.sigma(ts); T = 4900; tau = T + 300 - ts
K = Sx / math.exp(1.0 * sg * math.sqrt(tau))
chk("sigma ~0.001", sg, 0.001, 1e-6)
chk("fair_up = Φ(1)", F.fair_up(S, ts, T, K), 0.841344746, 1e-6)
chk("fair_up προ-ανοίγματος", F.fair_up(S, 4899, T, K), None)
chk("fair_up μετά το τέλος", F.fair_up(S, 5200, T, K), None)
chk("close_before = c[ts-1]", S.close_before(10), closes[9])

# 4. ανάλυση: 2 πορτοφόλια H1+W, M, net 10/μέρα, maxcap 100 -> ROC = 20n/200·30,4375/n = 3,04375 -> €1.521,875
with tempfile.TemporaryDirectory() as td:
    src = os.path.join(td, "out"); res = os.path.join(td, "res"); os.makedirs(src)
    tg = os.path.join(td, "t.csv")
    with open(tg, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["wallet", "groups", "arch_full", "arch_h1"])
        w.writerow(["0xa", "H1+W", "M", "M"]); w.writerow(["0xb", "H1+W", "M", "M"]); w.writerow(["0xc", "C", "M", "M"])
    days = [f"2026-01-{i:02d}" for i in range(1, 11)]
    for d in days:
        tr = {t: {"gross": 10, "fee": 0, "reb": 0, "net": 10.0, "usd": 100, "n": 1, "maxcap": 100.0} for t in F.TRACKS}
        wr = {x: {"bins": {"rel": {"0-60": {"mk": [100, 200, 10, 0, 0, 1]}}}, "tracks": tr, "dir": None, "q6": {}, "phase1_net": 10.0} for x in ("0xa", "0xb", "0xc")}
        with gzip.open(os.path.join(src, f"day={d}.json.gz"), "wt") as f: json.dump({"day": d, "wallets": wr}, f)
    subprocess.run([sys.executable, os.path.join(HERE, "analyze_phase2.py"), "--src", src, "--targets", tg, "--res", res, "--start", days[0], "--end", days[-1]], check=True, capture_output=True)
    r = json.load(open(os.path.join(res, "results_phase2.json")))
    c = r["ceiling"]["M"]["s100"]
    chk("ceiling €", c["ceiling_eur"], 500 * 3.04375, 1e-6)
    chk("ceiling lo90", c["ceiling_eur_lo90"], 500 * 3.04375, 1e-6)
    chk("h2 days", r["h2"][0], "2026-01-06")
    chk("consistency", str(r["consistency"]["pass"]), "True")
    chk("decision passes M", ",".join(r["decision"]["archetypes_passing"]), "M")
    chk("bin net/$", r["describe"]["W_M"]["bins"][0]["net_per_usd"], 0.1)
    # αλλοίωση: phase1_net ≠ net -> consistency αποτυγχάνει -> κλείνει
    for d in days:
        p = os.path.join(src, f"day={d}.json.gz"); doc = json.load(gzip.open(p, "rt")); doc["wallets"]["0xa"]["phase1_net"] = 12.0
        json.dump(doc, gzip.open(p, "wt"))
    subprocess.run([sys.executable, os.path.join(HERE, "analyze_phase2.py"), "--src", src, "--targets", tg, "--res", res, "--start", days[0], "--end", days[-1]], check=True, capture_output=True)
    r = json.load(open(os.path.join(res, "results_phase2.json")))
    chk("consistency catches mismatch", str(r["decision"]["phase2_closes"]), "True")

print("all checks passed" if not FAIL else f"FAILED: {FAIL}")
sys.exit(1 if FAIL else 0)
