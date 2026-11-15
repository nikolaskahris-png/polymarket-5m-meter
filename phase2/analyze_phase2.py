"""phase2/analyze_phase2.py — ανάλυση φάσης 2 (PREREG_phase2.md). Μόνο στάνταρντ βιβλιοθήκη.
Διαβάζει phase2/targets.csv και phase2/out/day=*.json.gz. Γράφει phase2/results/results_phase2.json, tables_phase2.md, signals_phase2.jsonl.
Κάθε νούμερο του RESULTS_phase2.md βγαίνει από εδώ.
"""
import argparse, csv, glob, gzip, json, math, os, random, statistics
from collections import defaultdict
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
CAPITAL_EUR = 500.0
THRESH_EUR = 200.0
BOOT, BOOT_BINS, SEED = 2000, 500, 20261116
CEIL_TRACKS = {"s100": "κύριο (≤$100)", "s50": "πλατό ≤$50", "s200": "πλατό ≤$200", "s100_no10": "T χωρίς τα τελευταία 10 s"}


def halves(start, end):
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    nd = (e - s).days + 1; mid = s + timedelta(days=nd // 2)
    alld = [(s + timedelta(days=i)).isoformat() for i in range(nd)]
    h1 = [d for d in alld if date.fromisoformat(d) < mid]
    return alld, h1, [d for d in alld if d not in set(h1)]


def load_days(src, alld):
    D = {}; missing = []
    for d in alld:
        f = os.path.join(src, f"day={d}.json.gz")
        if not os.path.exists(f): missing.append(d); continue
        with gzip.open(f, "rt") as fh: D[d] = json.load(fh)
    return D, missing


def q(v, p):
    v = sorted(v);
    if not v: return None
    k = (len(v) - 1) * p; i = int(k); f = k - i
    return v[i] if i + 1 >= len(v) else v[i] * (1 - f) + v[i + 1] * f


def consistency(D, wallets):
    """§2: Σ net(track all) = Σ phase1_net ανά πορτοφόλι, ανοχή 0,5% + $1."""
    s2 = defaultdict(float); s1 = defaultdict(float); nd = defaultdict(int)
    for d, doc in D.items():
        for w, r in doc["wallets"].items():
            if w not in wallets: continue
            if r.get("phase1_net") is not None: s1[w] += r["phase1_net"]; nd[w] += 1
            if "all" in r["tracks"]: s2[w] += r["tracks"]["all"]["net"]
    bad = []
    for w in set(s1) | set(s2):
        if abs(s2[w] - s1[w]) > 0.005 * abs(s1[w]) + 1.0: bad.append([w, round(s1[w], 2), round(s2[w], 2)])
    return {"wallets_checked": len(set(s1) | set(s2)), "bad": bad, "pass": not bad}


def ceiling(D, days, wallets, tr, rng, boot=BOOT):
    """ROC/μήνα = Σ net ÷ Σ_w median(ημερήσιο maxcap, μέρες με δραστηριότητα), × 30,4375/μέρες. Block bootstrap ανά μέρα."""
    per = {w: {} for w in wallets}   # w -> day -> (net, maxcap)
    for d in days:
        doc = D.get(d)
        if not doc: continue
        for w in wallets:
            t = (doc["wallets"].get(w) or {}).get("tracks", {}).get(tr)
            if t: per[w][d] = (t["net"], t["maxcap"])
    def roc(sample_days):
        net = 0.0; cap = 0.0
        for w in wallets:
            caps = [per[w][d][1] for d in sample_days if d in per[w] and per[w][d][1] > 0]
            net += sum(per[w][d][0] for d in sample_days if d in per[w])
            if caps: cap += statistics.median(caps)
        return (net / cap) * 30.4375 / len(sample_days) if cap > 0 else 0.0, net, cap
    r0, net0, cap0 = roc(days)
    bs = [roc([rng.choice(days) for _ in days])[0] for _ in range(boot)]
    return {"track": tr, "wallets": len(wallets), "net_usd": round(net0, 2), "cap_usd_sum_median": round(cap0, 2), "roc_month": r0,
            "ceiling_eur": CAPITAL_EUR * r0, "ceiling_eur_lo90": CAPITAL_EUR * q(bs, 0.05), "ceiling_eur_hi90": CAPITAL_EUR * q(bs, 0.95)}


def bin_tables(D, days, wallets, rng):
    """Q1–Q3 ανά (bt, label, role): όγκος, net (c − fee + reb), net/$ με 90% διάστημα (bootstrap ανά μέρα)."""
    daily = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0, 0]))  # (bt,lb,role) -> day -> [usd, net, n]
    for d in days:
        doc = D.get(d)
        if not doc: continue
        for w in wallets:
            r = doc["wallets"].get(w)
            if not r: continue
            for bt, L in r["bins"].items():
                for lb, roles in L.items():
                    for role, v in roles.items():
                        x = daily[(bt, lb, role)][d]; x[0] += v[0]; x[1] += v[2] - v[3] + v[4]; x[2] += v[5]
    out = []
    tot_usd = defaultdict(float); tot_net = defaultdict(float)
    for (bt, lb, role), dd in daily.items():
        tot_usd[bt] += sum(x[0] for x in dd.values()); tot_net[bt] += sum(x[1] for x in dd.values())
    for (bt, lb, role), dd in sorted(daily.items()):
        usd = sum(x[0] for x in dd.values()); net = sum(x[1] for x in dd.values()); n = sum(x[2] for x in dd.values())
        ds = list(dd.keys())
        bs = []
        for _ in range(BOOT_BINS):
            sm = [rng.choice(ds) for _ in ds]; u = sum(dd[s][0] for s in sm)
            bs.append(sum(dd[s][1] for s in sm) / u if u else 0.0)
        out.append({"bt": bt, "bin": lb, "role": role, "n_fills": n, "usd": round(usd, 2), "net": round(net, 2),
                    "share_usd": usd / tot_usd[bt] if tot_usd[bt] else None, "share_net": net / tot_net[bt] if tot_net[bt] else None,
                    "net_per_usd": net / usd if usd else None, "lo90": q(bs, 0.05), "hi90": q(bs, 0.95)})
    return out


def dir_stats(D, days, wallets):
    S = [0.0] * 6
    for d in days:
        doc = D.get(d)
        if not doc: continue
        for w in wallets:
            r = (doc["wallets"].get(w) or {}).get("dir")
            if r:
                for i, k in enumerate(["absX", "abs_flow", "absX_won", "dir_pnl", "gross", "markets"]): S[i] += r[k]
    return {"markets": S[5], "absX_over_flow": S[0] / S[1] if S[1] else None, "win_rate_directional_weighted": S[2] / S[0] if S[0] else None,
            "dir_pnl_usd": round(S[3], 2), "gross_usd": round(S[4], 2), "dir_share_of_gross": S[3] / S[4] if S[4] else None}


def q6_stats(D, days, wallets):
    S = defaultdict(lambda: [0.0, 0.0, 0, 0])
    for d in days:
        doc = D.get(d)
        if not doc: continue
        for w in wallets:
            r = (doc["wallets"].get(w) or {}).get("q6") or {}
            for role, L in r.items():
                for lb, v in L.items():
                    x = S[(role, lb)]; x[0] += v[0]; x[1] += v[1]; x[2] += v[2]; x[3] += v[3]
    return [{"role": role, "bin": lb, "edge_per_share": v[0] / v[1] if v[1] else None, "n": v[2], "n_unverified": v[3]} for (role, lb), v in sorted(S.items())]


def roc_by_wallet(D, days, wallets):
    out = []
    for w in wallets:
        net = 0.0; caps = []; nd = 0
        for d in days:
            t = ((D.get(d) or {}).get("wallets", {}).get(w) or {}).get("tracks", {}).get("all")
            if t:
                net += t["net"]; nd += 1
                if t["maxcap"] > 0: caps.append(t["maxcap"])
        mc = statistics.median(caps) if caps else None
        out.append({"wallet": w, "days": nd, "net": round(net, 2), "median_maxcap": mc, "roc_month": (net / mc * 30.4375 / len(days)) if mc else None})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=os.path.join(HERE, "out")); ap.add_argument("--targets", default=os.path.join(HERE, "targets.csv"))
    ap.add_argument("--res", default=os.path.join(HERE, "results"))
    ap.add_argument("--start", default="2026-08-07"); ap.add_argument("--end", default="2026-11-14")
    a = ap.parse_args(); os.makedirs(a.res, exist_ok=True)
    rng = random.Random(SEED)
    T = list(csv.DictReader(open(a.targets)))
    alld, h1, h2 = halves(a.start, a.end)
    D, missing = load_days(a.src, alld)
    alld = [d for d in alld if d in D]; h2 = [d for d in h2 if d in D]   # μέρες που λείπουν αναφέρονται, δεν μετρούν ως μηδέν
    allw = {r["wallet"] for r in T}
    res = {"period": [a.start, a.end], "days_loaded": len(D), "days_missing": missing, "h1": [h1[0], h1[-1]], "h2": [h2[0], h2[-1]]}
    res["consistency"] = consistency(D, allw)
    grp = lambda g, key, ar: sorted(r["wallet"] for r in T if g in r["groups"].split("+") and (ar is None or r[key] == ar))
    sig = open(os.path.join(a.res, "signals_phase2.jsonl"), "w")
    # περιγραφικά: W και C, ανά αρχέτυπο (πλήρες εύρος)
    res["describe"] = {}
    for g in ("W", "C"):
        for ar in ("M", "X", "T"):
            ws = grp(g, "arch_full", ar)
            if not ws: continue
            key = f"{g}_{ar}"
            bins = bin_tables(D, alld, ws, rng)
            for b in bins: sig.write(json.dumps({"group": key, **b}) + "\n")
            rw = [x for x in roc_by_wallet(D, alld, ws) if x["roc_month"] is not None]
            res["describe"][key] = {"wallets": len(ws), "dir": dir_stats(D, alld, ws), "q6": q6_stats(D, alld, ws) if ar == "T" else None,
                                    "roc_month_median": statistics.median([x["roc_month"] for x in rw]) if rw else None,
                                    "median_maxcap_median": statistics.median([x["median_maxcap"] for x in rw]) if rw else None,
                                    "bins": bins}
    # ταβάνι: H1 νικητές, αρχέτυπο από το 1ο μισό, μέτρηση στο 2ο μισό
    res["ceiling"] = {}
    for ar in ("M", "X", "T"):
        ws = grp("H1", "arch_h1", ar)
        if not ws: continue
        res["ceiling"][ar] = {tr: ceiling(D, h2, ws, tr, rng) for tr in CEIL_TRACKS if tr != "s100_no10" or ar == "T"}
    sig.close()
    # κανόνας διακοπής §4
    passing = []
    for ar, C in res["ceiling"].items():
        fa = C["s100"]["ceiling_eur_lo90"] < THRESH_EUR
        fb = C["s50"]["ceiling_eur"] < THRESH_EUR or C["s200"]["ceiling_eur"] < THRESH_EUR
        fc = ar == "T" and C["s100_no10"]["ceiling_eur"] < THRESH_EUR
        res["ceiling"][ar]["fails"] = {"a_lo90_below": fa, "b_not_plateau": fb, "c_T_needs_last10s": fc}
        if not (fa or fb or fc): passing.append(ar)
    res["decision"] = {"consistency_pass": res["consistency"]["pass"], "archetypes_passing": passing,
                       "phase2_closes": (not res["consistency"]["pass"]) or not passing,
                       "note": "Αν consistency_pass = false: ΣΤΟΠ πριν από οποιοδήποτε συμπέρασμα (§4δ)."}
    json.dump(res, open(os.path.join(a.res, "results_phase2.json"), "w"), indent=1, ensure_ascii=False, default=str)
    # σύντομος πίνακας για ανάγνωση
    L = ["| αρχέτυπο | track | πορτοφόλια | ταβάνι €/μήνα | 90% κάτω | 90% πάνω |", "|---|---|---|---|---|---|"]
    for ar, C in res["ceiling"].items():
        for tr, v in C.items():
            if tr == "fails": continue
            L.append(f"| {ar} | {CEIL_TRACKS[tr]} | {v['wallets']} | {v['ceiling_eur']:.0f} | {v['ceiling_eur_lo90']:.0f} | {v['ceiling_eur_hi90']:.0f} |")
    open(os.path.join(a.res, "tables_phase2.md"), "w").write("\n".join(L) + "\n\n" + json.dumps(res["decision"], ensure_ascii=False) + "\n")
    print(json.dumps({k: res[k] for k in ("days_loaded", "days_missing", "consistency", "decision")}, ensure_ascii=False, default=str)[:3000])
    print("\n".join(L))


if __name__ == "__main__":
    main()
