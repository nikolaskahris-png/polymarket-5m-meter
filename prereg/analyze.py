"""analyze.py — κλειδωμένο script ανάλυσης (βλ. PREREG.md). Διαβάζει out/wallets/day=*.csv.gz και out/markets/.
Χρήση: python analyze.py --out out --res results --start 2026-08-07 --end 2026-11-14 [--before-start 2026-07-08 --before-end 2026-08-06]
Τα ποσά είναι USD· το EUR/USD έρχεται από ECB (eurofxref-hist). Μόνο στάνταρντ βιβλιοθήκη."""
import argparse, csv, glob, gzip, io, math, os, statistics, sys, urllib.request, zipfile
from collections import defaultdict
from datetime import date, timedelta

MIN_MARKETS, MIN_DAYS, Q = 300, 14, 0.05

def load(outdir, d0, d1):
    W = defaultdict(lambda: {"days": {}, "mk_usd": 0.0, "tk_usd": 0.0, "fee": 0.0, "reb": 0.0, "gross": 0.0, "fills": 0, "markets": 0, "hours": [0] * 24})
    for f in sorted(glob.glob(os.path.join(outdir, "wallets", "day=*.csv.gz"))):
        day = os.path.basename(f)[4:14]
        if not (d0 <= day <= d1): continue
        with gzip.open(f, "rt") as fh:
            for r in csv.DictReader(fh):
                w = W[r["wallet"]]
                net = float(r["net"]); w["days"][day] = (net, float(r["net"]) - float(r["rebate_est"]), int(r["markets"]))
                w["mk_usd"] += float(r["mk_usd"]); w["tk_usd"] += float(r["tk_usd"]); w["fee"] += float(r["fee"]); w["reb"] += float(r["rebate_est"])
                w["gross"] += float(r["gross"]); w["fills"] += int(r["fills"]); w["markets"] += int(r["markets"])
                for i, c in enumerate(r["hours"].split("|")): w["hours"][i] += int(c)
    return W

def norm_sf(z): return 0.5 * math.erfc(z / math.sqrt(2))

def tstat(vals):
    n = len(vals)
    if n < 2: return 0.0, 1.0
    m = statistics.fmean(vals); sd = statistics.stdev(vals)
    if sd == 0: return (math.inf if m > 0 else 0.0), (0.0 if m > 0 else 1.0)
    t = m / (sd / math.sqrt(n)); return t, norm_sf(t)

def bh(pvals):
    """pvals: dict key->p. Επιστρέφει set κλειδιών που περνούν BH στο Q."""
    items = sorted(pvals.items(), key=lambda kv: kv[1]); m = len(items); k = 0
    for i, (_, p) in enumerate(items, 1):
        if p <= Q * i / m: k = i
    return {key for key, _ in items[:k]}

def winners(W, dates=None):
    cand, p = {}, {}
    for a, w in W.items():
        days = {d: v for d, v in w["days"].items() if dates is None or d in dates}
        nm = sum(v[2] for v in days.values())
        if nm < MIN_MARKETS or len(days) < MIN_DAYS: continue
        vals = [v[0] for v in days.values()]
        if sum(vals) <= 0: continue
        cand[a] = days; p[a] = tstat(vals)[1]
    return cand, bh(p), p

def ecb_rate(d0, d1):
    try:
        raw = urllib.request.urlopen("https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip", timeout=60).read()
        z = zipfile.ZipFile(io.BytesIO(raw)); rows = list(csv.DictReader(io.TextIOWrapper(z.open(z.namelist()[0]))))
        v = [float(r["USD"]) for r in rows if d0 <= r["Date"] <= d1 and r.get("USD") not in (None, "", "N/A")]
        return statistics.fmean(v), len(v)   # USD ανά 1 EUR
    except Exception as e:
        return None, repr(e)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="out"); ap.add_argument("--res", default="results")
    ap.add_argument("--start", default="2026-08-07"); ap.add_argument("--end", default="2026-11-14")
    ap.add_argument("--before-start"); ap.add_argument("--before-end")
    a = ap.parse_args(); os.makedirs(a.res, exist_ok=True)
    s, e = date.fromisoformat(a.start), date.fromisoformat(a.end)
    ndays = (e - s).days + 1; months = ndays / 30.4375
    mid = s + timedelta(days=ndays // 2)
    alld = {(s + timedelta(days=i)).isoformat() for i in range(ndays)}
    h1 = {d for d in alld if date.fromisoformat(d) < mid}; h2 = alld - h1
    W = load(a.out, a.start, a.end)
    cand, passbh, p = winners(W)
    win = []
    for x in cand:
        d = cand[x]; n1 = sum(v[0] for k, v in d.items() if k in h1); n2 = sum(v[0] for k, v in d.items() if k in h2)
        if x in passbh and n1 > 0 and n2 > 0: win.append(x)
    tot_usd = sum(sum(v[0] for v in W[x]["days"].values()) for x in win)
    tot_all = sum(sum(v[0] for v in w["days"].values()) for w in W.values())
    rate, nr = ecb_rate(a.start, a.end)
    # εκτός δείγματος: νικητές από πρώτο μισό -> κέρδος στο δεύτερο
    W1 = {x: {"days": {k: v for k, v in w["days"].items() if k in h1}} for x, w in W.items()}
    cand1, bh1, _ = winners(W1, h1)
    oos = [x for x in cand1 if x in bh1]
    oos_usd = sum(sum(v[0] for k, v in W[x]["days"].items() if k in h2) for x in oos)
    half_months = len(h2) / 30.4375
    def eur(u, mo): return None if rate is None else u / rate / mo
    # CSV σε μορφή pancake + επιπλέον στήλες
    hdr = ["πορτοφόλι", "πονταρίσματα", "USDC που πόνταρε", "καθαρό κέρδος USDC", "απόδοση %", "ποσοστό νικών %", "z (περίπου)", "πρώτο ποντάρισμα", "τελευταίο ποντάρισμα",
           "% maker", "fees πληρωμένα", "rebates (εκτίμηση)", "μέσο μέγεθος", "ώρες δραστηριότητας (UTC)"]
    rows = []
    for x in win:
        w = W[x]; days = w["days"]; vol = w["mk_usd"] + w["tk_usd"]; net = sum(v[0] for v in days.values())
        dv = [v[0] for v in days.values()]; t, _ = tstat(dv)
        wins = 100 * sum(1 for v in dv if v > 0) / len(dv)
        top = sorted(range(24), key=lambda i: -w["hours"][i])[:3]
        rows.append([x, w["markets"], round(vol, 2), round(net, 2), round(100 * net / vol, 3) if vol else 0, round(wins, 2), round(t, 2), min(days), max(days),
                     round(100 * w["mk_usd"] / vol, 1) if vol else 0, round(w["fee"], 2), round(w["reb"], 2), round(vol / w["markets"], 2) if w["markets"] else 0, " ".join(f"{h:02d}" for h in sorted(top))])
    rows.sort(key=lambda r: -r[3])
    with open(os.path.join(a.res, "kerdismenoi_5m.csv"), "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f, delimiter=";"); wr.writerow(hdr); wr.writerows(rows)
    # πριν/μετά
    before = {}
    if a.before_start and a.before_end:
        B = load(a.out, a.before_start, a.before_end)
        wl = [x for x in win if x in B and sum(v[2] for v in B[x]["days"].values()) >= 100]
        pos = sum(1 for x in wl if sum(v[0] for v in B[x]["days"].values()) > 0)
        both = [x for x in W if x in B]
        def rank(v): 
            o = sorted(range(len(v)), key=lambda i: v[i]); r = [0] * len(v)
            for i, j in enumerate(o): r[j] = i
            return r
        sp = None
        if len(both) > 5:
            r1 = rank([sum(v[0] for v in W[x]["days"].values()) for x in both]); r2 = rank([sum(v[0] for v in B[x]["days"].values()) for x in both])
            m1, m2 = statistics.fmean(r1), statistics.fmean(r2)
            num = sum((u - m1) * (v - m2) for u, v in zip(r1, r2)); den = math.sqrt(sum((u - m1) ** 2 for u in r1) * sum((v - m2) ** 2 for v in r2))
            sp = num / den if den else None
        before = {"winners_with_100plus_before": len(wl), "of_which_positive_before": pos, "wallets_in_both": len(both), "spearman_net": sp}
    res = {"period": [a.start, a.end], "days": ndays, "wallets_total": len(W), "wallets_tested": len(cand), "pass_BH": len(passbh), "winners": len(win),
           "usd_total_all_wallets": round(tot_all, 2), "winners_usd_total": round(tot_usd, 2), "winners_usd_per_month": round(tot_usd / months, 2),
           "eur_per_usd_inverse_rate_USD_per_EUR": rate, "ecb_days_or_err": nr, "winners_eur_per_month": eur(tot_usd, months),
           "oos_winners_selected_first_half": len(oos), "oos_usd_second_half": round(oos_usd, 2), "oos_eur_per_month": eur(oos_usd, half_months),
           "stop_rule_a_below_5000": (None if eur(tot_usd, months) is None else eur(tot_usd, months) < 5000), "stop_rule_b_no_wallet_passes": len(win) == 0, "before_after": before}
    import json; json.dump(res, open(os.path.join(a.res, "results.json"), "w"), indent=1, ensure_ascii=False, default=str)
    print(json.dumps(res, indent=1, ensure_ascii=False, default=str))
    # top 20 για RESULTS
    with open(os.path.join(a.res, "top20.csv"), "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f, delimiter=";"); wr.writerow(hdr); wr.writerows(rows[:20])

if __name__ == "__main__": main()
