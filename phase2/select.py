"""phase2/select.py — λίστα πορτοφολιών της φάσης 2 (βλ. PREREG_phase2.md §0, §1, §3).

Ομάδες:
  W  = οι 140 νικητές του results/kerdismenoi_5m.csv (sha256 ελέγχεται)
  H1 = νικητές επιλεγμένοι ΜΟΝΟ στο 1ο μισό, με τον ίδιο κώδικα της φάσης 1 (prereg/analyze.py: winners() στο h1)
  C  = 140 τυχαία πορτοφόλια (seed 20261116) που περνούν §3 της φάσης 1 στο πλήρες εύρος και δεν είναι W
Αρχέτυπο: M (maker ≥ 90% κατά όγκο), T (≤ 10%), X (ενδιάμεσα).
  arch_full = στο πλήρες εύρος· arch_h1 = μόνο με τις μέρες του 1ου μισού (για την ομάδα H1, χωρίς ματιά στο μέλλον).
Έξοδος: phase2/targets.csv
"""
import argparse, csv, gzip, glob, hashlib, os, random, sys
from collections import defaultdict
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "prereg"))
import analyze as A  # κλειδωμένο script της φάσης 1 (sha256 96733ab4…)

WINNERS_SHA = "caa847a16b64293a459ca9a07749b19da4ca9a503c79e1ff265aa7ededf05e18"
ANALYZE_SHA = "96733ab4a346b5da70ff791abfce3c104adf4ffcdd923d7c2a09bad5133fe6b8"
SEED, N_CONTROL = 20261116, 140


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def arch(mk, tk):
    v = mk + tk
    if v <= 0: return "NA"
    s = mk / v
    return "M" if s >= 0.9 else ("T" if s <= 0.1 else "X")


def halves(start, end):
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    nd = (e - s).days + 1; mid = s + timedelta(days=nd // 2)
    alld = [(s + timedelta(days=i)).isoformat() for i in range(nd)]
    h1 = {d for d in alld if date.fromisoformat(d) < mid}
    return alld, h1, set(alld) - h1


def maker_usd_by_day(outdir, d0, d1, wallets):
    """mk/tk usd ανά πορτοφόλι ανά μέρα (μόνο για όσα χρειάζονται)."""
    R = defaultdict(dict)
    for f in sorted(glob.glob(os.path.join(outdir, "wallets", "day=*.csv.gz"))):
        day = os.path.basename(f)[4:14]
        if not (d0 <= day <= d1): continue
        with gzip.open(f, "rt") as fh:
            for r in csv.DictReader(fh):
                if r["wallet"] in wallets:
                    R[r["wallet"]][day] = (float(r["mk_usd"]), float(r["tk_usd"]))
    return R


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="out"); ap.add_argument("--res", default="results")
    ap.add_argument("--start", default="2026-08-07"); ap.add_argument("--end", default="2026-11-14")
    ap.add_argument("--dest", default=os.path.join(HERE, "targets.csv"))
    a = ap.parse_args()
    wpath = os.path.join(a.res, "kerdismenoi_5m.csv")
    assert sha(wpath) == WINNERS_SHA, "kerdismenoi_5m.csv άλλαξε — ΣΤΟΠ"
    assert sha(os.path.join(HERE, "..", "prereg", "analyze.py")) == ANALYZE_SHA, "analyze.py άλλαξε — ΣΤΟΠ"
    with open(wpath, encoding="utf-8") as f:
        W = [r[0] for r in csv.reader(f, delimiter=";")][1:]
    assert len(W) == 140, len(W)

    alld, h1, h2 = halves(a.start, a.end)
    Wall = A.load(a.out, a.start, a.end)
    # H1: ίδια λογική με analyze.main() (εκτός δείγματος)
    W1 = {x: {"days": {k: v for k, v in w["days"].items() if k in h1}} for x, w in Wall.items()}
    cand1, bh1, _ = A.winners(W1, h1)
    H1 = sorted(x for x in cand1 if x in bh1)
    # C: περνούν §3 στο πλήρες εύρος, όχι νικητές
    elig = sorted(x for x, w in Wall.items()
                  if sum(v[2] for v in w["days"].values()) >= A.MIN_MARKETS and len(w["days"]) >= A.MIN_DAYS and x not in set(W))
    rng = random.Random(SEED)
    C = sorted(rng.sample(elig, N_CONTROL))

    groups = defaultdict(set)
    for x in W: groups[x].add("W")
    for x in H1: groups[x].add("H1")
    for x in C: groups[x].add("C")
    mk = maker_usd_by_day(a.out, a.start, a.end, set(groups))
    rows = []
    for x in sorted(groups):
        d = mk.get(x, {})
        full = arch(sum(v[0] for v in d.values()), sum(v[1] for v in d.values()))
        a1 = arch(sum(v[0] for k, v in d.items() if k in h1), sum(v[1] for k, v in d.items() if k in h1))
        rows.append([x, "+".join(sorted(groups[x])), full, a1])
    os.makedirs(os.path.dirname(a.dest), exist_ok=True)
    with open(a.dest, "w", newline="") as f:
        wr = csv.writer(f); wr.writerow(["wallet", "groups", "arch_full", "arch_h1"]); wr.writerows(rows)
    print(f"W {len(W)} H1 {len(H1)} C {len(C)} (eligible {len(elig)}) total {len(rows)}; h1 {min(h1)}..{max(h1)} h2 {min(h2)}..{max(h2)}")


if __name__ == "__main__":
    main()
