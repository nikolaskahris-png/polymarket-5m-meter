import json, os, time, urllib.request
from datetime import datetime, timezone

def get(url, tries=4):
    for a in range(tries):
        try:
            return json.loads(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "meter-diag"}), timeout=60).read())
        except Exception as e:
            err = repr(e)[:120]; time.sleep(2 + a * 2)
    return {"error": err}

def trades(cond):
    rows, cur = [], None
    for _ in range(60):
        j = get("https://data-api.polymarket.com/v2/trades?conditionId=" + cond + "&limit=500" + ("&cursor=" + cur if cur else ""))
        rows += j.get("data") or []
        pg = j.get("pagination") or {}; cur = pg.get("next_cursor")
        if not cur or not pg.get("has_more"): break
    return rows

out = []
for day in ["2026-08-10", "2026-09-15", "2026-10-20"]:
    t0 = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    for k in [0, 1, 4, 12, 36, 80, 140, 200, 250, 285]:
        T = t0 + k * 300
        ev = get("https://gamma-api.polymarket.com/events?slug=btc-updown-5m-" + str(T))
        if not isinstance(ev, list) or not ev: out.append({"T": T, "err": "no event"}); continue
        e = ev[0]; m = e["markets"][0]
        rows = trades(m["conditionId"])
        tot = sum(r["size"] for r in rows) or 1
        lead = lambda h: round(100 * sum(r["size"] for r in rows if r["timestamp"] < T - h * 3600) / tot, 3)
        first = min((r["timestamp"] for r in rows), default=None)
        out.append({"day": day, "k": k, "T": T, "rows": len(rows), "vol": round(tot, 1), "earliest_lead_h": None if first is None else round((T - first) / 3600, 2),
                    "pct_before_3h": lead(3), "pct_before_6h": lead(6), "pct_before_12h": lead(12), "pct_before_24h": lead(24),
                    "startDate": m.get("startDate"), "createdAt": m.get("createdAt") or e.get("createdAt"), "eventStart": (e.get("eventMetadata") or {}).get("startTime") or e.get("startTime")})
        print(json.dumps(out[-1]), flush=True)
os.makedirs("diag", exist_ok=True)
json.dump(out, open("diag/result2.json", "w"), indent=1)
