"""Probe: τι φτάνει από τον GitHub runner; Γράφει probe/result.json (καμία μυστική τιμή)."""
import gzip, json, os, sys, time, urllib.request, urllib.error
from datetime import datetime, timezone

OUT = {"started": datetime.now(timezone.utc).isoformat(), "tests": {}}

def get(url, headers=None, data=None, timeout=40):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    req.add_header("User-Agent", "polymarket-5m-meter/0.1")
    req.add_header("Accept-Encoding", "gzip")
    t = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            return {"status": r.status, "bytes": len(raw), "secs": round(time.time() - t, 2),
                    "hdr": {k: v for k, v in r.headers.items() if k.lower().startswith("x-sqd") or k.lower() == "retry-after"}, "body": raw}
    except urllib.error.HTTPError as e:
        b = e.read()[:300]
        return {"status": e.code, "bytes": len(b), "secs": round(time.time() - t, 2), "hdr": {}, "body": b}
    except Exception as e:
        return {"status": None, "error": repr(e)[:200], "secs": round(time.time() - t, 2), "hdr": {}, "body": b""}

def rec(name, r, keep_body=False, extra=None):
    d = {k: v for k, v in r.items() if k != "body"}
    if keep_body: d["body_head"] = r["body"][:400].decode("utf-8", "replace")
    if extra: d.update(extra)
    OUT["tests"][name] = d
    print(name, d.get("status"), d.get("bytes"), flush=True)

# 1. gamma: μία πρόσφατη και μία παλιά (Αύγουστος) αγορά
now = int(time.time()); T_recent = (now // 300) * 300 - 3600
T_old = 1786320000 // 300 * 300  # ~2026-08-10
cond = None
for name, T in (("gamma_recent", T_recent), ("gamma_old", T_old)):
    r = get(f"https://gamma-api.polymarket.com/events?slug=btc-updown-5m-{T}")
    ex = {}
    try:
        j = json.loads(r["body"]); m = j[0]["markets"][0]
        ex = {"conditionId": m.get("conditionId"), "clobTokenIds": m.get("clobTokenIds"), "outcomes": m.get("outcomes"),
              "outcomePrices": m.get("outcomePrices"), "closed": m.get("closed"), "volume": m.get("volume"), "keys": sorted(m.keys())[:80]}
        if name == "gamma_old": cond = m.get("conditionId")
    except Exception as e:
        ex = {"parse_error": repr(e)[:150]}
    rec(name, r, keep_body=not ex.get("conditionId"), extra=ex)
    time.sleep(0.5)

# 2. data-api
if cond:
    r = get(f"https://data-api.polymarket.com/trades?market={cond}&limit=5")
    rec("data_api_trades", r, keep_body=True)
r = get("https://clob.polymarket.com/time"); rec("clob_time", r, keep_body=True)

# 3. Hugging Face (εφεδρική πηγή αγορών)
r = get("https://huggingface.co/api/datasets/aliplayer1/polymarket-crypto-updown"); rec("hf_dataset_api", r)

# 4. SQD: μικρό δείγμα + μέτρηση μεγέθους
EX = ["0xE111180000d2663C0091e4f400237545B87B996B", "0xe2222d279d744050d28e00520010520000310F59",
      "0xe2222d002000ba0053cef3375333610f64600036", "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"]
TOPIC_FILLED = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
def sqd(frm, to, topics=True, key=None):
    body = {"type": "evm", "fromBlock": frm, "toBlock": to,
            "fields": {"block": {"number": True, "timestamp": True}, "log": {"address": True, "topics": True, "data": True}},
            "logs": [dict({"address": EX}, **({"topic0": [TOPIC_FILLED]} if topics else {}))]}
    h = {"content-type": "application/json"}
    if key: h["Authorization"] = "Bearer " + key
    return get("https://portal.sqd.dev/datasets/polygon-mainnet/stream", headers=h, data=json.dumps(body).encode(), timeout=120)

for attempt in range(4):
    r = sqd(89900000, 89900299)
    if r["status"] == 200: break
    time.sleep(12)
nlog = nblk = 0; last = None
if r["status"] == 200:
    for line in r["body"].splitlines():
        if not line.strip(): continue
        o = json.loads(line); nblk += 1; last = o["header"]["number"]; nlog += len(o.get("logs", []))
rec("sqd_300_blocks_nokey", r, extra={"blocks_returned": nblk, "logs": nlog, "last_block": last,
    "est_bytes_per_day_this_density": int(r.get("bytes", 0) / max(nblk, 1) * 43200)})
key = os.environ.get("SQD_API_KEY")
if key:
    r2 = sqd(89900000, 89900299, key=key)
    rec("sqd_300_blocks_withkey", r2)
else:
    OUT["tests"]["sqd_300_blocks_withkey"] = "no SQD_API_KEY in this run"

os.makedirs("probe", exist_ok=True)
json.dump(OUT, open("probe/result.json", "w"), indent=1, ensure_ascii=False)
print("done")
