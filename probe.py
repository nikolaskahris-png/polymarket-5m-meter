"""Probe 2: λεπτομέρειες fee/rebate (gamma), data-api v2, docs, ρυθμός SQD σήμερα. Γράφει probe/result.json."""
import gzip, json, os, time, urllib.request, urllib.error
from datetime import datetime, timezone
OUT = {"started": datetime.now(timezone.utc).isoformat(), "tests": {}}

def get(url, headers=None, data=None, timeout=60):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    req.add_header("User-Agent", "polymarket-5m-meter/0.1"); req.add_header("Accept-Encoding", "gzip")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip": raw = gzip.decompress(raw)
            return r.status, raw
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:2000]
    except Exception as e:
        return None, repr(e).encode()

def save(name, status, body, n=3500):
    OUT["tests"][name] = {"status": status, "bytes": len(body), "head": body[:n].decode("utf-8", "replace")}
    print(name, status, len(body), flush=True)

T_old = 1786320000 // 300 * 300
st, b = get(f"https://gamma-api.polymarket.com/events?slug=btc-updown-5m-{T_old}")
cond = None
try:
    ev = json.loads(b)[0]; m = ev["markets"][0]; cond = m["conditionId"]
    OUT["tests"]["gamma_fee_fields"] = {k: m.get(k) for k in ("feeSchedule", "feeType", "feesEnabled", "makerBaseFee", "takerBaseFee", "makerRebatesFeeShareBps",
        "version", "negRisk", "closedTime", "eventStartTime", "endDate", "startDate", "volume", "volumeNum", "volumeClob", "resolutionSource", "cryptoMarketConfig", "umaResolutionStatus", "orderPriceMinTickSize")}
    OUT["tests"]["gamma_event_fields"] = {k: ev.get(k) for k in ("slug", "startDate", "endDate", "volume", "closed", "seriesSlug", "eventMetadata", "cryptoMarketConfig")}
except Exception as e:
    save("gamma_old_raw", st, b)

for name, url in (
    ("data_v2_root", "https://data-api.polymarket.com/v2/"),
    ("data_v2_trades_market", f"https://data-api.polymarket.com/v2/trades?market={cond}&limit=3"),
    ("data_v2_trades_cond", f"https://data-api.polymarket.com/v2/trades?conditionId={cond}&limit=3"),
    ("docs_data_api_overview", "https://docs.polymarket.com/api-reference/data-api/overview.md"),
    ("docs_llms", "https://docs.polymarket.com/llms.txt"),
):
    st, b = get(url); save(name, st, b, 2500 if name != "docs_llms" else 6000); time.sleep(0.4)

# SQD: ρυθμός blocks και πυκνότητα σήμερα
EX = ["0xE111180000d2663C0091e4f400237545B87B996B", "0xe2222d279d744050d28e00520010520000310F59",
      "0xe2222d002000ba0053cef3375333610f64600036", "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"]
TOPIC_FILLED = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
def sqd(frm, to):
    body = {"type": "evm", "fromBlock": frm, "toBlock": to,
            "fields": {"block": {"number": True, "timestamp": True}, "log": {"address": True, "topics": True, "data": True}},
            "logs": [{"address": EX, "topic0": [TOPIC_FILLED]}]}
    for a in range(5):
        st, b = get("https://portal.sqd.dev/datasets/polygon-mainnet/stream", {"content-type": "application/json"}, json.dumps(body).encode(), 120)
        if st == 200: return st, b
        time.sleep(11)
    return st, b
for name, frm in (("sqd_today_97300000", 97300000), ("sqd_oct_95000000", 95000000), ("sqd_aug_91150000", 91150000)):
    st, b = sqd(frm, frm + 299); info = {"status": st, "bytes": len(b)}
    if st == 200:
        hs = [json.loads(l) for l in b.splitlines() if l.strip()]
        info.update({"blocks": len(hs), "logs": sum(len(h.get("logs", [])) for h in hs), "first_block": hs[0]["header"], "last_block": hs[-1]["header"]})
        if len(hs) > 1:
            dt = hs[-1]["header"]["timestamp"] - hs[0]["header"]["timestamp"]; dn = hs[-1]["header"]["number"] - hs[0]["header"]["number"]
            info["secs_per_block"] = round(dt / dn, 4) if dn else None
        cnt = {}
        for h in hs:
            for l in h.get("logs", []): cnt[l["address"]] = cnt.get(l["address"], 0) + 1
        info["by_address"] = cnt
    OUT["tests"][name] = info; print(name, info.get("status"), flush=True)
os.makedirs("probe", exist_ok=True)
json.dump(OUT, open("probe/result.json", "w"), indent=1, ensure_ascii=False)
print("done")
