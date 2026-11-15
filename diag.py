import json, os, time, urllib.request
COND = "0x01c278996b25d8dda2693dd71ee479bfb440f74a0766b7a12a4f7d5bb88f74c5"
PREFIX = ["0x2ce048f949", "0x0d1c5c168f", "0xaca9e81aba", "0x7dce028003", "0x90594c60a0", "0xfba02f12af"]
WALLET = ["0xb322462a", "0x3387ac61"]
EXCH = {a.lower() for a in ["0xE111180000d2663C0091e4f400237545B87B996B", "0xe2222d279d744050d28e00520010520000310F59", "0xe2222d002000ba0053cef3375333610f64600036", "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"]}
RPCS = ["https://polygon-bor-rpc.publicnode.com", "https://polygon-rpc.com", "https://1rpc.io/matic"]

def get(url, data=None, tries=4):
    for a in range(tries):
        try:
            req = urllib.request.Request(url, data=data, headers={"User-Agent": "meter-diag", "content-type": "application/json"})
            return json.loads(urllib.request.urlopen(req, timeout=60).read())
        except Exception as e:
            err = repr(e)[:150]; time.sleep(2 + a * 2)
    return {"error": err}

rows, cur = [], None
for _ in range(40):
    j = get("https://data-api.polymarket.com/v2/trades?conditionId=" + COND + "&limit=500" + ("&cursor=" + cur if cur else ""))
    rows += j.get("data") or []
    pg = j.get("pagination") or {}; cur = pg.get("next_cursor")
    if not cur or not pg.get("has_more"): break
out = {"rows": len(rows), "row_keys": sorted(rows[0].keys()) if rows else None, "row0": rows[0] if rows else None}
hits = [r for r in rows if any(r["transaction_hash"].startswith(p) for p in PREFIX)]
whits = [r for r in rows if any(r["proxy_wallet"].lower().startswith(p) for p in WALLET)]
out["wallet_rows"] = {w: [[r["transaction_hash"], r["side"], r["size"], r["price"]] for r in rows if r["proxy_wallet"].lower().startswith(w)][:8] for w in WALLET}
out["wallet_row_counts"] = {w: sum(1 for r in rows if r["proxy_wallet"].lower().startswith(w)) for w in WALLET}
out["full_wallets"] = {w: sorted({r["proxy_wallet"] for r in rows if r["proxy_wallet"].lower().startswith(w)}) for w in WALLET}
def receipt(tx):
    for rpc in RPCS:
        j = get(rpc, json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionReceipt", "params": [tx]}).encode(), tries=2)
        if j.get("result"): return j["result"], rpc
    return None, None
out["tx"] = []
for r in hits[:6]:
    rc, rpc = receipt(r["transaction_hash"])
    d = {"tx": r["transaction_hash"], "data_api": {k: r[k] for k in r if k not in ("transaction_hash",)}, "rpc": rpc}
    if rc:
        d["block"] = int(rc["blockNumber"], 16); d["to"] = rc["to"]; d["n_logs"] = len(rc["logs"])
        d["logs"] = [[l["address"], l["topics"][0][:12] if l["topics"] else None, len(l["topics"]), len(l["data"]), l["address"].lower() in EXCH] for l in rc["logs"]]
    out["tx"].append(d)
os.makedirs("diag", exist_ok=True)
json.dump(out, open("diag/result.json", "w"), indent=1)
print(json.dumps(out)[:4000])
