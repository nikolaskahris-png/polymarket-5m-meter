"""Συνθετικά μηνύματα με γνωστή απάντηση για phase3/recorder.py (Rec, Writer). python phase3/test_recorder.py -> all checks passed"""
import gzip, os, sys, tempfile, time
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import recorder as RC
F = []
def chk(n, got, exp):
    ok = got == exp; print(("ok  " if ok else "FAIL"), n, got, exp)
    if not ok: F.append(n)
R = RC.Rec(levels=2); R.add_market(1000, "UP", "DN")
rows = R.handle({"event_type": "book", "asset_id": "UP", "timestamp": "5", "bids": [{"price": "0.50", "size": "10"}, {"price": "0.49", "size": "5"}, {"price": "0.48", "size": "7"}], "asks": [{"price": "0.52", "size": "3"}]}, 1)
chk("book rows", [r[0] for r in rows], ["B", "B"])
chk("book top2 bids", rows[0][5], "0.5@10|0.49@5")
chk("down book ignored", R.handle({"event_type": "book", "asset_id": "DN", "bids": [], "asks": []}, 2), [])
# price_change στο επίπεδο 0.48 (3ο): εκτός top-2 πριν και μετά -> αγνοείται
r = R.handle({"event_type": "price_change", "timestamp": "6", "price_changes": [{"asset_id": "UP", "price": "0.48", "size": "9", "side": "BUY"}]}, 3)
chk("deep change dropped", r, [])
# νέο επίπεδο 0.51 μπαίνει στο top-2 -> καταγράφεται, και 0.49 βγαίνει
r = R.handle({"event_type": "price_change", "timestamp": "7", "price_changes": [{"asset_id": "UP", "price": "0.51", "size": "4", "side": "BUY", "best_bid": "0.51", "best_ask": "0.52"}]}, 4)
chk("new top level kept", [x[0] for x in r], ["P"]); chk("size semantics A", R.books["UP"].bids[0.51], 4.0)
# αφαίρεση 0.50 (size 0) -> ήταν top -> καταγράφεται και φεύγει
r = R.handle({"event_type": "price_change", "timestamp": "8", "price_changes": [{"asset_id": "UP", "price": "0.50", "size": "0", "side": "BUY"}]}, 5)
chk("removal kept", len(r), 1); chk("removed", 0.5 in R.books["UP"].bids, False)
chk("top after", R.books["UP"].top("BUY", 2), [0.51, 0.49])
# Down price_change αγνοείται
chk("down pc ignored", R.handle({"event_type": "price_change", "price_changes": [{"asset_id": "DN", "price": "0.4", "size": "1", "side": "BUY"}]}, 6), [])
# trades και στα δύο
x = R.handle({"event_type": "last_trade_price", "asset_id": "DN", "timestamp": "9", "side": "BUY", "price": "0.47", "size": "12", "fee_rate_bps": "0", "transaction_hash": "0xab"}, 7)
chk("trade down", x[0][:6], ["X", 7, "9", 1000, "D", "BUY"])
chk("unknown asset trade", R.handle({"event_type": "last_trade_price", "asset_id": "ZZ"}, 8), [])
chk("resolved", R.handle({"event_type": "market_resolved", "assets_ids": ["UP", "DN"], "winning_outcome": "Up", "timestamp": "10"}, 9), [["R", 9, "10", 1000, "Up"]])
R.drop_market("UP", "DN"); chk("dropped", ("UP" in R.assets, "UP" in R.books), (False, False))
with tempfile.TemporaryDirectory() as td:
    W = RC.Writer(td); t = 1794900000.0
    W.write([["X", 1, 2, 3]], t); W.write([["X", 4, 5, 6]], t + 3600); W.flush(); W.f.close()
    files = sorted(os.path.relpath(os.path.join(d, f), td) for d, _, fs in os.walk(td) for f in fs)
    chk("hourly files", len(files), 2)
    chk("content", gzip.open(os.path.join(td, files[0]), "rt").read(), "X,1,2,3\n")
print("all checks passed" if not F else f"FAILED {F}"); sys.exit(1 if F else 0)
