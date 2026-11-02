"""
[O8] Opóźnienie pętli zdarzeń: autokorelacja próbek sondy i przedział ufności różnicy median POOL - SYNC
bootstrapem blokowym (ruchome bloki, długość bloku = ceil(n^(1/3)) w każdej próbie, B = 5000, ziarno 12345
z manifestu), w porównaniu z bootstrapem zwykłym z rundy. Wejście: concurrency_lag_samples.parquet (faza measure),
concurrency_h3_tests.json. Wyjście: derived/o8_lag_bootstrap_blokowy.json
"""
import json
import math

import numpy as np
import pandas as pd

from wspolne import RUN, record, write_json

SEED = json.loads((RUN / "manifest.json").read_text(encoding="utf-8"))["seeds"]["bootstrap"]
s = pd.read_parquet(RUN / "concurrency_lag_samples.parquet")
s = s[s.phase == "measure"].sort_values("ts_ns")
h3 = {t["concurrency_level"]: t for t in json.loads((RUN / "concurrency_h3_tests.json").read_text(encoding="utf-8"))}


def series(var, w, m):
    q = s[(s.variant == var) & (s.concurrency_level == m)]
    if var != "SYNC":
        q = q[q.workers == w]
    return q.lag_ms.to_numpy()


def acf1(x):
    return float(np.corrcoef(x[:-1], x[1:])[0, 1]) if len(x) > 2 and np.std(x) > 0 else None


def mbb_median(x, rng, B):
    n = len(x); L = max(1, math.ceil(n ** (1 / 3)))
    starts = np.arange(n - L + 1)
    k = math.ceil(n / L)
    out = np.empty(B)
    for b in range(B):
        idx = np.concatenate([np.arange(st, st + L) for st in rng.choice(starts, k)])[:n]
        out[b] = np.median(x[idx])
    return out, L


rng = np.random.default_rng(SEED)
res = {}
for m in (100, 500):
    a, b = series("POOL", 4, m), series("SYNC", None, m)
    ba, la = mbb_median(a, rng, 5000)
    bb, lb = mbb_median(b, rng, 5000)
    diff = ba - bb
    res[str(m)] = {"n_pool": len(a), "n_sync": len(b), "block_pool": la, "block_sync": lb,
                   "acf1_pool": acf1(a), "acf1_sync": acf1(b),
                   "median_pool_ms": float(np.median(a)), "median_sync_ms": float(np.median(b)),
                   "diff_ci_block_ms": [float(np.percentile(diff, 2.5)), float(np.percentile(diff, 97.5))],
                   "diff_ci_iid_round_ms": [h3[m]["median_diff_ci"]["ci_low"], h3[m]["median_diff_ci"]["ci_high"]],
                   "mann_whitney_p_round": h3[m]["p_value"]}
o = write_json("o8_lag_bootstrap_blokowy.json", {"seed": SEED, "B": 5000, "results": res})
record(__file__, [RUN / "concurrency_lag_samples.parquet", RUN / "concurrency_h3_tests.json", RUN / "manifest.json"], [o])
print(json.dumps(res, indent=1))
