"""
[C1/O8] Przyczyna ogona p99 opóźnienia pętli zdarzeń w wariancie POOL.

Próbnik pamięci aparatury (bench_concurrency.tree_memory, co 0,5 s) wywołuje synchronicznie w pętli zdarzeń
psutil.memory_full_info() (odczyt PSS z /proc) dla procesu głównego i wszystkich potomków. Skrypt sprawdza, czy
próbki sondy z opóźnieniem > 50 ms pokrywają się w czasie z próbkami pamięci, oraz liczy percentyle opóźnienia
po wyłączeniu próbek sondy zarejestrowanych w ciągu 20 ms po próbce pamięci (analiza wtórna; nie zastępuje
wyników rundy). Wejścia: concurrency_lag_samples.parquet, concurrency_memory_samples.csv.
Wyjście: derived/o8b_ogon_lag.json
"""
import numpy as np
import pandas as pd

from wspolne import RUN, record, write_json

s = pd.read_parquet(RUN / "concurrency_lag_samples.parquet")
s = s[s.phase == "measure"]
m = pd.read_csv(RUN / "concurrency_memory_samples.csv")
m = m[m.phase == "measure"]
res = {}
for (var, w, M), q in s.groupby(["variant", "workers", "concurrency_level"], dropna=False):
    q = q.sort_values("ts_ns")
    mm = m[(m.variant == var) & (m.concurrency_level == M) & ((m.workers == w) if pd.notna(w) else m.workers.isna())]
    if mm.empty:
        continue
    mts = np.sort(mm.ts_ns.values)
    # odległość od najbliższej wcześniejszej próbki pamięci (próbka sondy jest zapisywana po wybudzeniu)
    idx = np.searchsorted(mts, q.ts_ns.values) - 1
    after = np.where(idx >= 0, (q.ts_ns.values - mts[np.clip(idx, 0, None)]) / 1e6, np.inf)
    near = after <= 20
    big = q.lag_ms.values > 50
    key = f"{var}|{'' if pd.isna(w) else int(w)}|{M}"
    clean = q.lag_ms.values[~near]
    res[key] = {"n": len(q), "mem_samples": len(mm), "children_max": int(mm.n_children.max()),
                "spikes_gt50": int(big.sum()), "spikes_gt50_near_mem": int((big & near).sum()),
                "p99_all_ms": float(np.percentile(q.lag_ms, 99)), "median_all_ms": float(np.median(q.lag_ms)),
                "n_excluded": int(near.sum()),
                "p99_excl_ms": float(np.percentile(clean, 99)) if len(clean) else None,
                "max_excl_ms": float(clean.max()) if len(clean) else None}
o = write_json("o8b_ogon_lag.json", res)
record(__file__, [RUN / "concurrency_lag_samples.parquet", RUN / "concurrency_memory_samples.csv"], [o])
for k, v in res.items():
    if k.startswith(("POOL", "THREAD|4|100", "SYNC||100")):
        print(k, {a: (round(b, 2) if isinstance(b, float) else b) for a, b in v.items()})
