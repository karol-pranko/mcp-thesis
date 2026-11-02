"""
[O1] Scenariusz A: pełna czułość, EX warunkowe przy pełnej czułości, podział na liczbę tabel ścieżki.

Wejścia: schema_retrieval.parquet (czułość per zapytanie, wszystkie strategie i k),
         schema_quality_generation.parquet (EX per zapytanie, k=5 i warianty równego budżetu, oba profile).
Pełna czułość: Table-Recall = 1 (wszystkie tabele GT w wycinku). EX warunkowe: odsetek EX=True wśród zapytań
z pełną czułością (95% CI Cloppera-Pearsona). Wyjście: derived/o1_czulosc_a_ex.json
"""
import json

import numpy as np
import pandas as pd
from scipy.stats import beta

from wspolne import RUN, record, write_json


def cp(k, n):
    lo = beta.ppf(0.025, k, n - k + 1) if k > 0 else 0.0
    hi = beta.ppf(0.975, k + 1, n - k) if k < n else 1.0
    return [100 * lo, 100 * hi]


r = pd.read_parquet(RUN / "schema_retrieval.parquet")
g = pd.read_parquet(RUN / "schema_quality_generation.parquet")
r["full"] = r.table_recall >= 1 - 1e-12
ds = {json.loads(l)["query_id"]: len(json.loads(l)["ground_truth_tables"]) for l in (RUN / "schema_quality_dataset.jsonl").open(encoding="utf-8")}
r["n_gt"] = r.query_id.map(ds)
g["n_gt"] = g.query_id.map(ds)

full = {}
for (s, k, b), d in r.groupby(["strategy", "k", "budget_variant"]):
    full[f"{s}@{k}"] = {"n": len(d), "full_recall_n": int(d.full.sum()), "full_recall_pct": 100 * d.full.mean(),
                        "mean_recall_pct": 100 * d.table_recall.mean(), "mean_n_tables": d.n_tables.mean(),
                        "mean_ddl_tokens": d.ddl_tokens.mean()}
key = r[["query_id", "strategy", "k", "full", "table_recall"]]
gg = g.merge(key, on=["query_id", "strategy", "k"], how="left", validate="many_to_one")
cond = {}
for (p, s, k), d in gg.groupby(["profile", "strategy", "k"]):
    f = d[d.full]
    nf = d[~d.full]
    cond[f"{p}|{s}@{k}"] = {
        "n": len(d), "ex_pct": 100 * d.ex_match.mean(),
        "full_n": len(f), "ex_given_full_k": int(f.ex_match.sum()),
        "ex_given_full_pct": 100 * f.ex_match.mean() if len(f) else None, "ex_given_full_ci": cp(int(f.ex_match.sum()), len(f)) if len(f) else None,
        "not_full_n": len(nf), "ex_given_not_full_k": int(nf.ex_match.sum()),
        "ex_given_not_full_pct": 100 * nf.ex_match.mean() if len(nf) else None,
        "error_categories_given_full": f[~f.ex_match].error_category.value_counts().to_dict(),
    }
by_len = {}
for (s, k, n_gt), d in r[r.k.isin([5, 8, 15])].groupby(["strategy", "k", "n_gt"]):
    by_len.setdefault(f"{s}@{k}", {})[int(n_gt)] = {"n": len(d), "recall_pct": 100 * d.table_recall.mean(), "full_pct": 100 * d.full.mean()}
for (p, s, k, n_gt), d in gg.groupby(["profile", "strategy", "k", "n_gt"]):
    by_len[f"{s}@{k}"][int(n_gt)][f"ex_{p}_pct"] = 100 * d.ex_match.mean()
    by_len[f"{s}@{k}"][int(n_gt)][f"ex_{p}_k"] = int(d.ex_match.sum())
out = write_json("o1_czulosc_a_ex.json", {"full_recall": full, "conditional_ex": cond, "by_gt_tables": by_len})
record(__file__, [RUN / "schema_retrieval.parquet", RUN / "schema_quality_generation.parquet", RUN / "schema_quality_dataset.jsonl"], [out])
for kk in ["Graph@5", "RRF@5", "BM25@5", "Dense@5", "RRF@8", "BM25@8", "DomainScoped@15"]:
    print(kk, {x: round(y, 1) if isinstance(y, float) else y for x, y in full[kk].items()})
for kk, v in cond.items():
    print(kk, v["full_n"], v["ex_given_full_k"], None if v["ex_given_full_pct"] is None else round(v["ex_given_full_pct"], 1),
          [round(x, 1) for x in v["ex_given_full_ci"]] if v["ex_given_full_ci"] else None, round(v["ex_given_not_full_pct"], 1), v["error_categories_given_full"])
for kk in ["Graph@5", "BM25@8", "RRF@8", "BM25@5"]:
    print(kk, {n: {a: round(b, 1) if isinstance(b, float) else b for a, b in v.items()} for n, v in by_len[kk].items()})
