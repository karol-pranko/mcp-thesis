#!/usr/bin/env python3
"""
Analiza statystyczna rundy pomiarowej. Każdy test jest wykonywany na danych z katalogu wyników
tego samego przebiegu; jeśli danych brakuje, test nie jest raportowany (status "not_reported"
z przyczyną). Żadna statystyka nie jest wpisywana na sztywno.

Testy:
  H1  (schema_retrieval.parquet)    Wilcoxon par: Graph@5 vs RRF/BM25 przy porównywalnym budżecie
                                    tabel (equal_budget i k = 10) oraz test konstrukcyjny Graph@5 vs RRF@5,
  H1b (schema_retrieval.parquet)    to samo dla Column-Recall,
  EX  (schema_quality_generation)   dokładny McNemar Graph@5 vs baseline'y, osobno dla profili,
  H2  (oob_h2_tests.json)           Mann-Whitney + bootstrapowy CI różnicy median (z scenariusza B1),
  H3  (concurrency_h3_tests.json)   Mann-Whitney na próbkach lagu + bootstrapowy CI (z scenariusza C1),
  TTFT(ttft_scaling.parquet)        nachylenie TTFT względem liczby tokenów promptu z bootstrapowym CI.
Bootstrap: ziarno seeds.bootstrap z konfiguracji.

Pliki: statistical_hypothesis_tests.csv, statistical_bootstrap_ci.csv, statistical_report.json.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.common.config import add_common_args, load_config  # noqa: E402
from benchmarks.common.stats import bootstrap_mean_ci, mcnemar_exact, wilcoxon_paired  # noqa: E402


def _not_reported(test_id: str, reason: str) -> Dict[str, Any]:
    return {"test_id": test_id, "status": "not_reported", "reason": reason}


def h1_tests(out: Path) -> List[Dict[str, Any]]:
    p = out / "schema_retrieval.parquet"
    if not p.exists():
        return [_not_reported("H1", "brak schema_retrieval.parquet")]
    r = pd.read_parquet(p)
    res: List[Dict[str, Any]] = []

    def series(strategy: str, k: int, variant: str, metric: str) -> pd.Series:
        sub = r[(r.strategy == strategy) & (r.k == k) & (r.budget_variant == variant)]
        return sub.set_index("query_id")[metric].sort_index()

    graph = lambda m: series("Graph", 5, "k_sweep", m)  # noqa: E731
    comparisons = []
    for base in ("RRF", "BM25"):
        comparisons.append((f"{base}@5", base, 5, "k_sweep", "konstrukcyjny" if base == "RRF" else "ten_sam_k"))
        for kb in sorted(r[(r.strategy == base) & (r.budget_variant == "equal_budget")].k.unique()):
            comparisons.append((f"{base}@{kb} (equal_budget)", base, int(kb), "equal_budget", "rowny_budzet"))
        if 10 in set(r[(r.strategy == base) & (r.budget_variant == "k_sweep")].k):
            comparisons.append((f"{base}@10", base, 10, "k_sweep", "budzet_nie_mniejszy"))
    for metric, hid in (("table_recall", "H1"), ("column_recall", "H1b")):
        g = graph(metric)
        if g.empty:
            res.append(_not_reported(hid, "brak Graph@5"))
            continue
        for label, base, k, variant, kind in comparisons:
            b = series(base, k, variant, metric)
            idx = g.index.intersection(b.index)
            if len(idx) < 2:
                res.append(_not_reported(f"{hid}:{label}", "za mało par"))
                continue
            t = wilcoxon_paired(g.loc[idx], b.loc[idx])
            res.append({"test_id": f"{hid}: Graph@5 vs {label}", "hypothesis": hid, "kind": kind, "metric": metric,
                        "mean_a": float(g.loc[idx].mean()), "mean_b": float(b.loc[idx].mean()),
                        "status": "reported" if t.get("reported") else "not_reported", **t})
    return res


def ex_tests(out: Path) -> List[Dict[str, Any]]:
    p = out / "schema_quality_generation.parquet"
    if not p.exists():
        return [_not_reported("EX", "brak schema_quality_generation.parquet")]
    g = pd.read_parquet(p)
    res = []
    for profile, d in g.groupby("profile"):
        ref = d[(d.strategy == "Graph") & (d.budget_variant == "k_sweep")].set_index("query_id")["ex_match"]
        for (s, k, v), sub in d.groupby(["strategy", "k", "budget_variant"]):
            if s == "Graph":
                continue
            b = sub.set_index("query_id")["ex_match"]
            idx = ref.index.intersection(b.index)
            if len(idx) == 0:
                continue
            t = mcnemar_exact(ref.loc[idx], b.loc[idx])
            res.append({"test_id": f"EX[{profile}]: Graph@5 vs {s}@{k} ({v})", "hypothesis": "EX", "profile": profile,
                        "ex_a": float(ref.loc[idx].mean()), "ex_b": float(b.loc[idx].mean()), "status": "reported", **t})
    return res or [_not_reported("EX", "brak par")]


def copied_tests(out: Path, fname: str, hid: str) -> List[Dict[str, Any]]:
    p = out / fname
    if not p.exists():
        return [_not_reported(hid, f"brak {fname}")]
    items = json.loads(p.read_text(encoding="utf-8"))
    res = []
    for it in items:
        ci = it.pop("median_diff_ci", {}) or {}
        res.append({"test_id": f"{hid}: {it.get('comparison')} @ {it.get('payload_size_bytes', it.get('concurrency_level'))}",
                    "status": "reported" if it.get("reported") else "not_reported", "source_file": fname, **it,
                    **{f"median_diff_{k}": v for k, v in ci.items() if k in ("estimate", "ci_low", "ci_high", "n_resamples", "seed")}})
    return res


def ttft_slope(out: Path, seed: int) -> List[Dict[str, Any]]:
    p = out / "ttft_scaling.parquet"
    if not p.exists():
        return [_not_reported("TTFT", "brak ttft_scaling.parquet")]
    d = pd.read_parquet(p)
    d = d[~d.warmup]
    res = []
    rng = np.random.default_rng(seed)
    for profile, sub in d.groupby("profile"):
        x = sub["prompt_eval_count"].astype(float).values / 1000.0
        y = sub["ttft_ms"].astype(float).values
        if len(np.unique(x)) < 2:
            res.append(_not_reported(f"TTFT[{profile}]", "mniej niż 2 długości promptu"))
            continue
        slope = float(np.polyfit(x, y, 1)[0])
        boots = []
        for _ in range(2000):
            i = rng.integers(0, len(x), len(x))
            if len(np.unique(x[i])) > 1:
                boots.append(np.polyfit(x[i], y[i], 1)[0])
        res.append({"test_id": f"TTFT[{profile}]: nachylenie ms / 1000 tokenów promptu", "status": "reported",
                    "slope_ms_per_1k_tokens": slope, "ci_low": float(np.percentile(boots, 2.5)),
                    "ci_high": float(np.percentile(boots, 97.5)), "n": int(len(x)), "seed": seed})
    return res


def bootstrap_cis(out: Path, seed: int) -> pd.DataFrame:
    rows = []
    p = out / "schema_quality_generation.parquet"
    if p.exists():
        g = pd.read_parquet(p)
        for (prof, s, k, v), sub in g.groupby(["profile", "strategy", "k", "budget_variant"]):
            for metric in ("ex_match", "valid_sql"):
                ci = bootstrap_mean_ci(sub[metric].astype(float), seed)
                rows.append({"metric": metric, "profile": prof, "strategy": s, "k": k, "budget_variant": v, **ci})
    p = out / "schema_retrieval.parquet"
    if p.exists():
        r = pd.read_parquet(p)
        for (s, k, v), sub in r.groupby(["strategy", "k", "budget_variant"]):
            ci = bootstrap_mean_ci(sub["table_recall"].astype(float), seed)
            rows.append({"metric": "table_recall", "strategy": s, "k": k, "budget_variant": v, **ci})
    return pd.DataFrame(rows)


def main() -> None:
    args = add_common_args(argparse.ArgumentParser(description=__doc__)).parse_args()
    cfg = load_config(args.config, args.mode)
    out = Path(args.output_dir)
    seed = cfg["seeds"]["bootstrap"]
    tests = h1_tests(out) + ex_tests(out) + copied_tests(out, "oob_h2_tests.json", "H2") \
        + copied_tests(out, "concurrency_h3_tests.json", "H3") + ttft_slope(out, seed)
    pd.DataFrame(tests).to_csv(out / "statistical_hypothesis_tests.csv", index=False)
    ci = bootstrap_cis(out, seed)
    ci.to_csv(out / "statistical_bootstrap_ci.csv", index=False)
    (out / "statistical_report.json").write_text(json.dumps({"tests": tests, "bootstrap_seed": seed}, ensure_ascii=False,
                                                            indent=2, default=float), encoding="utf-8")
    n_rep = sum(1 for t in tests if t.get("status") == "reported")
    print(f"Testy raportowane: {n_rep}; nieraportowane: {len(tests) - n_rep}")


if __name__ == "__main__":
    main()
