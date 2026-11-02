#!/usr/bin/env python3
"""
Zestawienie kluczowych liczb rundy w <RESULTS_DIR>/summary.json. Każda wartość ma pole `source`
(plik w katalogu wyników i sposób wyliczenia). Brak pliku źródłowego = brak wpisu (nie wartość domyślna).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.common.config import add_common_args  # noqa: E402


def item(value: Any, source: str) -> Dict[str, Any]:
    if isinstance(value, (np.floating, np.integer)):
        value = value.item()
    return {"value": value, "source": source}


def main() -> None:
    args = add_common_args(argparse.ArgumentParser(description=__doc__)).parse_args()
    out = Path(args.output_dir)
    s: Dict[str, Any] = {}
    m = json.loads((out / "manifest.json").read_text()) if (out / "manifest.json").exists() else {}
    s["run"] = {"run_id": m.get("run_id"), "git_commit": m.get("git", {}).get("commit"), "git_dirty": m.get("git", {}).get("dirty"),
                "mode": m.get("mode"), "model_digests": m.get("model_digests")}

    if (out / "schema_retrieval.parquet").exists():
        r = pd.read_parquet(out / "schema_retrieval.parquet")
        g = r.groupby(["strategy", "k", "budget_variant"]).agg(recall=("table_recall", "mean"), precision=("table_precision", "mean"),
                                                               n_tables=("n_tables", "mean"), prompt_tokens=("prompt_tokens", "mean"),
                                                               ddl_tokens=("ddl_tokens", "mean"))
        s["schema_retrieval"] = {f"{a}@{k} ({v})": {c: item(row[c], "schema_retrieval.parquet: średnia po zapytaniach") for c in g.columns}
                                 for (a, k, v), row in g.iterrows()}
    if (out / "schema_full_dump_tokens.json").exists():
        s["full_schema_dump"] = item(json.loads((out / "schema_full_dump_tokens.json").read_text()), "schema_full_dump_tokens.json")
    if (out / "schema_quality_summary.csv").exists():
        q = pd.read_csv(out / "schema_quality_summary.csv")
        s["text2sql"] = {f"{r.profile}/{r.strategy}@{r.k} ({r.budget_variant})": {
            "valid_sql_pct": item(r.valid_sql_pct, "schema_quality_summary.csv"),
            "execution_accuracy_pct": item(r.execution_accuracy_pct, "schema_quality_summary.csv"),
            "n": item(int(r.n), "schema_quality_summary.csv"), "model": r.model} for r in q.itertuples()}
    if (out / "statistical_hypothesis_tests.csv").exists():
        t = pd.read_csv(out / "statistical_hypothesis_tests.csv")
        s["tests"] = {row.test_id: {k: item(v, "statistical_hypothesis_tests.csv") for k, v in row._asdict().items()
                                    if k in ("p_value", "W", "U", "n_eff", "N", "rank_biserial_r", "only_a", "only_b", "mean_a", "mean_b",
                                             "median_diff_estimate", "median_diff_ci_low", "median_diff_ci_high", "slope_ms_per_1k_tokens")
                                    and pd.notna(v)} for row in t[t.status == "reported"].itertuples()}
    if (out / "ablation_m_exp_results.csv").exists():
        a = pd.read_csv(out / "ablation_m_exp_results.csv")
        s["ablation"] = {f"m_exp={int(r.m_exp)}": {c: item(getattr(r, c), "ablation_m_exp_results.csv") for c in
                         ("table_recall_at_5", "avg_ddl_tokens_uniform_format", "median_total_ms", "median_bridge_search_ms", "p95_bridge_search_ms")}
                         for r in a.itertuples()}
        if (out / "ablation_m_exp_sweet_spot.json").exists():
            s["ablation"]["sweet_spot"] = item(json.loads((out / "ablation_m_exp_sweet_spot.json").read_text()), "ablation_m_exp_sweet_spot.json")
    if (out / "ttft_scaling_summary.csv").exists():
        tt = pd.read_csv(out / "ttft_scaling_summary.csv")
        s["ttft_scaling"] = {f"{r.profile}/{int(r.target_ddl_tokens)}": {"ttft_median_ms": item(r.ttft_median_ms, "ttft_scaling_summary.csv"),
                             "prompt_eval_count": item(r.prompt_eval_count, "ttft_scaling_summary.csv")} for r in tt.itertuples()}
    if (out / "oob_crossover_metrics.json").exists():
        s["oob"] = item(json.loads((out / "oob_crossover_metrics.json").read_text()), "oob_crossover_metrics.json (S*, CI, tryb pomiaru)")
    if (out / "pagination_advanced_1m.csv").exists():
        p = pd.read_csv(out / "pagination_advanced_1m.csv").groupby(["variant", "offset_rows"]).latency_ms.median()
        s["pagination_median_ms"] = {f"{v}@{o}": item(x, "pagination_advanced_1m.csv: mediana") for (v, o), x in p.items()}
    if (out / "concurrency_summary.csv").exists():
        c = pd.read_csv(out / "concurrency_summary.csv")
        s["concurrency"] = {f"{r.variant}/w={r.workers}/M={r.concurrency_level}": {
            "rps": item(r.requests_per_second, "concurrency_summary.csv"), "lag_median_ms": item(r.lag_median_ms, "concurrency_summary.csv"),
            "lag_p99_ms": item(r.lag_p99_ms, "concurrency_summary.csv"), "tree_mem_peak_mb": item(r.tree_mem_peak_mb, "concurrency_summary.csv")}
            for r in c.itertuples()}
    if (out / "streaming_summary.csv").exists():
        st = pd.read_csv(out / "streaming_summary.csv")
        s["streaming"] = {r.variant: {"latency_median_ms": item(r.latency_median_ms, "streaming_summary.csv"),
                                      "latency_p95_ms": item(r.latency_p95_ms, "streaming_summary.csv")} for r in st.itertuples()}
        idle = pd.read_csv(out / "streaming_idle_bandwidth.csv")
        s["streaming_idle"] = {r.variant: item(r.bytes_per_s, "streaming_idle_bandwidth.csv") for r in idle.itertuples()}
    if (out / "security_suite_metrics.json").exists():
        s["security"] = item(json.loads((out / "security_suite_metrics.json").read_text()), "security_suite_metrics.json")
    if (out / "security_xml_token_overhead.csv").exists():
        x = pd.read_csv(out / "security_xml_token_overhead.csv")
        s["xml_token_overhead"] = item(x.groupby("record_kind").added_tokens.describe().to_dict(orient="index"), "security_xml_token_overhead.csv")
    if (out / "schema_drift_summary.json").exists():
        s["drift"] = item(json.loads((out / "schema_drift_summary.json").read_text()), "schema_drift_summary.json")
    (out / "summary.json").write_text(json.dumps(s, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"summary.json: {len(s)} sekcji")


if __name__ == "__main__":
    main()
