#!/usr/bin/env python3
"""
Walidacja kompletności rundy: oczekiwane pliki, liczby rekordów zgodne z konfiguracją, brak NaN
w kluczowych kolumnach, spójność taksonomii błędów, udział rekordów possible_truncation (< 1%,
inaczej ostrzeżenie) oraz obecność wykresów. Zapisuje <RESULTS_DIR>/validation_report.md;
kod wyjścia != 0 przy jakiejkolwiek niezgodności (ostrzeżenia nie zmieniają kodu wyjścia).
Sprawdzane są tylko scenariusze oznaczone w manifeście jako ukończone (oraz wymagane przez --require).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.common.config import add_common_args, load_config  # noqa: E402
from benchmarks.common.metrics_text2sql import ERROR_CATEGORIES  # noqa: E402
from benchmarks.plot_generators.generate_thesis_plots import plots_for_completed  # noqa: E402


class Report:
    def __init__(self) -> None:
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.ok: List[str] = []

    def check(self, cond: bool, msg: str) -> None:
        (self.ok if cond else self.errors).append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)


def _csv(out: Path, name: str, rep: Report) -> pd.DataFrame:
    p = out / name
    rep.check(p.exists(), f"plik {name} istnieje")
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_csv(p)


def _nan(df: pd.DataFrame, cols: List[str], name: str, rep: Report) -> None:
    for c in cols:
        if c in df:
            rep.check(not df[c].isna().any(), f"{name}: brak NaN w kolumnie {c}")
        else:
            rep.check(False, f"{name}: brak kolumny {c}")


def v_pagination(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    sc = cfg["scenarios"]["pagination"]
    d = _csv(out, "pagination_advanced_1m.csv", rep)
    n_un = sum(1 for o in sc["offsets"] if o <= sc["unindexed_max_offset"])
    exp = sc["repetitions"] * (2 * len(sc["offsets"]) + n_un)
    rep.check(len(d) == exp, f"pagination_advanced_1m: {len(d)} rekordów (oczekiwano {exp})")
    _nan(d, ["latency_ms"], "pagination_advanced_1m", rep)
    a = _csv(out, "pagination_predicate_ablation.csv", rep)
    rep.check(len(a) == 2 * len(sc["offsets"]) * sc["repetitions"], f"pagination_predicate_ablation: {len(a)} rekordów")
    p = out / "pagination_explain_plans.json"
    rep.check(p.exists() and len(json.loads(p.read_text())) == 2 * len(sc["explain_offsets"]), "pagination_explain_plans.json kompletny")
    rep.check((out / "pagination_consistency.json").exists(), "pagination_consistency.json istnieje")


def v_oob(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    sc = cfg["scenarios"]["oob"]
    d = _csv(out, "oob_crossover_results.csv", rep)
    loop = sum(3 * max(sc["repetitions"], sc["h2_min_repetitions"] if s in sc["h2_sizes_bytes"] else 0) for s in sc["payload_sizes_bytes"])
    exp = loop + len(sc["profiles"]) * len(sc["payload_sizes_bytes"]) * 2 * sc["repetitions"]
    rep.check(len(d) == exp, f"oob_crossover_results: {len(d)} rekordów (oczekiwano {exp})")
    _nan(d, ["e2e_ns", "measurement_mode"], "oob_crossover_results", rep)
    if len(d):
        rep.check(bool(d["sha256_verified"].all()), "OOB: wszystkie transfery zweryfikowane SHA-256")
        if (d.measurement_mode == "emulated").any():
            rep.warn("OOB: profile LAN/WAN zmierzone w trybie EMULATED (opóźnienie sieci ze wzoru)")
    for f in ("oob_crossover_metrics.json", "oob_model_fit.json", "oob_h2_tests.json"):
        rep.check((out / f).exists(), f"{f} istnieje")


def v_concurrency(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    sc = cfg["scenarios"]["concurrency"]
    d = _csv(out, "concurrency_summary.csv", rep)
    n_grid = sum(1 for w in sc["workers_grid"] if w != sc["default_workers"]) * sum(1 for v in sc["variants"] if v != "SYNC")
    exp = len(sc["levels"]) * len(sc["variants"]) + n_grid
    rep.check(len(d) == exp, f"concurrency_summary: {len(d)} poziomów (oczekiwano {exp})")
    _nan(d, ["requests_per_second", "lag_median_ms", "tree_mem_peak_mb"], "concurrency_summary", rep)
    if len(d):
        rep.check(bool((d.lag_samples_n > 0).all()), "C1: każdy poziom ma próbki lagu")
    rep.check((out / "concurrency_lag_samples.parquet").exists(), "concurrency_lag_samples.parquet istnieje")
    rep.check((out / "concurrency_h3_tests.json").exists(), "concurrency_h3_tests.json istnieje")


def v_streaming(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    sc = cfg["scenarios"]["streaming"]
    d = _csv(out, "streaming_events.csv", rep)
    exp = sc["n_events"] * (1 + len(sc["poll_intervals_s"]))
    rep.check(len(d) == exp, f"streaming_events: {len(d)} zdarzeń (oczekiwano {exp})")
    if len(d):
        rep.check(bool(d.detected.all()), "C2: wszystkie zdarzenia wykryte")
    i = _csv(out, "streaming_idle_bandwidth.csv", rep)
    _nan(i, ["bytes_per_s"], "streaming_idle_bandwidth", rep)


def v_security(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    d = _csv(out, "security_suite_results.csv", rep)
    u = _csv(out, "security_vectors_unique.csv", rep)
    rep.check(len(d) == len(u) and len(d) > 0, f"security: {len(d)} wyników dla {len(u)} unikalnych wektorów")
    rep.check((out / "security_xml_token_overhead.csv").exists(), "security_xml_token_overhead.csv istnieje")
    if cfg["scenarios"]["security"].get("llm_injection"):
        rep.check((out / "security_llm_injection.jsonl").exists(), "security_llm_injection.jsonl istnieje")


def v_drift(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    rep.check((out / "schema_drift_summary.json").exists(), "schema_drift_summary.json istnieje")


def v_ablation(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    sc = cfg["scenarios"]["ablation"]
    d = _csv(out, "ablation_m_exp_calls.csv", rep)
    rep.check(len(d) == len(sc["m_exp_values"]) * sc["n_queries"], f"ablation_m_exp_calls: {len(d)} wywołań")
    _nan(d, ["t_total_ns", "t_bridge_search_ns", "table_recall"], "ablation_m_exp_calls", rep)
    rep.check((out / "ablation_m_exp_sweet_spot.json").exists(), "ablation_m_exp_sweet_spot.json istnieje")


def v_ttft(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    sc = cfg["scenarios"]["ttft_scaling"]
    if not sc.get("enabled", True):
        return
    d = _csv(out, "ttft_scaling.parquet", rep)
    exp = len(sc["profiles"]) * len(sc["ddl_token_targets"]) * (sc["repetitions"] + 1)
    rep.check(len(d) == exp, f"ttft_scaling: {len(d)} wywołań (oczekiwano {exp})")
    _nan(d, ["ttft_ms", "prompt_eval_count"], "ttft_scaling", rep)
    if len(d) and d.possible_truncation.any():
        rep.warn(f"ttft_scaling: {int(d.possible_truncation.sum())} wywołań z possible_truncation")


def v_schema_quality(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    sc = cfg["scenarios"]["schema_quality"]
    g = _csv(out, "schema_quality_generation.parquet", rep)
    per_query = len(sc.get("generation_strategies", [])) + len(sc["equal_budget"]["strategies"])
    exp = len(sc["profiles"]) * sc["n_queries"] * per_query
    rep.check(len(g) == exp, f"schema_quality_generation: {len(g)} rekordów (oczekiwano {exp})")
    _nan(g, ["valid_sql", "ex_match", "error_category", "prompt_eval_count", "eval_count"], "schema_quality_generation", rep)
    if len(g):
        f = g[~g.ex_match]
        rep.check(bool(f.error_category.isin(ERROR_CATEGORIES).all()), "taksonomia: każdy rekord EX=False ma kategorię błędu")
        rep.check(bool((g[g.ex_match].error_category == "SUCCESS").all()), "taksonomia: rekordy EX=True mają kategorię SUCCESS")
        dec = _csv(out, "schema_error_decomposition.csv", rep)
        rep.check(int(dec["count"].sum()) == len(f) if len(dec) else len(f) == 0, "taksonomia: suma kategorii = liczba EX=False")
        share = float(g.possible_truncation.mean())
        if share >= 0.01:
            rep.warn(f"possible_truncation: {share:.2%} rekordów (>= 1%) - rozważ zwiększenie num_ctx")
        rep.check(not g.gt_empty.any(), "brak zapytań GT z pustym wynikiem")
    r = _csv(out, "schema_retrieval.parquet", rep)
    _nan(r, ["table_recall", "table_precision", "ddl_tokens", "prompt_tokens"], "schema_retrieval", rep)
    rep.check((out / "schema_quality_dataset.jsonl").exists(), "schema_quality_dataset.jsonl istnieje")


def v_statistics(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    for f in ("statistical_hypothesis_tests.csv", "statistical_bootstrap_ci.csv", "statistical_report.json"):
        rep.check((out / f).exists(), f"{f} istnieje")


def v_plots(out: Path, cfg: Dict[str, Any], rep: Report) -> None:
    manifest = json.loads((out / "manifest.json").read_text())
    for p in plots_for_completed(manifest):
        rep.check((out / "plots" / f"{p}.pdf").exists(), f"wykres {p}.pdf istnieje")


VALIDATORS: Dict[str, Callable[[Path, Dict[str, Any], Report], None]] = {
    "pagination": v_pagination, "oob": v_oob, "concurrency": v_concurrency, "streaming": v_streaming,
    "security": v_security, "drift": v_drift, "ablation": v_ablation, "ttft_scaling": v_ttft,
    "schema_quality": v_schema_quality, "statistics": v_statistics, "plots": v_plots,
}


def main() -> int:
    ap = add_common_args(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--require", nargs="*", default=None, help="Scenariusze wymagane niezależnie od manifestu.")
    args = ap.parse_args()
    cfg = load_config(args.config, args.mode)
    out = Path(args.output_dir)
    rep = Report()
    for f in ("manifest.json", "environment.json"):
        rep.check((out / f).exists(), f"{f} istnieje")
    manifest = json.loads((out / "manifest.json").read_text()) if (out / "manifest.json").exists() else {}
    sc_status = manifest.get("scenarios", {})
    selected = set(args.require or []) | {k for k, v in sc_status.items() if v.get("status") == "completed"}
    for name, st in sc_status.items():
        if st.get("status") == "failed":
            rep.check(False, f"scenariusz {name} zakończony błędem (kod {st.get('exit_code')})")
        if name in VALIDATORS:
            rep.check((out / "logs" / f"{name}.log").exists(), f"log scenariusza {name} istnieje")
    for name in VALIDATORS:
        if name in selected:
            try:
                VALIDATORS[name](out, cfg, rep)
            except Exception as exc:
                rep.check(False, f"walidacja {name}: wyjątek {exc}")
    lines = [f"# Raport walidacji rundy {manifest.get('run_id', '?')}", "", f"Tryb: {args.mode}", "",
             f"Błędy: {len(rep.errors)}, ostrzeżenia: {len(rep.warnings)}, kontrole zaliczone: {len(rep.ok)}", ""]
    lines += ["## Błędy", ""] + [f"- {e}" for e in rep.errors] + [""]
    lines += ["## Ostrzeżenia", ""] + [f"- {w}" for w in rep.warnings] + [""]
    lines += ["## Kontrole zaliczone", ""] + [f"- {o}" for o in rep.ok]
    (out / "validation_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines[:6]))
    for e in rep.errors:
        print("BŁĄD:", e)
    for w in rep.warnings:
        print("OSTRZEŻENIE:", w)
    return 1 if rep.errors else 0


if __name__ == "__main__":
    sys.exit(main())
