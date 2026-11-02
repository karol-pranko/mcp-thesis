#!/usr/bin/env python3
"""
Analiza końcowej rundy pomiarowej (results-remote-new-sunday). Jedyne źródło liczb do pracy.

Każda wartość jest zapisywana w out/wyniki.json jako {"value", "src", "how"}. Źródłem są wyłącznie
pliki katalogu wyników tej rundy. Dodatkowo powstają tabele pomocnicze (out/*.csv) i raport
pomocniczy out/analiza.md wklejany do ANALIZA_WYNIKOW.md.

Uruchomienie: .venv/bin/python benchmarks/analysis_final/analiza.py [--results results-remote-new-sunday]
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd
from scipy import stats as sps

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "out"
V: Dict[str, Dict[str, Any]] = {}


def put(key: str, value: Any, src: str, how: str) -> None:
    if isinstance(value, (np.floating, np.integer, np.bool_)):
        value = value.item()
    V[key] = {"value": value, "src": src, "how": how}


def cp_interval(k: int, n: int, level: float = 0.95):
    a = 1 - level
    lo = 0.0 if k == 0 else sps.beta.ppf(a / 2, k, n - k + 1)
    hi = 1.0 if k == n else sps.beta.ppf(1 - a / 2, k + 1, n - k)
    return float(lo), float(hi)


def boot_median_ci(x, seed, n=5000):
    x = np.asarray(x, float)
    rng = np.random.default_rng(seed)
    meds = np.median(x[rng.integers(0, len(x), (n, len(x)))], axis=1)
    return float(np.percentile(meds, 2.5)), float(np.percentile(meds, 97.5))


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    h.update(p.read_bytes())
    return h.hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=str(ROOT / "results-remote-new-sunday"))
    R = Path(ap.parse_args().results)
    OUT.mkdir(parents=True, exist_ok=True)
    m = json.loads((R / "manifest.json").read_text())
    env = json.loads((R / "environment.json").read_text())
    seed = m["seeds"]["bootstrap"]
    cfg = m["config"]

    # ------------------------------------------------------------------ przebieg
    put("run.id", m["run_id"], "manifest.json", "run_id")
    put("run.mode", m["mode"], "manifest.json", "mode")
    put("run.commit", m["git"]["commit"][:7], "manifest.json", "git.commit")
    put("run.dirty", m["git"]["dirty"], "manifest.json", "git.dirty")
    put("run.subst", m["smoke_test_model_substitution"], "manifest.json", "smoke_test_model_substitution")
    hashes = m.get("files_sha256", {})
    ok = sum(1 for f, h in hashes.items() if (R / f).exists() and sha256(R / f) == h)
    put("run.sha_ok", ok, "manifest.json files_sha256", "liczba plików zgodnych z sumą SHA-256")
    put("run.sha_total", len(hashes), "manifest.json files_sha256", "liczba plików w manifeście")
    put("run.sha_mismatch", [f for f, h in hashes.items() if (R / f).exists() and sha256(R / f) != h], "manifest.json", "pliki niezgodne (zmienione po zapisie sumy)")
    st = {k: v["status"] for k, v in m["scenarios"].items()}
    put("run.status", st, "manifest.json", "status scenariuszy")
    gpu = env["nvidia_smi"]["stdout"].splitlines()[1].split(", ")
    put("env.gpu", gpu[0], "environment.json", "nvidia-smi name")
    put("env.gpu_mem_gib", round(int(gpu[1].split()[0]) / 1024), "environment.json", "nvidia-smi memory.total MiB / 1024")
    cpu = [l.split(":", 1)[1].strip() for l in env["lscpu"]["stdout"].splitlines() if l.startswith("Model name")][0]
    put("env.cpu", cpu, "environment.json", "lscpu Model name")
    put("env.vcpu", int(env["runpod"]["RUNPOD_CPU_COUNT"]), "environment.json", "RUNPOD_CPU_COUNT (przydział poda)")
    put("env.ram_gb", int(env["runpod"]["RUNPOD_MEM_GB"]), "environment.json", "RUNPOD_MEM_GB (przydział poda)")
    put("env.kernel", env["uname"]["stdout"].split()[2], "environment.json", "uname -r")
    put("env.pg", env["postgres"]["version"].split()[1], "environment.json", "SELECT version()")
    put("env.pgvector", env["postgres"]["extensions"]["vector"], "environment.json", "pg_extension")
    put("env.kafka", env["kafka"]["kafka_topics_version"], "environment.json", "kafka-topics --version")
    put("env.ollama", env["ollama"]["version"], "environment.json", "/api/version")
    put("env.python", env["python"]["version"], "environment.json", "platform")
    put("env.docker", env["capabilities"]["docker"], "environment.json", "capabilities")
    put("env.net_admin", env["capabilities"]["net_admin"], "environment.json", "capabilities")
    for k, v in env["postgres"]["settings"].items():
        put(f"env.pg.{k}", v, "environment.json", "pg_settings")
    put("models", m["model_digests"], "manifest.json", "model_digests")

    # ------------------------------------------------------------------ A: selekcja
    r = pd.read_parquet(R / "schema_retrieval.parquet")
    for (s, k, v), g in r.groupby(["strategy", "k", "budget_variant"]):
        for col in ("table_recall", "table_precision", "column_recall", "n_tables", "ddl_tokens", "prompt_tokens"):
            put(f"A.{col}.{s}.{k}", float(g[col].mean()), "schema_retrieval.parquet", f"średnia {col} po {len(g)} zapytaniach ({v})")
    ds = [json.loads(l) for l in (R / "schema_quality_dataset.jsonl").read_text().splitlines()]
    put("A.n_queries", len(ds), "schema_quality_dataset.jsonl", "liczba zapytań")
    ngt = pd.Series([len(d["ground_truth_tables"]) for d in ds])
    for n, c in ngt.value_counts().sort_index().items():
        put(f"A.gt_tables.{n}", int(c), "schema_quality_dataset.jsonl", f"liczba zapytań z {n} tabelami GT")
    put("A.gt_tables_ge3", int((ngt >= 3).sum()), "schema_quality_dataset.jsonl", "zapytania z >= 3 tabelami GT")
    put("A.gt_tables_ge3_pct", float((ngt >= 3).mean() * 100), "schema_quality_dataset.jsonl", "% zapytań z >= 3 tabelami GT")
    put("A.difficulty", pd.Series([d["difficulty"] for d in ds]).value_counts().to_dict(), "schema_quality_dataset.jsonl", "liczność poziomów trudności")
    g5 = r[(r.strategy == "Graph") & (r.k == 5)].set_index("query_id").table_recall
    rr5 = r[(r.strategy == "RRF") & (r.k == 5)].set_index("query_id").table_recall
    put("A.expansion_gain_n", int((g5 > rr5).sum()), "schema_retrieval.parquet", "zapytania z recall Graph@5 > RRF@5")
    put("A.expansion_gain_pct", float((g5 > rr5).mean() * 100), "schema_retrieval.parquet", "% zapytań z recall Graph@5 > RRF@5")
    gk = r[(r.strategy == "Graph") & (r.k == 5)].n_tables
    put("A.graph5_ntables_min", int(gk.min()), "schema_retrieval.parquet", "min |T_final| Graph@5")
    put("A.graph5_ntables_max", int(gk.max()), "schema_retrieval.parquet", "max |T_final| Graph@5")
    fd = json.loads((R / "schema_full_dump_tokens.json").read_text())
    put("A.full_dump_tokens", fd["ddl_tokens_cl100k"], "schema_full_dump_tokens.json", "cl100k, jednolity format synthesize_ddl, 1000 tabel")
    put("A.full_dump_tables", fd["n_tables"], "schema_full_dump_tokens.json", "n_tables")
    put("A.reduction_vs_full_pct", (1 - V["A.ddl_tokens.Graph.5"]["value"] / fd["ddl_tokens_cl100k"]) * 100,
        "schema_retrieval.parquet + schema_full_dump_tokens.json", "1 - średni DDL Graph@5 / DDL pełnego zrzutu (ten sam format)")

    tests = pd.read_csv(R / "statistical_hypothesis_tests.csv")
    for _, t in tests[tests.status == "reported"].iterrows():
        key = "T." + t.test_id.replace(" ", "")
        put(key, {c: (None if pd.isna(t[c]) else (t[c].item() if hasattr(t[c], "item") else t[c])) for c in
                  ("mean_a", "mean_b", "N", "n_eff", "W", "U", "p_value", "rank_biserial_r", "ex_a", "ex_b", "only_a", "only_b",
                   "n_a", "n_b", "median_a", "median_b", "median_diff_estimate", "median_diff_ci_low", "median_diff_ci_high",
                   "slope_ms_per_1k_tokens", "ci_low", "ci_high") if c in t},
            "statistical_hypothesis_tests.csv", t.test_id)

    gen = pd.read_parquet(R / "schema_quality_generation.parquet")
    put("A.gen_records", len(gen), "schema_quality_generation.parquet", "liczba rekordów generacji")
    put("A.possible_truncation_n", int(gen.possible_truncation.sum()), "schema_quality_generation.parquet", "rekordy z possible_truncation")
    put("A.max_prompt_eval", int(gen.prompt_eval_count.max()), "schema_quality_generation.parquet", "maksymalny prompt_eval_count")
    put("A.num_ctx", int(gen.num_ctx.iloc[0]), "schema_quality_generation.parquet", "num_ctx")
    for (p, s, k, v), g in gen.groupby(["profile", "strategy", "k", "budget_variant"]):
        n = len(g)
        for col in ("ex_match", "valid_sql"):
            kk = int(g[col].sum())
            lo, hi = cp_interval(kk, n)
            put(f"A.{col}.{p}.{s}.{k}", {"pct": kk / n * 100, "k": kk, "n": n, "ci_low": lo * 100, "ci_high": hi * 100},
                "schema_quality_generation.parquet", f"odsetek {col}; 95% CI Cloppera-Pearsona")
        fail = g[~g.ex_match]
        put(f"A.errors.{p}.{s}.{k}", {"n_false": len(fail), **fail.error_category.value_counts().to_dict()},
            "schema_quality_generation.parquet", "liczba rekordów EX=False w kategoriach")
        put(f"A.eval_count_median.{p}.{s}.{k}", float(g.eval_count.median()), "schema_quality_generation.parquet", "mediana eval_count")
        put(f"A.total_ms_median.{p}.{s}.{k}", float(g.total_duration.median() / 1e6), "schema_quality_generation.parquet", "mediana total_duration [ms]")
    ex = gen[gen.budget_variant != "equal_budget"].groupby(["profile", "strategy"]).ex_match.mean() * 100
    for p in gen.profile.unique():
        base = ex[p].drop("Graph")
        put(f"A.best_baseline.{p}", {"strategy": base.idxmax(), "ex_pct": float(base.max())}, "schema_quality_generation.parquet",
            "baseline o najwyższym EX (k=5 lub Domain-Scoped)")
    # Taksonomia: kontrola sum
    dec = pd.read_csv(R / "schema_error_decomposition.csv")
    chk = dec.groupby(["profile", "strategy", "k", "budget_variant"])["count"].sum()
    nf = gen[~gen.ex_match].groupby(["profile", "strategy", "k", "budget_variant"]).size()
    put("A.taxonomy_consistent", bool((chk.sort_index() == nf.sort_index()).all()), "schema_error_decomposition.csv", "suma kategorii = liczba EX=False")

    # ------------------------------------------------------------------ ablacja
    ab = pd.read_csv(R / "ablation_m_exp_results.csv")
    calls = pd.read_parquet(R / "ablation_m_exp_calls.parquet")
    for _, row in ab.iterrows():
        mm = int(row.m_exp)
        put(f"AB.recall.{mm}", row.table_recall_at_5 * 100, "ablation_m_exp_results.csv", "Table-Recall@5 [%]")
        put(f"AB.ddl_tokens.{mm}", row.avg_ddl_tokens_uniform_format, "ablation_m_exp_results.csv", "średni DDL (format jednolity)")
    ss = json.loads((R / "ablation_m_exp_sweet_spot.json").read_text())
    put("AB.sweet_spot", ss["m_exp"], "ablation_m_exp_sweet_spot.json", ss["rule"])
    put("AB.sweet_gain_pp", ss["gain_to_next_pp"], "ablation_m_exp_sweet_spot.json", "wzrost recall do następnej wartości [pp]")
    c3 = calls[calls.m_exp == 3]
    for comp in ("rrf", "graph_load", "bridge_search", "ddl_synthesis", "total"):
        x = c3[f"t_{comp}_ns"] / 1e6
        put(f"AB.t.{comp}", {"median": float(x.median()), "p95": float(np.percentile(x, 95)), "max": float(x.max()), "n": int(len(x))},
            "ablation_m_exp_calls.parquet", f"czas składowej {comp} [ms] dla M_exp = 3 (mediana, p95, max)")

    # ------------------------------------------------------------------ TTFT
    tt = pd.read_parquet(R / "ttft_scaling.parquet")
    tt = tt[~tt.warmup]
    put("TT.n_per_point", int(tt.groupby(["profile", "target_ddl_tokens"]).size().min()), "ttft_scaling.parquet", "powtórzenia na punkt")
    put("TT.trunc", int(tt.possible_truncation.sum()), "ttft_scaling.parquet", "wywołania z possible_truncation")
    for (p, tgt), g in tt.groupby(["profile", "target_ddl_tokens"]):
        lo, hi = boot_median_ci(g.ttft_ms, seed)
        put(f"TT.{p}.{tgt}", {"prompt_eval": float(g.prompt_eval_count.median()), "ttft_median": float(g.ttft_ms.median()),
                              "ttft_p95": float(np.percentile(g.ttft_ms, 95)), "ttft_ci_low": lo, "ttft_ci_high": hi,
                              "total_median": float(g.total_ms.median()), "prefill_median": float(g.prompt_eval_duration.median() / 1e6),
                              "num_ctx": int(g.num_ctx.iloc[0]), "n": int(len(g))},
            "ttft_scaling.parquet", "mediana, p95, bootstrapowy 95% CI mediany (ziarno seeds.bootstrap)")

    # ------------------------------------------------------------------ B1 OOB
    o = pd.read_parquet(R / "oob_crossover_results.parquet")
    put("B1.modes", o.groupby("network_profile").measurement_mode.first().to_dict(), "oob_crossover_results.parquet", "measurement_mode")
    met = json.loads((R / "oob_crossover_metrics.json").read_text())
    for prof, d in met["profiles"].items():
        for mode, c in d.items():
            if isinstance(c, dict):
                put(f"B1.sstar.{prof}.{mode}", c, "oob_crossover_metrics.json", c.get("method", c.get("reason", "")))
    fit = json.loads((R / "oob_model_fit.json").read_text())
    put("B1.fit", fit, "oob_model_fit.json", "L = a + bS (MNK)")
    med = o.groupby(["network_profile", "transport_mode", "payload_size_bytes"]).e2e_ms.median()
    put("B1.medians", {f"{a}|{b}|{c}": float(v) for (a, b, c), v in med.items()}, "oob_crossover_results.parquet", "mediana E2E [ms]")
    lb50_in, lb50_shm = med[("loopback", "IN_BAND_BASE64", 52428800)], med[("loopback", "OOB_POSIX_SHM", 52428800)]
    put("B1.speedup50", float(lb50_in / lb50_shm), "oob_crossover_results.parquet", "mediana In-Band / mediana SHM przy 50 MB (loopback)")
    fb = o[(o.transport_mode == "IN_BAND_BASE64") & (o.network_profile == "loopback")].groupby("payload_size_bytes").frame_bytes.first()
    put("B1.frame_overhead_pct", {int(s): float((f / s - 1) * 100) for s, f in fb.items()}, "oob_crossover_results.parquet",
        "frame_bytes / payload - 1 (ramka JSON-RPC z Base64)")
    comp = o[(o.network_profile == "loopback") & (o.payload_size_bytes == 52428800)]
    put("B1.components50", {mode: {c: float(g[c].median() / 1e6) for c in g.columns if c.endswith("_ns") and g[c].notna().any() and c != "e2e_ns"}
                            for mode, g in comp.groupby("transport_mode")}, "oob_crossover_results.parquet", "mediany składowych [ms] przy 50 MB")
    for t in json.loads((R / "oob_h2_tests.json").read_text()):
        put(f"B1.h2.{t['payload_size_bytes']}", t, "oob_h2_tests.json", "Mann-Whitney + bootstrapowy CI różnicy median")

    # ------------------------------------------------------------------ B2 paginacja
    pa = pd.read_csv(R / "pagination_advanced_1m.csv")
    pm = pa.groupby(["variant", "offset_rows"]).latency_ms.agg(["median", "min", "max", "count"])
    put("B2.depth", {f"{v}|{o_}": row.to_dict() for (v, o_), row in pm.iterrows()}, "pagination_advanced_1m.csv", "mediana, min, max, n [ms]")
    med2 = pa.groupby(["variant", "offset_rows"]).latency_ms.median().unstack(0)
    lr = sps.linregress(med2.index.values, med2["Offset/Limit"].values)
    put("B2.offset_slope_per_100k", lr.slope * 1e5, "pagination_advanced_1m.csv", "regresja median OFFSET/LIMIT [ms / 100 tys. wierszy]")
    put("B2.offset_r2", lr.rvalue ** 2, "pagination_advanced_1m.csv", "R^2 regresji")
    lk = sps.linregress(med2.index.values, med2["Indexed Keyset B-Tree"].values)
    put("B2.keyset_slope_per_100k", lk.slope * 1e5, "pagination_advanced_1m.csv", "regresja median keyset [ms / 100 tys. wierszy]")
    put("B2.keyset_slope_p", lk.pvalue, "pagination_advanced_1m.csv", "p nachylenia keyset (H0: nachylenie = 0)")
    put("B2.ratio_950k", float(med2.loc[950000, "Offset/Limit"] / med2.loc[950000, "Indexed Keyset B-Tree"]), "pagination_advanced_1m.csv", "mediana OFFSET / keyset przy 950 000")
    un = pa[pa.variant == "Unindexed Keyset"].latency_ms
    put("B2.unindexed", {"min": float(un.min()), "max": float(un.max()), "median": float(un.median()), "max_offset": int(pa[pa.variant == "Unindexed Keyset"].offset_rows.max())},
        "pagination_advanced_1m.csv", "wariant bez indeksu")
    put("B2.reps", int(pm["count"].min()), "pagination_advanced_1m.csv", "powtórzeń na punkt")
    pr = pd.read_csv(R / "pagination_predicate_ablation.csv").groupby(["variant", "offset_rows"]).latency_ms.median().unstack(0)
    put("B2.predicate", {int(i): row.to_dict() for i, row in pr.iterrows()}, "pagination_predicate_ablation.csv", "mediana [ms]")
    plans = json.loads((R / "pagination_explain_plans.json").read_text())

    def walk(node, acc):
        acc["nodes"].append(node.get("Node Type"))
        if "Index Cond" in node:
            acc["index_cond"] += 1
        if "Filter" in node:
            acc["filter"] += 1
        acc["rows_removed"] += int(node.get("Rows Removed by Filter", 0) or 0)
        for c in node.get("Plans", []) or []:
            walk(c, acc)

    ex_rows = {}
    for p_ in plans:
        root = p_["plan"][0]
        acc = {"nodes": [], "index_cond": 0, "filter": 0, "rows_removed": 0}
        walk(root["Plan"], acc)
        ex_rows[f"{p_['variant']}|{p_['offset_rows']}"] = {
            "execution_ms": root["Execution Time"], "shared_hit": root["Plan"].get("Shared Hit Blocks", 0),
            "shared_read": root["Plan"].get("Shared Read Blocks", 0), "index_cond_nodes": acc["index_cond"],
            "filter_nodes": acc["filter"], "rows_removed_by_filter": acc["rows_removed"], "has_merge_append": "Merge Append" in acc["nodes"]}
    put("B2.explain", ex_rows, "pagination_explain_plans.json", "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)")
    put("B2.consistency", json.loads((R / "pagination_consistency.json").read_text()), "pagination_consistency.json", "spójność i snapshot")

    # ------------------------------------------------------------------ C1
    cs = pd.read_csv(R / "concurrency_summary.csv")
    for _, row in cs.iterrows():
        w = "na" if pd.isna(row.workers) else int(row.workers)
        put(f"C1.{row.variant}.{w}.{int(row.concurrency_level)}", {c: (row[c].item() if hasattr(row[c], "item") else row[c]) for c in
            ("requests_per_second", "lag_samples_n", "lag_median_ms", "lag_p95_ms", "lag_p99_ms", "lag_max_ms", "tree_mem_peak_mb",
             "children_max", "latency_p50_ms", "latency_p95_ms", "memory_metric")}, "concurrency_summary.csv", "wiersz poziomu")
    for t in json.loads((R / "concurrency_h3_tests.json").read_text()):
        put(f"C1.h3.{t['concurrency_level']}", t, "concurrency_h3_tests.json", "Mann-Whitney + bootstrapowy CI różnicy median")

    # ------------------------------------------------------------------ D
    sm = json.loads((R / "security_suite_metrics.json").read_text())
    put("D.unique", sm["unique_vectors_by_category"], "security_suite_metrics.json", "unikalne wektory")
    put("D.total_unique", sum(sm["unique_vectors_by_category"].values()), "security_suite_metrics.json", "suma unikalnych wektorów")
    for key in ("FPR", "AMR", "side_effect_allowed_rate", "sanitization_residual_rate"):
        d = sm[key]
        lo, hi = cp_interval(d["k"], d["n"])
        put(f"D.{key}", {**d, "ci_low": lo * 100, "ci_high": hi * 100}, "security_suite_metrics.json", "rate + 95% CI Cloppera-Pearsona")
    put("D.latency_mean", sm["latency_ms_mean_by_category"], "security_suite_metrics.json", "średni czas walidacji [ms]")
    put("D.latency_p95_all", sm["latency_ms_p95_all"], "security_suite_metrics.json", "p95 czasu [ms]")
    res = pd.read_csv(R / "security_suite_results.csv")
    se = res[res.category == "SIDE_EFFECT_SELECT"]
    put("D.side_effect_allowed", se[se.validator_allowed].sub_category.tolist(), "security_suite_results.csv", "wektory przepuszczone")
    xo = pd.read_csv(R / "security_xml_token_overhead.csv")
    put("D.xml_tokens", {k: {"median": float(g.added_tokens.median()), "min": int(g.added_tokens.min()), "max": int(g.added_tokens.max()), "n": int(len(g))}
                         for k, g in xo.groupby("record_kind")}, "security_xml_token_overhead.csv", "tokeny dodane przez enkapsulację")
    if (R / "security_llm_injection_summary.csv").exists():
        li = pd.read_csv(R / "security_llm_injection.jsonl".replace(".jsonl", "_summary.csv"))
        rows = [json.loads(l) for l in (R / "security_llm_injection.jsonl").read_text().splitlines()]
        dfl = pd.DataFrame(rows)
        piv = dfl.pivot(index="vector_id", columns="variant", values="attack_success")
        mc_b, mc_c = int((piv.raw & ~piv.xml_encapsulated).sum()), int((~piv.raw & piv.xml_encapsulated).sum())
        p_mc = float(sps.binomtest(mc_b, mc_b + mc_c, 0.5).pvalue) if mc_b + mc_c else 1.0
        put("D.llm_injection", {"by_variant": {r_.variant: {"n": int(r_.n), "k": int(round(r_.attack_success_rate * r_.n)),
                                                            "rate_pct": r_.attack_success_rate * 100} for r_ in li.itertuples()},
                                "mcnemar_raw_only": mc_b, "mcnemar_xml_only": mc_c, "mcnemar_p": p_mc, "model": dfl.model.iloc[0]},
            "security_llm_injection.jsonl", "odsetek dopasowań reguły llm_success_regex; McNemar raw vs xml (obliczony tu)")

    # ------------------------------------------------------------------ drift
    dj = json.loads((R / "schema_drift_summary.json").read_text())
    put("DR.telemetry", dj["drift_telemetry"], "schema_drift_summary.json", "czasy składowe [ms]")
    put("DR.phases", dj["phases"], "schema_drift_summary.json", "p50/p95/p99 [ms] faz")
    put("DR.discovered_pct", dj.get("drift_tables_discovered_pct"), "schema_drift_summary.json", "odsetek wykrytych tabel driftu")
    put("DR.errors", dj.get("in_flight_error_count"), "schema_drift_summary.json", "błędy zapytań w toku")

    (OUT / "wyniki.json").write_text(json.dumps(V, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    print(f"Zapisano {len(V)} wartości do {OUT / 'wyniki.json'}")


if __name__ == "__main__":
    main()
