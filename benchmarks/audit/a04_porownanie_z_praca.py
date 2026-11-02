#!/usr/bin/env python3
"""
Audyt, Krok 3: porównanie liczb z pracy (wersja HEAD, przed korektą Etapu A) z wartościami
przeliczonymi z plików danych. Każdy wiersz: identyfikator, miejsce w pracy, wartość w pracy,
wartość z danych, plik źródłowy, zgodność (tolerancja względna lub bezwzględna).

Wynik: out/a04_porownanie.csv oraz wydruk.
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import linregress

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "benchmarks" / "raw_results"
GPU = ROOT / "results-remote-gpu"
OUT = Path(__file__).resolve().parent / "out"

rows = []


def check(cid, place, thesis, data, source, tol=0.051, note=""):
    if isinstance(data, (int, float, np.floating, np.integer)) and isinstance(thesis, (int, float)):
        ok = abs(float(data) - thesis) <= tol
        dv = round(float(data), 6)
    else:
        ok = str(thesis) == str(data)
        dv = data
    rows.append({"id": cid, "miejsce": place, "praca": thesis, "dane": dv, "plik": source, "zgodne": ok, "uwagi": note})


def main() -> None:
    # ---------------- Scenariusz A ----------------
    sq = pd.read_parquet(RAW / "schema_quality_results.parquet")
    k5 = sq[sq.top_k == 5]
    B = k5[k5.engine_name.str.contains("Profil B")]
    A = k5[k5.engine_name.str.contains("Profil A")]
    names = {"Baseline A": "Baseline A (Full Schema)", "BM25": "Baseline B (Lexical BM25)",
             "RRF": "Baseline C (Hybrid pgvector+BM25 RRF)", "Dense": "Baseline D (Dense-only pgvector k-NN)",
             "Graph": "Autorskie (Graph Pruning & Bridges)"}
    g = lambda d, s: d[d.approach == names[s]]
    for s, rec, prec, cf1, val, ex, tok in [
        ("Baseline A", 15.0, 3.5, 1.1, 10.4, 5.8, 1032.2), ("BM25", 45.9, 30.9, 14.2, 18.0, 14.0, 492.1),
        ("RRF", 39.2, 25.8, 11.8, 18.2, 14.6, 493.7), ("Dense", 29.7, 19.0, 8.6, 16.4, 13.2, 491.8),
        ("Graph", 54.5, 29.8, 11.9, 19.8, 15.8, 2218.8)]:
        d = g(B, s)
        check(f"T7-{s}-recall", "tab. 7", rec, d.table_recall.mean() * 100, "schema_quality_results")
        check(f"T7-{s}-prec", "tab. 7", prec, d.table_precision.mean() * 100, "schema_quality_results")
        check(f"T7-{s}-cf1", "tab. 7", cf1, d.column_f1.mean() * 100, "schema_quality_results")
        check(f"T7-{s}-valid", "tab. 7", val, d.valid_sql.mean() * 100, "schema_quality_results")
        check(f"T7-{s}-EX", "tab. 7", ex, d.ex_match.mean() * 100, "schema_quality_results")
        check(f"T7-{s}-tok", "tab. 7 (opis: tokeny DDL)", tok, d.prompt_tokens.mean(), "schema_quality_results",
              note="kolumna prompt_tokens = cały prompt, nie sam DDL")
    for s, kv in [("Graph", {3: 45.0, 10: 66.8, 20: 79.3}), ("BM25", {3: 41.3, 10: 53.2, 20: 64.8}),
                  ("RRF", {3: 34.0, 10: 48.8, 20: 61.6}), ("Dense", {3: 24.4, 10: 40.1, 20: 49.8})]:
        for k, v in kv.items():
            d = g(sq[(sq.top_k == k) & sq.engine_name.str.contains("Profil A")], s)
            check(f"6.2.2-{s}@{k}", "6.2.2 / rys. 2", v, d.table_recall.mean() * 100, "schema_quality_results")
    check("6.2.3-A-EX-graph", "6.2.3 / rys. 3", 10.8, g(A, "Graph").ex_match.mean() * 100, "schema_quality_results")
    check("6.2.3-A-EX-baseA", "6.2.3 / rys. 3", 4.6, g(A, "Baseline A").ex_match.mean() * 100, "schema_quality_results")
    check("6.2.3-A-valid-graph", "6.2.3 / rys. 3", 12.0, g(A, "Graph").valid_sql.mean() * 100, "schema_quality_results")
    check("6.2.3-A-valid-baseA", "6.2.3 / rys. 3", 7.0, g(A, "Baseline A").valid_sql.mean() * 100, "schema_quality_results")
    fail = lambda d: d[~d.ex_match]
    pct = lambda d, c: (fail(d).error_category == c).mean() * 100
    cnt = lambda d, c: int((fail(d).error_category == c).sum())
    check("6.2.4-A-trunc-graph", "6.2.4 / rys. 4", 30.0, pct(g(A, "Graph"), "TRUNCATION"), "schema_quality_results (k=5)")
    check("6.2.4-A-trunc-baseA", "6.2.4 / rys. 4", 30.4, pct(g(A, "Baseline A"), "TRUNCATION"), "schema_quality_results (k=5)")
    check("6.2.4-A-syntax-graph", "6.2.4", 17.3, pct(g(A, "Graph"), "SYNTAX_ERROR"), "schema_quality_results (k=5)")
    check("6.2.4-A-colhal-graph", "6.2.4", 40.1, pct(g(A, "Graph"), "COLUMN_HALLUCINATION"), "schema_quality_results (k=5)")
    check("6.2.4-B-colhal-graph", "6.2.4", 82.9, pct(g(B, "Graph"), "COLUMN_HALLUCINATION"), "schema_quality_results (k=5)")
    check("6.2.4-B-tabmiss-graph", "6.2.4 / 6.8.1 / tab. 13", 11.9, pct(g(B, "Graph"), "TABLE_MISSING"), "schema_quality_results (k=5)")
    check("6.2.4-B-tabmiss-baseA", "6.2.4 / 6.8.1 / tab. 13", 27.0, pct(g(B, "Baseline A"), "TABLE_MISSING"), "schema_quality_results (k=5)")
    check("6.2.4-B-semmis-graph", "6.2.4", 4.8, pct(g(B, "Graph"), "SEMANTIC_MISMATCH"), "schema_quality_results (k=5)")
    check("6.2.4-B-tabmiss-graph-n", "6.2.4 („200 przypadków”)", 200, cnt(g(B, "Graph"), "TABLE_MISSING"), "schema_quality_results (k=5)", tol=0,
          note="200 = 4 x 50: zliczenie kopii k = 3, 5, 10, 20")
    check("6.2.4-B-tabmiss-baseA-n", "6.2.4 („508 przypadków”)", 508, cnt(g(B, "Baseline A"), "TABLE_MISSING"), "schema_quality_results (k=5)", tol=0,
          note="508 = 4 x 127; >500 niemożliwe")
    check("6.2.4-B-tabmiss-RRF", "(brak w pracy; kontekst)", "n/d", round(pct(g(B, "RRF"), "TABLE_MISSING"), 1), "schema_quality_results (k=5)",
          note="RRF ma mniej TABLE_MISSING (4,4%) niż Graph (11,9%)")
    ab = pd.read_csv(RAW / "ablation_m_exp_results.csv").set_index("m_exp")
    for m, r, t in [(0, 39.2, 1622.5), (1, 52.4, 1894.1), (2, 54.5, None), (3, 54.5, 1998.4), (5, 54.5, 1999.0)]:
        check(f"6.2.5-M{m}-recall", "6.2.5 / rys. 5", r, ab.loc[m, "table_recall_at_5"] * 100, "ablation_m_exp_results")
        if t:
            check(f"6.2.5-M{m}-tok", "6.2.5 / rys. 5", t, ab.loc[m, "avg_ddl_tokens"], "ablation_m_exp_results")
    check("6.2.5-M3-lat", "6.2.5", 20.6, ab.loc[3, "avg_graph_latency_ms"], "ablation_m_exp_results")
    check("4.3.4-lat<2ms", "4.3.4 („poniżej 2 ms”)", "<2 ms", f"{ab.avg_graph_latency_ms.min()}-{ab.avg_graph_latency_ms.max()} ms (średnie)",
          "ablation_m_exp_results", note="brak pomiaru samego kroku grafowego")
    check("tab14-lat-2.1ms", "tab. 14 / 7.3 („≈2,1 ms”, „do 4,9 ms”)", "2,1 / 4,9 ms", "brak w danych (20,6 ms; przebudowa grafu 5,328 ms w drift)",
          "ablation_m_exp_results; schema_drift_summary.json")
    sc = pd.read_csv(RAW / "schema_scaling_analysis.csv").set_index("table_count")
    check("abs-100555", "streszczenie, 6.2.1, 1.2, 4.3.1", 100555, sc.loc[1000, "baseline_tokens"], "schema_scaling_analysis.csv", tol=0)
    check("abs-97.8", "streszczenie, 6.2.1, tab. 14", 97.8, (1 - g(B, "Graph").prompt_tokens.mean() / sc.loc[1000, "baseline_tokens"]) * 100,
          "schema_scaling_analysis.csv + schema_quality_results", note="porównanie dwóch różnych pomiarów i formatów")
    check("tab13-PB1-<2200", "tab. 13 PB1 („poniżej 2200”)", "<2200", round(g(B, "Graph").prompt_tokens.mean(), 1), "schema_quality_results",
          note="średni prompt 2218,8 > 2200 (sam DDL z ablacji: 1998,4)")

    st = pd.read_csv(RAW / "statistical_hypothesis_tests.csv").set_index("hypothesis_id")
    check("T12-H1-p", "tab. 12 / 13", 5.53e-42, st.loc["H1_SCHEMA_RECALL", "p_value"], "statistical_hypothesis_tests.csv", tol=1e-44)
    check("T12-H1b-p", "tab. 12 / 13", 9.91e-42, st.loc["H1_COLUMN_RECALL", "p_value"], "statistical_hypothesis_tests.csv", tol=1e-44)
    check("T12-H2-p", "tab. 12 / 13", 1.78e-15, st.loc["H2_OOB_LATENCY_REDUCTION", "p_value"], "statistical_hypothesis_tests.csv", tol=1e-17)
    check("T12-H2-med-a", "tab. 12 / 13", 127.6, st.loc["H2_OOB_LATENCY_REDUCTION", "median_b"], "statistical_hypothesis_tests.csv")
    check("T12-H2-med-b", "tab. 12 / 13", 15.5, st.loc["H2_OOB_LATENCY_REDUCTION", "median_a"], "statistical_hypothesis_tests.csv")
    check("H3-stat-hardcoded", "rys. 8 (adnotacja „Wilcoxon p = 1,00e-5”)", "p = 1e-5", "wpisane na sztywno w statistical_analysis.py l. 437-439 (W=0, p=1e-5, r=0,95, N=100)",
          "statistical_hypothesis_tests.csv", note="test nie był wykonany")

    # ---------------- Scenariusz B1 (OOB) ----------------
    om = json.load(open(RAW / "oob_crossover_metrics.json", encoding="utf-8"))
    check("B1-S*", "6.3, streszczenie?, tab. 13-14", 5.19, om["crossover_point_kb"], "oob_crossover_metrics.json")
    check("B1-delta", "6.3", 0.0514, om["delta_oob_shm_ms"], "oob_crossover_metrics.json", tol=1e-5)
    check("B1-LAN", "6.3, tab. 13-14", 122.92, om["profiles"]["lan"]["crossover_point_http_kb"], "oob_crossover_metrics.json")
    check("B1-WAN", "6.3, tab. 13-14", 1681.22, om["profiles"]["wan"]["crossover_point_http_kb"], "oob_crossover_metrics.json")
    check("B1-speedup", "6.3, tab. 13-14", 6.87, om["speedup_at_50mb"], "oob_crossover_metrics.json")
    os_ = pd.read_csv(RAW / "oob_crossover_summary.csv")
    sel = lambda size, mode: os_[(os_.payload_size_bytes == size) & (os_.transport_mode == mode)]
    check("B1-50MB-shm", "6.3, tab. 13", 97.9, sel(52428800, "OOB_POSIX_SHM").mean_latency_ms.iloc[0], "oob_crossover_summary.csv")
    check("B1-50MB-inband", "6.3, tab. 13", 673.0, sel(52428800, "IN_BAND_BASE64").mean_latency_ms.iloc[0], "oob_crossover_summary.csv")
    check("H2-10MB-inband-med", "tab. 12-13", 127.6, sel(10485760, "IN_BAND_BASE64").median_latency_ms.iloc[0], "oob_crossover_summary.csv")
    check("H2-10MB-shm-med", "tab. 12-13", 15.5, sel(10485760, "OOB_POSIX_SHM").median_latency_ms.iloc[0], "oob_crossover_summary.csv")
    ht = pd.read_csv(RAW / "heavy_tail_comparison.csv")
    check("B2-base64", "6.4.1, tab. 14 (33,33%)", 33.33, (ht.inband_bytes.iloc[-1] / ht.raw_bytes.iloc[-1] - 1) * 100, "heavy_tail_comparison.csv (4 MB)")

    # ---------------- Paginacja ----------------
    ps = pd.read_csv(RAW / "pagination_advanced_summary.csv")
    pv = ps.pivot(index="offset_rows", columns="variant", values="p50")
    t8 = {0: (0.30, 0.27, 191.5), 100: (0.53, 0.30, 197.4), 1000: (0.47, 0.42, 196.9), 10000: (0.45, 1.95, 199.7),
          50000: (0.43, 16.64, 197.0), 100000: (0.46, 47.03, None), 250000: (0.44, 121.0, None), 500000: (0.45, 234.1, None),
          750000: (0.44, 324.5, None), 950000: (0.43, 429.1, None)}
    for off, (ki, ol, ku) in t8.items():
        check(f"T8-{off}-keyset", "tab. 8 / rys. 7", ki, pv.loc[off, "Indexed Keyset B-Tree"], "pagination_advanced_summary.csv", tol=0.006)
        check(f"T8-{off}-offset", "tab. 8 / rys. 7", ol, pv.loc[off, "Offset/Limit"], "pagination_advanced_summary.csv", tol=0.06)
        if ku:
            check(f"T8-{off}-unidx", "tab. 8 / rys. 7", ku, pv.loc[off, "Unindexed Keyset"], "pagination_advanced_summary.csv", tol=0.06)
    check("T8-991x", "tab. 8, streszczenie, tab. 13", 991, pv.loc[950000, "Offset/Limit"] / pv.loc[950000, "Indexed Keyset B-Tree"], "pagination_advanced_summary.csv", tol=0.6)
    lr = linregress(pv.index.values, pv["Offset/Limit"].values)
    check("6.4.2-slope", "6.4.2", 44.8, lr.slope * 1e5, "pagination_advanced_summary.csv (regresja median)", tol=0.06)
    check("6.4.2-R2", "6.4.2", 0.998, lr.rvalue ** 2, "pagination_advanced_summary.csv", tol=0.0006)
    km = ps[ps.variant == "Indexed Keyset B-Tree"]
    lk = linregress(km.offset_rows.values, km["mean"].values)
    check("6.4.2-keyset-slope", "6.4.2", -0.006, lk.slope * 1e5, "pagination_advanced_summary.csv (regresja średnich)", tol=0.0006)
    raw = pd.read_csv(RAW / "pagination_advanced_1m.csv")
    r950 = raw[(raw.variant == "Offset/Limit") & (raw.offset_rows == 950000)].latency_ms
    check("6.4.2-950k-range", "6.4.2 (412–639 ms)", "412-639", f"{r950.min():.0f}-{r950.max():.0f}", "pagination_advanced_1m.csv")
    un = raw[raw.variant == "Unindexed Keyset"].latency_ms
    check("6.4.2-unidx-range", "6.4.2 (190–211 ms)", "190-211", f"{un.min():.0f}-{un.max():.0f}", "pagination_advanced_1m.csv")
    check("6.4.2-unidx-med", "6.4.2 (mediana 197)", 197, un.median(), "pagination_advanced_1m.csv", tol=0.6)
    sd = ps[(ps.variant == "Indexed Keyset B-Tree") & (ps.offset_rows >= 10000)]["std"].max()
    check("6.4.2-sd", "6.4.2 (SD ≤ 0,04)", "<=0.04", "<=0.04" if sd <= 0.04 else f"{sd:.3f}", "pagination_advanced_summary.csv")
    pa = pd.read_csv(RAW / "pagination_predicate_ablation.csv")
    pm = pa.groupby(["variant", "offset_rows"]).latency_ms.median().unstack(0)
    print("Mediany ablacji predykatu:\n", pm.round(3))
    cols = list(pm.columns)
    dnf = [c for c in cols if "DNF" in c][0]
    ua = [c for c in cols if "UNION" in c][0]
    for off, (d, u) in {1000: (0.41, 0.31), 10000: (1.51, 0.31), 50000: (8.05, 0.29), 250000: (36.8, 0.50), 950000: (140.3, 0.34)}.items():
        check(f"T9-{off}-dnf", "tab. 9", d, pm.loc[off, dnf], "pagination_predicate_ablation.csv", tol=max(0.006, d * 0.002))
        check(f"T9-{off}-union", "tab. 9", u, pm.loc[off, ua], "pagination_predicate_ablation.csv", tol=0.006)
    check("T9-union-range", "6.4.3 (0,17–0,50)", "0.17-0.50", f"{pm[ua].min():.2f}-{pm[ua].max():.2f}", "pagination_predicate_ablation.csv")
    check("T9-reps", "tab. 9 (10 powtórzeń)", 10, int(pa.groupby(["variant", "offset_rows"]).size().max()), "pagination_predicate_ablation.csv", tol=0)
    check("6.4.3-explain", "6.4.3 (162 ms, 80 139 / 20 stron bufora, 0,6 ms)", "162 / 0,6 ms", "brak pliku z planem EXPLAIN", "-")

    # ---------------- Współbieżność ----------------
    cs = pd.read_csv(RAW / "concurrency_summary.csv")
    t10 = {1: (296.7, 3360.2, 1057.3, 82.5, 0.25), 5: (297.0, 3357.6, 1058.6, 285.3, 0.91), 10: (295.9, 3369.7, 1059.6, 269.1, 0.96),
           25: (295.3, 3376.6, 1060.4, 600.7, 0.52), 50: (296.0, 3368.7, 1061.3, 1176.7, 0.51), 100: (297.0, 3357.2, 1062.5, 1177.7, 0.53),
           500: (286.9, 8702.7, 1064.8, 1173.0, 0.53)}
    for m, (srps, slag, srss, prps, plag) in t10.items():
        s = cs[(cs.concurrency_level == m) & (cs.approach == "SYNCHRONOUS_AST")].iloc[0]
        p = cs[(cs.concurrency_level == m) & (cs.approach == "MULTIPROCESS_AST_POOL")].iloc[0]
        check(f"T10-{m}-sync-rps", "tab. 10", srps, s.requests_per_second, "concurrency_summary.csv")
        check(f"T10-{m}-sync-lag", "tab. 10", slag, s.mean_loop_lag_ms, "concurrency_summary.csv")
        check(f"T10-{m}-sync-rss", "tab. 10", srss, s.peak_rss_mb, "concurrency_summary.csv")
        check(f"T10-{m}-pool-rps", "tab. 10", prps, p.requests_per_second, "concurrency_summary.csv")
        check(f"T10-{m}-pool-lag", "tab. 10", plag, p.mean_loop_lag_ms, "concurrency_summary.csv", tol=0.006)
    p500 = cs[(cs.concurrency_level == 500) & (cs.approach == "MULTIPROCESS_AST_POOL")].iloc[0]
    s500 = cs[(cs.concurrency_level == 500) & (cs.approach == "SYNCHRONOUS_AST")].iloc[0]
    check("6.4.4-gain", "6.4.4, tab. 13-14 (+308,9%)", 308.9, (p500.requests_per_second / s500.requests_per_second - 1) * 100, "concurrency_summary.csv")
    check("6.4.4-rss", "6.4.4, tab. 14 (8,9 MB)", 8.9, p500.peak_rss_mb - p500.baseline_rss_mb, "concurrency_summary.csv")
    p100 = cs[(cs.concurrency_level == 100) & (cs.approach == "MULTIPROCESS_AST_POOL")].iloc[0]
    check("6.7.1-H3-p95", "6.7.1 (p95 0,97)", 0.97, p100.p95_loop_lag_ms, "concurrency_summary.csv", tol=0.006)
    check("6.7.1-H3-max", "6.7.1 (maks. 1,09)", 1.09, p100.max_loop_lag_ms, "concurrency_summary.csv", tol=0.006)
    check("tab13-H3-mediana", "tab. 13 („mediana lagu 0,53 vs 3357,2”)", "mediana", "w danych średnia (mean_loop_lag_ms); SYNC: 1 obserwacja (mean=p95=max)",
          "concurrency_summary.csv")

    # ---------------- Strumieniowanie ----------------
    sc_raw = pd.read_csv(RAW / "streaming_comparison.csv").set_index("approach")
    check("6.5-SSE-mean", "6.5, tab. 14 (1,13 ms)", 1.13, sc_raw.loc["MCP_SSE_PUSH", "ttfe_mean_ms"], "raw_results/streaming_comparison.csv")
    check("6.5-SSE-p99", "6.5 (p99 1,26)", 1.26, sc_raw.loc["MCP_SSE_PUSH", "ttfe_p99_ms"], "raw_results/streaming_comparison.csv", tol=0.01)
    check("6.5-P1-mean", "6.5 (66,65 ms)", 66.65, sc_raw.loc["REST_POLLING_1S", "ttfe_mean_ms"], "raw_results/streaming_comparison.csv", tol=0.006)
    check("6.5-P5-mean", "6.5 (126,59 ms)", 126.59, sc_raw.loc["REST_POLLING_5S", "ttfe_mean_ms"], "raw_results/streaming_comparison.csv", tol=0.006)
    check("6.5-P1-interval", "6.5 („interwał 1 s”)", 1000.0, sc_raw.loc["REST_POLLING_1S", "polling_interval_ms"], "raw_results/streaming_comparison.csv", tol=0,
          note="w danych 100 ms")
    check("6.5-P5-interval", "6.5 („interwał 5 s”)", 5000.0, sc_raw.loc["REST_POLLING_5S", "polling_interval_ms"], "raw_results/streaming_comparison.csv", tol=0,
          note="w danych 500 ms")
    check("6.5-n", "6.5 (liczebność)", "n/d", len(json.loads(sc_raw.loc["MCP_SSE_PUSH", "raw_samples"])), "raw_results/streaming_comparison.csv",
          note="2 próbki na wariant")
    check("6.5-qps", "6.5 (9,94 żądań/s)", 9.94, sc_raw.loc["REST_POLLING_1S", "idle_queries_per_sec"], "raw_results/streaming_comparison.csv", tol=0.001)
    check("6.5-bw", "6.5 (4,52 kB/s)", 4.52, sc_raw.loc["REST_POLLING_1S", "wasted_bandwidth_bytes_sec"] / 1000, "raw_results/streaming_comparison.csv", tol=0.006)
    check("6.5-58.7x", "6.5 (58,7×)", 58.7, sc_raw.loc["REST_POLLING_1S", "latency_penalty_ratio"], "raw_results/streaming_comparison.csv")
    gj = {d["approach"]: d for d in json.load(open(GPU / "streaming_comparison.json", encoding="utf-8"))}
    med = {k: float(np.median(v["raw_samples"])) for k, v in gj.items()}
    check("F9-medSSE", "rys. 9 (med 0,64 ms)", 0.64, med["MCP_SSE_PUSH"], "results-remote-gpu/streaming_comparison.json", tol=0.006,
          note="rys. 9 i tekst 6.5 pochodzą z różnych przebiegów")
    check("F9-709x", "rys. 9 (709×)", 709, med["REST_POLLING_1S"] / med["MCP_SSE_PUSH"], "results-remote-gpu/streaming_comparison.json", tol=0.6,
          note="adnotacja wpisana na sztywno w generate_publication_plots.py l. 911")
    check("F9-4165x", "rys. 9 (4165×)", 4165, med["REST_POLLING_5S"] / med["MCP_SSE_PUSH"], "results-remote-gpu/streaming_comparison.json", tol=0.6,
          note="adnotacja wpisana na sztywno")
    check("F9-bw-1s", "rys. 9 (457,0 B/s)", 457.0, gj["REST_POLLING_1S"]["wasted_bandwidth_bytes_sec"], "results-remote-gpu/streaming_comparison.json")

    # ---------------- Bezpieczeństwo ----------------
    ss = pd.read_csv(RAW / "security_suite_summary.csv")
    sm = json.load(open(RAW / "security_suite_metrics.json", encoding="utf-8"))
    check("T11-AMR", "tab. 11", 100.0, sm["amr_pct"], "security_suite_metrics.json")
    check("T11-FPR", "tab. 11", 0.0, sm["fpr_pct"], "security_suite_metrics.json")
    check("T11-ASR", "tab. 11", 0.0, sm["asr_pct"], "security_suite_metrics.json", note="ASR = test obecności tokenów po enkapsulacji, bez modelu")
    lat = ss.groupby("evaluation_metric").avg_latency_ms.mean()
    check("T11-lat-FPR", "tab. 11", 2.41, lat["FPR"], "security_suite_summary.csv")
    check("T11-lat-AMR", "tab. 11", 0.32, lat["AMR"], "security_suite_summary.csv", tol=0.006)
    check("T11-lat-ASR", "tab. 11", 0.04, lat["ASR"], "security_suite_summary.csv", tol=0.006)
    check("T11-lat-all", "tab. 11", 1.30, sm["mean_validation_latency_ms"], "security_suite_metrics.json", tol=0.006)
    check("6.6-p95", "6.6 (p95 3,46)", 3.46, sm["p95_validation_latency_ms"], "security_suite_metrics.json", tol=0.006)
    from scipy.stats import beta
    check("T11-CP-AMR", "tab. 11", 98.5, beta.ppf(0.025, 250, 1) * 100, "wzór Cloppera-Pearsona (250/250)")
    check("T11-CP-FPR", "tab. 11", 0.74, beta.ppf(0.975, 1, 500) * 100, "wzór (0/500)", tol=0.006)
    check("T11-CP-ASR", "tab. 11", 1.47, beta.ppf(0.975, 1, 250) * 100, "wzór (0/250)", tol=0.006)

    # ---------------- Drift (7.4) ----------------
    dj = json.load(open(RAW / "schema_drift_summary.json", encoding="utf-8"))
    t = dj["drift_telemetry"]
    for key, v in [("total_drift_reload_ms", 344.6), ("embedding_calc_ms", 52.7), ("db_insert_embeddings_ms", 140.8),
                   ("bm25_rebuild_ms", 113.4), ("graph_rebuild_ms", 5.3)]:
        check(f"7.4-{key}", "7.4 p. 2", v, t[key], "schema_drift_summary.json")
    check("7.4-p50-base", "7.4 p. 2", 9.9, dj["phases"]["baseline"]["p50"], "schema_drift_summary.json")
    check("7.4-p50-drift", "7.4 p. 2", 25.1, dj["phases"]["in_flight_drift"]["p50"], "schema_drift_summary.json")
    check("7.4-p95-drift", "7.4 p. 2", 70.1, dj["phases"]["in_flight_drift"]["p95"], "schema_drift_summary.json")

    df = pd.DataFrame(rows)
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT / "a04_porownanie.csv", index=False)
    pd.set_option("display.width", 250)
    pd.set_option("display.max_colwidth", 70)
    print(df[~df.zgodne][["id", "miejsce", "praca", "dane", "uwagi"]].to_string(index=False))
    print(f"\nZgodne: {int(df.zgodne.sum())} / {len(df)}")


if __name__ == "__main__":
    main()
