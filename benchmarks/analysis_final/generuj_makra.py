#!/usr/bin/env python3
r"""
Generuje jedno źródło liczb dla pracy z wyników końcowej rundy (out/wyniki.json, zob. analiza.py):
  doc/chapters/wyniki_liczby.tex   makra \newcommand dla liczb używanych w tekście,
  doc/chapters/tabele/*.tex        tabele wynikowe (ciała tabel) wstawiane przez \input.
Każde makro i tabela ma komentarz z plikiem źródłowym i sposobem obliczenia.
Format polski: przecinek dziesiętny ({,}), spacja cienka (\,) jako separator tysięcy.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[2]
W = json.loads((Path(__file__).resolve().parent / "out" / "wyniki.json").read_text(encoding="utf-8"))
DOC = ROOT / "doc" / "chapters"
TAB = DOC / "tabele"


def v(key: str) -> Any:
    return W[key]["value"]


def src(key: str) -> str:
    return f"{W[key]['src']}: {W[key]['how']}"


# --------------------------------------------------------------------------- formatowanie
def num(x: float, d: int = 1) -> str:
    s = f"{x:,.{d}f}".replace(",", "\u0000").replace(".", "{,}").replace("\u0000", "\\,")
    return s


def pct(x: float) -> str:
    return num(x, 1)


def ms(x: float) -> str:
    a = abs(x)
    if a < 10:
        return num(x, 2)
    if a < 100:
        return num(x, 1)
    return num(x, 0)


def intf(x: float) -> str:
    return num(float(x), 0)


def pval(p: float) -> str:
    if p is None or (isinstance(p, float) and math.isnan(p)):
        return "n/d"
    if p >= 0.001:
        return f"\\ensuremath{{{num(p, 3)}}}"
    m, e = f"{p:.1e}".split("e")
    return f"\\ensuremath{{{m.replace('.', '{,}')} \\cdot 10^{{{int(e)}}}}}"


def r2(x: float) -> str:
    return num(x, 2)


MACROS: List[Tuple[str, str, str]] = []  # (nazwa, wartość LaTeX, źródło)


def M(name: str, value: str, source: str) -> None:
    assert name.isalpha(), name
    MACROS.append((name, value, source))


T = lambda k: v(k)  # noqa: E731


def test(tid: str) -> Dict[str, Any]:
    return v("T." + tid.replace(" ", ""))


# --------------------------------------------------------------------------- środowisko i przebieg
M("wynRunId", v("run.id").replace("_", "\\_"), src("run.id"))
M("wynCommit", v("run.commit"), src("run.commit"))
M("wynGpu", v("env.gpu").replace("NVIDIA ", "NVIDIA~"), src("env.gpu"))
M("wynGpuMem", intf(v("env.gpu_mem_gib")), src("env.gpu_mem_gib"))
M("wynCpu", v("env.cpu"), src("env.cpu"))
M("wynVcpu", intf(v("env.vcpu")), src("env.vcpu"))
M("wynRam", intf(v("env.ram_gb")), src("env.ram_gb"))
M("wynKernel", v("env.kernel"), src("env.kernel"))
M("wynPg", v("env.pg"), src("env.pg"))
M("wynPgvector", v("env.pgvector"), src("env.pgvector"))
M("wynKafka", v("env.kafka"), src("env.kafka"))
M("wynOllama", v("env.ollama"), src("env.ollama"))
M("wynPython", v("env.python"), src("env.python"))
M("wynStmtTimeout", v("env.pg.statement_timeout").replace("ms", "~ms"), src("env.pg.statement_timeout"))
M("wynShaOk", intf(v("run.sha_ok")), src("run.sha_ok"))
_sb_kb = int(v("env.pg.shared_buffers").replace("kB", ""))
M("wynPgSharedBuffers", num(_sb_kb / 1024 / 1024, 1) + "~GiB", src("env.pg.shared_buffers") + " (kB / 1024^2)")
M("wynPgWorkMem", intf(int(v("env.pg.work_mem").replace("kB", "")) / 1024) + "~MiB", src("env.pg.work_mem") + " (kB / 1024)")
M("wynPgMaxConn", v("env.pg.max_connections"), src("env.pg.max_connections"))
M("wynPgJit", v("env.pg.jit"), src("env.pg.jit"))
M("wynPgParallel", v("env.pg.max_parallel_workers_per_gather"), src("env.pg.max_parallel_workers_per_gather"))

# --------------------------------------------------------------------------- Scenariusz A
M("wynNQueries", intf(v("A.n_queries")), src("A.n_queries"))
M("wynGtGeThreePct", pct(v("A.gt_tables_ge3_pct")), src("A.gt_tables_ge3_pct"))
M("wynExpGainN", intf(v("A.expansion_gain_n")), src("A.expansion_gain_n"))
M("wynExpGainPct", pct(v("A.expansion_gain_pct")), src("A.expansion_gain_pct"))
M("wynGraphTablesMin", intf(v("A.graph5_ntables_min")), src("A.graph5_ntables_min"))
M("wynGraphTablesMax", intf(v("A.graph5_ntables_max")), src("A.graph5_ntables_max"))
M("wynGraphTablesMean", num(v("A.n_tables.Graph.5"), 2), src("A.n_tables.Graph.5"))
for s, k, name in [("Graph", 5, "GraphFive"), ("RRF", 5, "RrfFive"), ("BM25", 5, "BmFive"), ("Dense", 5, "DenseFive"),
                   ("RRF", 8, "RrfEight"), ("BM25", 8, "BmEight"), ("DomainScoped", 15, "Domain"),
                   ("Graph", 20, "GraphTwenty"), ("Graph", 3, "GraphThree"), ("RRF", 10, "RrfTen"), ("BM25", 10, "BmTen")]:
    M(f"wynRecall{name}", pct(v(f"A.table_recall.{s}.{k}") * 100), src(f"A.table_recall.{s}.{k}"))
    M(f"wynDdl{name}", intf(v(f"A.ddl_tokens.{s}.{k}")), src(f"A.ddl_tokens.{s}.{k}"))
for s, k, name in [("Graph", 5, "GraphFive"), ("RRF", 8, "RrfEight"), ("BM25", 8, "BmEight")]:
    M(f"wynColRecall{name}", pct(v(f"A.column_recall.{s}.{k}") * 100), src(f"A.column_recall.{s}.{k}"))
M("wynFullDumpTokens", intf(v("A.full_dump_tokens")), src("A.full_dump_tokens"))
M("wynReductionVsFull", pct(v("A.reduction_vs_full_pct")), src("A.reduction_vs_full_pct"))
M("wynMaxPromptEval", intf(v("A.max_prompt_eval")), src("A.max_prompt_eval"))
M("wynNumCtx", intf(v("A.num_ctx")), src("A.num_ctx"))
M("wynTruncN", intf(v("A.possible_truncation_n")), src("A.possible_truncation_n"))
M("wynGenRecords", intf(v("A.gen_records")), src("A.gen_records"))

for tid, name in [("H1: Graph@5 vs RRF@8 (equal_budget)", "HjedenRrf"), ("H1: Graph@5 vs BM25@8 (equal_budget)", "HjedenBm"),
                  ("H1: Graph@5 vs RRF@5", "HjedenKonstr"), ("H1b: Graph@5 vs RRF@8 (equal_budget)", "HjedenbRrf"),
                  ("H1b: Graph@5 vs BM25@8 (equal_budget)", "HjedenbBm"), ("H1: Graph@5 vs BM25@10", "HjedenBmTen"),
                  ("H1: Graph@5 vs RRF@10", "HjedenRrfTen")]:
    t = test(tid)
    s_ = f"statistical_hypothesis_tests.csv: {tid}"
    M(f"wyn{name}P", pval(t["p_value"]), s_)
    M(f"wyn{name}R", r2(t["rank_biserial_r"]), s_ + " (korelacja rangowo-biserialna)")
    M(f"wyn{name}W", intf(t["W"]), s_ + " (statystyka W)")
    M(f"wyn{name}Neff", intf(t["n_eff"]), s_ + " (n_eff)")

prof_name = {"small": "Small", "large": "Large"}
for p in ("small", "large"):
    P = prof_name[p]
    for s, k, name in [("Graph", 5, "GraphFive"), ("RRF", 5, "RrfFive"), ("BM25", 5, "BmFive"), ("Dense", 5, "DenseFive"),
                       ("RRF", 8, "RrfEight"), ("BM25", 8, "BmEight"), ("DomainScoped", 15, "Domain")]:
        e = v(f"A.ex_match.{p}.{s}.{k}")
        M(f"wynEx{P}{name}", pct(e["pct"]), src(f"A.ex_match.{p}.{s}.{k}"))
        M(f"wynExCi{P}{name}", f"[{pct(e['ci_low'])}; {pct(e['ci_high'])}]", src(f"A.ex_match.{p}.{s}.{k}") + " (95% CI)")
        M(f"wynValid{P}{name}", pct(v(f"A.valid_sql.{p}.{s}.{k}")["pct"]), src(f"A.valid_sql.{p}.{s}.{k}"))
    for b, name in [("BM25@8(equal_budget)", "BmEight"), ("RRF@8(equal_budget)", "RrfEight"), ("RRF@5(k_sweep)", "RrfFive"),
                    ("BM25@5(k_sweep)", "BmFive"), ("DomainScoped@15(k_independent)", "Domain")]:
        t = test(f"EX[{p}]:Graph@5vs{b}")
        s_ = f"statistical_hypothesis_tests.csv: EX[{p}] Graph@5 vs {b} (McNemar dokładny)"
        M(f"wynMc{P}{name}P", pval(t["p_value"]), s_)
        M(f"wynMc{P}{name}A", intf(t["only_a"]), s_ + " (pary: tylko Graph poprawny)")
        M(f"wynMc{P}{name}B", intf(t["only_b"]), s_ + " (pary: tylko baseline poprawny)")
    errs = v(f"A.errors.{p}.Graph.5")
    M(f"wynErrFalse{P}", intf(errs["n_false"]), src(f"A.errors.{p}.Graph.5"))
    M(f"wynErrTableMissing{P}", intf(errs.get("TABLE_MISSING", 0)), src(f"A.errors.{p}.Graph.5"))
    M(f"wynErrTableMissingPct{P}", pct(100 * errs.get("TABLE_MISSING", 0) / errs["n_false"]), src(f"A.errors.{p}.Graph.5") + " (% EX=False)")
    M(f"wynErrJoin{P}", intf(errs.get("JOIN_CONDITION_MISMATCH", 0)), src(f"A.errors.{p}.Graph.5"))
    M(f"wynErrColumn{P}", intf(errs.get("COLUMN_HALLUCINATION", 0)), src(f"A.errors.{p}.Graph.5"))
    M(f"wynErrSemantic{P}", intf(errs.get("SEMANTIC_MISMATCH", 0)), src(f"A.errors.{p}.Graph.5"))

# --------------------------------------------------------------------------- ablacja
for mm, name in [(0, "Zero"), (1, "One"), (2, "Two"), (3, "Three"), (5, "Five")]:
    M(f"wynAbRecall{name}", pct(v(f"AB.recall.{mm}")), src(f"AB.recall.{mm}"))
    M(f"wynAbDdl{name}", intf(v(f"AB.ddl_tokens.{mm}")), src(f"AB.ddl_tokens.{mm}"))
M("wynAbSweet", intf(v("AB.sweet_spot")), src("AB.sweet_spot"))
M("wynAbSweetGain", num(v("AB.sweet_gain_pp"), 2), src("AB.sweet_gain_pp"))
for comp, name in [("rrf", "Rrf"), ("graph_load", "Graph"), ("bridge_search", "Bridge"), ("ddl_synthesis", "Ddl"), ("total", "Total")]:
    t = v(f"AB.t.{comp}")
    M(f"wynT{name}Med", ms(t["median"]), src(f"AB.t.{comp}") + " (mediana)")
    M(f"wynT{name}Pnn", ms(t["p95"]), src(f"AB.t.{comp}") + " (p95)")

# --------------------------------------------------------------------------- TTFT
M("wynTtftReps", intf(v("TT.n_per_point")), src("TT.n_per_point"))
for p in ("small", "large"):
    P = prof_name[p]
    t = test(f"TTFT[{p}]: nachylenie ms / 1000 tokenów promptu")
    M(f"wynTtftSlope{P}", ms(t["slope_ms_per_1k_tokens"]), f"statistical_hypothesis_tests.csv: TTFT[{p}] nachylenie")
    M(f"wynTtftSlopeCi{P}", f"[{ms(t['ci_low'])}; {ms(t['ci_high'])}]", f"statistical_hypothesis_tests.csv: TTFT[{p}] 95% CI bootstrap")
    for tgt, name in [(1000, "One"), (28000, "Max")]:
        d = v(f"TT.{p}.{tgt}")
        M(f"wynTtft{P}{name}", ms(d["ttft_median"]), src(f"TT.{p}.{tgt}") + " (mediana TTFT)")
        M(f"wynTtftPrompt{P}{name}", intf(d["prompt_eval"]), src(f"TT.{p}.{tgt}") + " (mediana prompt_eval_count)")

# --------------------------------------------------------------------------- B1
sst = lambda prof, mode: v(f"B1.sstar.{prof}.{mode}")  # noqa: E731
for prof, mode, name in [("loopback", "OOB_POSIX_SHM", "LoopShm"), ("loopback", "OOB_HTTP_STREAM", "LoopHttp"),
                         ("lan", "OOB_HTTP_STREAM", "Lan"), ("wan", "OOB_HTTP_STREAM", "Wan")]:
    c = sst(prof, mode)
    M(f"wynSstar{name}", num(c["s_star_kb"], 1), src(f"B1.sstar.{prof}.{mode}") + " (KiB)")
    M(f"wynSstarCi{name}", f"[{num(c['ci95_bytes'][0] / 1024, 1)}; {num(c['ci95_bytes'][1] / 1024, 1)}]", src(f"B1.sstar.{prof}.{mode}") + " (95% CI bootstrap, KiB)")
M("wynSpeedupFifty", num(v("B1.speedup50"), 2), src("B1.speedup50"))
med = v("B1.medians")
M("wynFiftyInband", ms(med["loopback|IN_BAND_BASE64|52428800"]), "oob_crossover_results.parquet: mediana E2E In-Band 50 MB (loopback)")
M("wynFiftyShm", ms(med["loopback|OOB_POSIX_SHM|52428800"]), "oob_crossover_results.parquet: mediana E2E SHM 50 MB (loopback)")
M("wynFrameOverheadTen", num(v("B1.frame_overhead_pct")["10485760"], 2), src("B1.frame_overhead_pct") + " (10 MB)")
M("wynFrameOverheadSmall", num(v("B1.frame_overhead_pct")["512"], 1), src("B1.frame_overhead_pct") + " (512 B)")
comp50 = v("B1.components50")
M("wynFiftyEnc", ms(comp50["IN_BAND_BASE64"]["base64_encode_ns"]), src("B1.components50") + " (Base64 encode)")
M("wynFiftyDec", ms(comp50["IN_BAND_BASE64"]["base64_decode_ns"]), src("B1.components50") + " (Base64 decode)")
M("wynFiftySha", ms(comp50["IN_BAND_BASE64"]["sha256_ns"]), src("B1.components50") + " (SHA-256)")
M("wynFiftyShmCopy", ms(comp50["OOB_POSIX_SHM"]["shm_copy_ns"]), src("B1.components50") + " (kopiowanie do SHM)")
for size, name in [(2048, "Small"), (10485760, "Big")]:
    t = v(f"B1.h2.{size}")
    s_ = src(f"B1.h2.{size}")
    M(f"wynHdwa{name}P", pval(t["p_value"]), s_)
    M(f"wynHdwa{name}Shm", ms(t["median_a"]), s_ + " (mediana SHM)")
    M(f"wynHdwa{name}Inband", ms(t["median_b"]), s_ + " (mediana In-Band)")
    M(f"wynHdwa{name}R", r2(t["rank_biserial_r"]), s_ + " (r)")
    ci = t["median_diff_ci"]
    M(f"wynHdwa{name}Ci", f"[{ms(ci['ci_low'])}; {ms(ci['ci_high'])}]", s_ + " (95% CI różnicy median)")
M("wynOobReps", intf(v("B1.h2.2048")["n_a"]), src("B1.h2.2048") + " (n powtórzeń)")
prof_cfg = {"lan": (1.0, 1000.0), "wan": (30.0, 250.0)}

# --------------------------------------------------------------------------- B2
dp = v("B2.depth")
M("wynKeysetDeep", ms(dp["Indexed Keyset B-Tree|950000"]["median"]), "pagination_advanced_1m.csv: mediana keyset przy 950 000")
M("wynKeysetFirst", ms(dp["Indexed Keyset B-Tree|0"]["median"]), "pagination_advanced_1m.csv: mediana keyset przy 0")
M("wynOffsetDeep", ms(dp["Offset/Limit|950000"]["median"]), "pagination_advanced_1m.csv: mediana OFFSET przy 950 000")
M("wynOffsetDeepMin", ms(dp["Offset/Limit|950000"]["min"]), "pagination_advanced_1m.csv: min OFFSET przy 950 000")
M("wynOffsetDeepMax", ms(dp["Offset/Limit|950000"]["max"]), "pagination_advanced_1m.csv: max OFFSET przy 950 000")
M("wynRatioDeep", intf(v("B2.ratio_950k")), src("B2.ratio_950k"))
M("wynOffsetSlope", num(v("B2.offset_slope_per_100k"), 1), src("B2.offset_slope_per_100k"))
M("wynOffsetRsq", num(v("B2.offset_r2"), 3), src("B2.offset_r2"))
M("wynKeysetSlope", num(v("B2.keyset_slope_per_100k"), 4), src("B2.keyset_slope_per_100k"))
M("wynKeysetSlopeP", pval(v("B2.keyset_slope_p")), src("B2.keyset_slope_p"))
un = v("B2.unindexed")
M("wynUnidxMin", ms(un["min"]), src("B2.unindexed"))
M("wynUnidxMax", ms(un["max"]), src("B2.unindexed"))
M("wynUnidxMed", ms(un["median"]), src("B2.unindexed"))
M("wynPagReps", intf(v("B2.reps")), src("B2.reps"))
pr = v("B2.predicate")
M("wynDnfDeep", ms(pr["950000"]["DNF (OR)"]), src("B2.predicate") + " (DNF, 950 000)")
M("wynUnionDeep", ms(pr["950000"]["UNION ALL"]), src("B2.predicate") + " (UNION ALL, 950 000)")
exl = v("B2.explain")
M("wynExplainDnfMs", ms(exl["DNF (OR)|950000"]["execution_ms"]), src("B2.explain") + " (Execution Time, DNF, 950 000)")
M("wynExplainUnionMs", ms(exl["UNION ALL|950000"]["execution_ms"]), src("B2.explain") + " (Execution Time, UNION ALL, 950 000)")
M("wynExplainDnfRemoved", intf(exl["DNF (OR)|950000"]["rows_removed_by_filter"]), src("B2.explain") + " (Rows Removed by Filter)")
M("wynExplainDnfBuf", intf(exl["DNF (OR)|950000"]["shared_hit"] + exl["DNF (OR)|950000"]["shared_read"]), src("B2.explain") + " (strony bufora hit+read)")
M("wynExplainUnionBuf", intf(exl["UNION ALL|950000"]["shared_hit"] + exl["UNION ALL|950000"]["shared_read"]), src("B2.explain") + " (strony bufora hit+read)")
M("wynExplainUnionIdx", intf(exl["UNION ALL|950000"]["index_cond_nodes"]), src("B2.explain") + " (węzły z Index Cond)")
cons = v("B2.consistency")["consistency"]
M("wynConsSamples", intf(cons["sample_limit"]), "pagination_consistency.json: sample_limit")

# --------------------------------------------------------------------------- C1
c = lambda var, w, m: v(f"C1.{var}.{w}.{m}")  # noqa: E731
for var, w, name in [("SYNC", "na", "Sync"), ("THREAD", 4, "Thread"), ("POOL", 4, "Pool")]:
    for m, mn in [(1, "One"), (100, "Hundred"), (500, "Fivehundred")]:
        d = c(var, w, m)
        s_ = f"concurrency_summary.csv: {var} workers={w} M={m}"
        M(f"wynCc{name}{mn}Rps", intf(d["requests_per_second"]), s_ + " (RPS)")
        M(f"wynCc{name}{mn}LagMed", ms(d["lag_median_ms"]), s_ + " (mediana lagu)")
        M(f"wynCc{name}{mn}LagPnn", ms(d["lag_p99_ms"]), s_ + " (p99 lagu)")
        M(f"wynCc{name}{mn}LagN", intf(d["lag_samples_n"]), s_ + " (liczba próbek lagu)")
        M(f"wynCc{name}{mn}Mem", num(d["tree_mem_peak_mb"] / 1024, 2), s_ + " (szczytowa suma PSS drzewa procesów, GiB)")
M("wynCcPoolPerWorker", num((c("POOL", 4, 100)["tree_mem_peak_mb"] - c("SYNC", "na", 100)["tree_mem_peak_mb"]) / 1024 / 4, 2),
  "concurrency_summary.csv: (PSS POOL w=4 - PSS SYNC) / 4 przy M=100 [GiB na proces roboczy]")
for w, name in [(2, "Two"), (8, "Eight")]:
    d = c("POOL", w, 100)
    M(f"wynCcPoolW{name}Rps", intf(d["requests_per_second"]), f"concurrency_summary.csv: POOL workers={w} M=100 (RPS)")
    M(f"wynCcPoolW{name}Mem", num(d["tree_mem_peak_mb"] / 1024, 2), f"concurrency_summary.csv: POOL workers={w} M=100 (PSS GiB)")
for m, name in [(100, "Hundred"), (500, "Fivehundred")]:
    t = v(f"C1.h3.{m}")
    M(f"wynHtrzy{name}P", pval(t["p_value"]), src(f"C1.h3.{m}"))
    M(f"wynHtrzy{name}Ci", f"[{ms(t['median_diff_ci']['ci_low'])}; {ms(t['median_diff_ci']['ci_high'])}]", src(f"C1.h3.{m}") + " (95% CI różnicy median)")

# --------------------------------------------------------------------------- D
u = v("D.unique")
M("wynSecTotal", intf(v("D.total_unique")), src("D.total_unique"))
M("wynSecAnalytic", intf(u["ANALYTICAL_SELECT"]), src("D.unique"))
M("wynSecInjection", intf(u["SQL_INJECTION"]), src("D.unique"))
M("wynSecSideEffect", intf(u["SIDE_EFFECT_SELECT"]), src("D.unique"))
M("wynSecPrompt", intf(u["PROMPT_INJECTION"]), src("D.unique"))
for key, name in [("AMR", "Amr"), ("FPR", "Fpr"), ("side_effect_allowed_rate", "Side"), ("sanitization_residual_rate", "Resid")]:
    d = v(f"D.{key}")
    M(f"wyn{name}", pct(d["rate_pct"]), src(f"D.{key}"))
    M(f"wyn{name}K", intf(d["k"]), src(f"D.{key}") + " (k)")
    M(f"wyn{name}Ci", f"[{pct(d['ci_low'])}; {pct(d['ci_high'])}]", src(f"D.{key}") + " (95% CI Cloppera-Pearsona)")
lat = v("D.latency_mean")
M("wynLatAnalytic", ms(lat["ANALYTICAL_SELECT"]), src("D.latency_mean"))
M("wynLatInjection", ms(lat["SQL_INJECTION"]), src("D.latency_mean"))
M("wynLatPpp", ms(v("D.latency_p95_all")), src("D.latency_p95_all"))
xt = v("D.xml_tokens")
M("wynXmlMin", intf(min(d["min"] for d in xt.values())), src("D.xml_tokens"))
M("wynXmlMax", intf(max(d["max"] for d in xt.values())), src("D.xml_tokens"))
M("wynXmlMedPi", intf(xt["PROMPT_INJECTION"]["median"]), src("D.xml_tokens"))
li = v("D.llm_injection")
M("wynLlmRawK", intf(li["by_variant"]["raw"]["k"]), src("D.llm_injection"))
M("wynLlmXmlK", intf(li["by_variant"]["xml_encapsulated"]["k"]), src("D.llm_injection"))
M("wynLlmN", intf(li["by_variant"]["raw"]["n"]), src("D.llm_injection"))
M("wynLlmP", pval(li["mcnemar_p"]), src("D.llm_injection"))

# --------------------------------------------------------------------------- drift
dt = v("DR.telemetry")
ph = v("DR.phases")
M("wynDriftTables", intf(dt["num_tables_migrated"]), src("DR.telemetry"))
M("wynDriftTotal", ms(dt["total_drift_reload_ms"]), src("DR.telemetry"))
M("wynDriftEmb", ms(dt["embedding_calc_ms"]), src("DR.telemetry"))
M("wynDriftIns", ms(dt["db_insert_embeddings_ms"]), src("DR.telemetry"))
M("wynDriftBm", ms(dt["bm25_rebuild_ms"]), src("DR.telemetry"))
M("wynDriftGraph", ms(dt["graph_rebuild_ms"]), src("DR.telemetry"))
M("wynDriftBase", ms(ph["baseline"]["p50"]), src("DR.phases"))
M("wynDriftInflight", ms(ph["in_flight_drift"]["p50"]), src("DR.phases"))
M("wynDriftInflightPpp", ms(ph["in_flight_drift"]["p95"]), src("DR.phases"))
M("wynDriftErrors", intf(v("DR.errors")), src("DR.errors"))


# --------------------------------------------------------------------------- wersje angielskie (abstract)
EN = ["ReductionVsFull", "DdlGraphFive", "FullDumpTokens", "RecallGraphFive", "RecallRrfEight", "RecallBmEight",
      "ExSmallGraphFive", "ExLargeGraphFive", "KeysetDeep", "OffsetDeep", "SstarLoopShm", "SpeedupFifty",
      "CcSyncHundredLagMed", "CcPoolHundredLagMed", "CcPoolHundredMem", "CcSyncHundredMem", "SecInjection",
      "SecSideEffect", "NQueries"]
_by_name = {n: (val, s_) for n, val, s_ in MACROS}
for n in EN:
    val, s_ = _by_name["wyn" + n]
    M(f"wyn{n}En", val.replace("{,}", ".").replace("\\,", ","), s_ + " (format angielski)")


# =========================================================================== tabele
def write_table(name: str, header_comment: str, rows: List[str]) -> None:
    TAB.mkdir(parents=True, exist_ok=True)
    (TAB / f"{name}.tex").write_text(f"% Wygenerowano: benchmarks/analysis_final/generuj_makra.py\n% Źródło: {header_comment}\n" + "\n".join(rows) + "\n", encoding="utf-8")


LBL = {"DomainScoped": "Domain-Scoped (15 tabel)", "BM25": "BM25", "Dense": "Dense-only k-NN", "RRF": "Hybrid RRF", "Graph": "Ekspansja grafowa (autorska)"}
SEL = [("DomainScoped", 15), ("BM25", 5), ("Dense", 5), ("RRF", 5), ("Graph", 5), ("BM25", 8), ("RRF", 8)]


def lab(s: str, k: int) -> str:
    if s == "DomainScoped":
        return LBL[s]
    return f"{LBL[s]}, $k = {k}$" + (" (równy budżet)" if k == 8 else "")


rows = []
for s, k in SEL:
    rows.append(f"{lab(s, k)} & {num(v(f'A.n_tables.{s}.{k}'), 2)} & {pct(v(f'A.table_recall.{s}.{k}') * 100)} & {pct(v(f'A.table_precision.{s}.{k}') * 100)} & "
                f"{pct(v(f'A.column_recall.{s}.{k}') * 100)} & {intf(v(f'A.ddl_tokens.{s}.{k}'))} & {intf(v(f'A.prompt_tokens.{s}.{k}'))} \\\\")
write_table("tab_selekcja", "schema_retrieval.parquet (średnie po 500 zapytaniach)", rows)

rows = []
for s, k in SEL:
    cells = []
    for p in ("small", "large"):
        vv, e = v(f"A.valid_sql.{p}.{s}.{k}"), v(f"A.ex_match.{p}.{s}.{k}")
        cells += [pct(vv["pct"]), f"{pct(e['pct'])} [{pct(e['ci_low'])}; {pct(e['ci_high'])}]"]
    rows.append(f"{lab(s, k)} & " + " & ".join(cells) + " \\\\")
write_table("tab_text2sql", "schema_quality_generation.parquet (odsetki; 95% CI Cloppera-Pearsona)", rows)

CATS = ["TABLE_MISSING", "COLUMN_HALLUCINATION", "SEMANTIC_MISMATCH", "JOIN_CONDITION_MISMATCH", "SYNTAX_ERROR", "TRUNCATION"]
rows = []
for p, pn in (("small", "3B"), ("large", "7B")):
    for s, k in SEL:
        e = v(f"A.errors.{p}.{s}.{k}")
        rows.append(f"{pn} & {lab(s, k)} & {intf(e['n_false'])} & " + " & ".join(intf(e.get(c_, 0)) for c_ in CATS) + " \\\\")
    if p == "small":
        rows.append("\\midrule")
write_table("tab_taksonomia", "schema_quality_generation.parquet (liczby rekordów z EX = False w kategoriach)", rows)

rows = []
for tgt in (1000, 2000, 4000, 8000, 16000, 28000):
    cells = []
    for p in ("small", "large"):
        d = v(f"TT.{p}.{tgt}")
        cells += [f"{ms(d['ttft_median'])} [{ms(d['ttft_ci_low'])}; {ms(d['ttft_ci_high'])}]", ms(d["ttft_p95"]), ms(d["total_median"])]
    d0 = v(f"TT.small.{tgt}")
    rows.append(f"{intf(d0['prompt_eval'])} & {intf(d0['num_ctx'])} & " + " & ".join(cells) + " \\\\")
write_table("tab_ttft", "ttft_scaling.parquet (mediana [95% CI bootstrap], p95, mediana czasu całkowitego; ms)", rows)

rows = []
for prof, mode in [("loopback", "OOB_POSIX_SHM"), ("loopback", "OOB_HTTP_STREAM"), ("lan", "OOB_HTTP_STREAM"), ("wan", "OOB_HTTP_STREAM")]:
    cc = sst(prof, mode)
    mm = v("B1.modes")[prof]
    par = "--" if prof == "loopback" else f"RTT {num(prof_cfg[prof][0], 0)}~ms, {intf(prof_cfg[prof][1])}~Mb/s"
    mlabel = {"measured_loopback": "zmierzony", "emulated": "modelowany", "netem": "emulowany (netem)"}.get(mm, mm)
    plabel = {"loopback": "jeden węzeł", "lan": "LAN", "wan": "WAN"}[prof]
    olabel = {"OOB_POSIX_SHM": "SHM", "OOB_HTTP_STREAM": "obiekt + URI"}[mode]
    ci = "--" if mm != "measured_loopback" else f"[{num(cc['ci95_bytes'][0] / 1024, 1)}; {num(cc['ci95_bytes'][1] / 1024, 1)}]"
    rows.append(f"{plabel} & {olabel} & {mlabel} & {par} & {num(cc['s_star_kb'], 1)} & {ci} \\\\")
write_table("tab_sstar", "oob_crossover_metrics.json (S* w KiB, 95% CI bootstrap)", rows)

rows = []
for off in (0, 100, 1000, 10000, 50000, 100000, 250000, 500000, 750000, 950000):
    k_ = dp[f"Indexed Keyset B-Tree|{off}"]
    o_ = dp[f"Offset/Limit|{off}"]
    u_ = dp.get(f"Unindexed Keyset|{off}")
    rows.append(f"{intf(off)} & {intf(off // 50 + 1)} & {ms(k_['median'])} & {ms(o_['median'])} & {ms(u_['median']) if u_ else '--'} & "
                f"{num(o_['median'] / k_['median'], 1)}$\\times$ \\\\")
write_table("tab_paginacja", "pagination_advanced_1m.csv (mediana z 10 powtórzeń, ms)", rows)

rows = []
for off in (1000, 10000, 50000, 250000, 950000):
    d = pr[str(off)]
    rows.append(f"{intf(off)} & {ms(d['DNF (OR)'])} & {ms(d['UNION ALL'])} & {num(d['DNF (OR)'] / d['UNION ALL'], 1)}$\\times$ \\\\")
write_table("tab_predykat", "pagination_predicate_ablation.csv (mediana z 10 powtórzeń, ms)", rows)

rows = []
for off in (10000, 250000, 950000):
    a, b = exl[f"DNF (OR)|{off}"], exl[f"UNION ALL|{off}"]
    rows.append(f"{intf(off)} & {ms(a['execution_ms'])} & {intf(a['rows_removed_by_filter'])} & {intf(a['shared_hit'] + a['shared_read'])} & "
                f"{ms(b['execution_ms'])} & {intf(b['index_cond_nodes'])} & {intf(b['shared_hit'] + b['shared_read'])} \\\\")
write_table("tab_explain", "pagination_explain_plans.json (Execution Time [ms], Rows Removed by Filter, strony bufora shared hit+read, liczba węzłów z Index Cond)", rows)

rows = []
for var, w in [("SYNC", "na"), ("THREAD", 4), ("POOL", 4)]:
    for m in (1, 5, 10, 25, 50, 100, 500):
        d = c(var, w, m)
        rows.append(f"{var} & {m} & {intf(d['requests_per_second'])} & {intf(d['lag_samples_n'])} & {ms(d['lag_median_ms'])} & {ms(d['lag_p95_ms'])} & "
                    f"{ms(d['lag_p99_ms'])} & {num(d['tree_mem_peak_mb'] / 1024, 2)} \\\\")
    rows.append("\\midrule")
for var, w in [("THREAD", 2), ("THREAD", 8), ("POOL", 2), ("POOL", 8)]:
    d = c(var, w, 100)
    rows.append(f"{var} ($w = {w}$) & 100 & {intf(d['requests_per_second'])} & {intf(d['lag_samples_n'])} & {ms(d['lag_median_ms'])} & {ms(d['lag_p95_ms'])} & "
                f"{ms(d['lag_p99_ms'])} & {num(d['tree_mem_peak_mb'] / 1024, 2)} \\\\")
write_table("tab_wspolbieznosc", "concurrency_summary.csv (RPS, liczba próbek lagu, mediana/p95/p99 lagu [ms], szczytowa suma PSS drzewa procesów [GiB])", rows)

rows = []
for key, cat, metric in [("FPR", "ANALYTICAL_SELECT", "FPR (odrzucone poprawne)"), ("AMR", "SQL_INJECTION", "AMR (zablokowane)"),
                         ("side_effect_allowed_rate", "SIDE_EFFECT_SELECT", "przepuszczone przez walidator"),
                         ("sanitization_residual_rate", "PROMPT_INJECTION", "rekordy z pozostałym tokenem sterującym")]:
    d = v(f"D.{key}")
    rows.append(f"{cat.replace('_', '\\_')} & {d['n']} & {metric} & {d['k']} & {pct(d['rate_pct'])} & [{pct(d['ci_low'])}; {pct(d['ci_high'])}] & {ms(lat[cat])} \\\\")
write_table("tab_bezpieczenstwo", "security_suite_metrics.json, security_suite_results.csv (95% CI Cloppera-Pearsona; średni czas walidacji [ms])", rows)

rows = []
hyp = [("H1", "Table-Recall, Graph@5 vs RRF@8", "H1: Graph@5 vs RRF@8 (equal_budget)", "W"),
       ("H1", "Table-Recall, Graph@5 vs BM25@8", "H1: Graph@5 vs BM25@8 (equal_budget)", "W"),
       ("H1b", "Column-Recall, Graph@5 vs RRF@8", "H1b: Graph@5 vs RRF@8 (equal_budget)", "W"),
       ("H1b", "Column-Recall, Graph@5 vs BM25@8", "H1b: Graph@5 vs BM25@8 (equal_budget)", "W")]
for h, label, tid, stat in hyp:
    t = test(tid)
    rows.append(f"{h} & {label} & Wilcoxon & {intf(t['N'])} ({intf(t['n_eff'])}) & {pct(t['mean_a'] * 100)} / {pct(t['mean_b'] * 100)}~\\% & "
                f"$W = {intf(t['W'])}$ & {pval(t['p_value'])} & {r2(t['rank_biserial_r'])} \\\\")
for size, label in [(2048, "2~KB"), (10485760, "10~MB")]:
    t = v(f"B1.h2.{size}")
    rows.append(f"H2 & SHM vs In-Band, loopback, {label} & Mann--Whitney & {t['n_a']} / {t['n_b']} & {ms(t['median_a'])} / {ms(t['median_b'])}~ms & "
                f"$U = {intf(t['U'])}$ & {pval(t['p_value'])} & {r2(t['rank_biserial_r'])} \\\\")
for m in (100, 500):
    t = v(f"C1.h3.{m}")
    rows.append(f"H3 & lag, POOL vs SYNC, $M = {m}$ & Mann--Whitney & {t['n_a']} / {t['n_b']} & {ms(t['median_a'])} / {ms(t['median_b'])}~ms & "
                f"$U = {intf(t['U'])}$ & {pval(t['p_value'])} & {r2(t['rank_biserial_r'])} \\\\")
write_table("tab_hipotezy", "statistical_hypothesis_tests.csv, oob_h2_tests.json, concurrency_h3_tests.json (r: korelacja rangowo-biserialna; dla Manna-Whitneya r = 1 - 2U/(n_a n_b))", rows)

O2 = json.loads((ROOT / "results-remote-new-sunday" / "derived" / "o2_holm_mcnemar.json").read_text(encoding="utf-8"))
rows = []
for p, pn in (("small", "3B"), ("large", "7B")):
    for b, lbl_ in [("RRF@5(k_sweep)", "RRF, $k = 5$"), ("BM25@5(k_sweep)", "BM25, $k = 5$"), ("RRF@8(equal_budget)", "RRF, $k = 8$"),
                    ("BM25@8(equal_budget)", "BM25, $k = 8$"), ("Dense@5(k_sweep)", "Dense-only, $k = 5$"), ("DomainScoped@15(k_independent)", "Domain-Scoped")]:
        t = test(f"EX[{p}]:Graph@5vs{b}")
        holm = {r["test"]: r["p_holm"] for r in O2["tests"][p]}[f"EX[{p}]: Graph@5 vs {b.replace('(', ' (')}"]
        rows.append(f"{pn} & {lbl_} & {pct(t['ex_a'] * 100)} / {pct(t['ex_b'] * 100)} & {intf(t['only_a'])} & {intf(t['only_b'])} & {pval(t['p_value'])} & {pval(holm)} \\\\")
    if p == "small":
        rows.append("\\midrule")
write_table("tab_mcnemar", "statistical_hypothesis_tests.csv (EX[...]: dokładny test McNemara); p_Holm: derived/o2_holm_mcnemar.json (analiza wtórna)", rows)

# =========================================================================== makra
lines = ["% Wygenerowano: benchmarks/analysis_final/generuj_makra.py -- NIE EDYTOWAĆ RĘCZNIE.",
         f"% Runda: {v('run.id')}, commit {v('run.commit')}, tryb {v('run.mode')}. Źródła: results-remote-new-sunday/.", "",
         "% Wstawianie wierszy tabel z chapters/tabele/ (pierwotne \\input TeX-a działa wewnątrz środowiska tabular)",
         "\\makeatletter\\newcommand{\\tabelawynikow}[1]{\\@@input chapters/tabele/#1.tex }\\makeatother", ""]
# Emitowane są tylko makra użyte w tekście pracy (pozostałe wartości trafiają do tabel w chapters/tabele/);
# kompletność sprawdza sprawdz_makra.py.
import os  # noqa: E402
import re  # noqa: E402
_tex = [p for p in DOC.glob("*.tex") if p.name not in ("wyniki_liczby.tex", "wyniki_wtorne.tex")] + [Path(os.path.realpath(ROOT / "doc" / "main.tex"))]
USED = set(re.findall(r"\\(wyn[A-Za-z]+)", "\n".join(p.read_text(encoding="utf-8") for p in _tex)))
seen = set()
skipped = 0
for name, val, s in MACROS:
    if name not in USED:
        skipped += 1
        continue
    assert name not in seen, name
    seen.add(name)
    lines.append(f"% {s}")
    lines.append(f"\\newcommand{{\\{name}}}{{{val}}}")
(DOC / "wyniki_liczby.tex").write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"Makra: {len(MACROS) - skipped} (pominięte nieużyte: {skipped}); tabele: {len(list(TAB.glob('*.tex')))}")
