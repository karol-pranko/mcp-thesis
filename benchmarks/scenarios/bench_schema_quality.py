#!/usr/bin/env python3
"""
Scenariusz A: selekcja schematu i Text-to-SQL.

Faza 1 (bez LLM): dla każdego zapytania i k ∈ scenarios.schema_quality.k_values wyznaczane są zbiory
tabel strategii Domain-Scoped, BM25, Dense, RRF i Graph oraz Table/Column-Recall i Precision.
Wszystkie strategie dostają DDL z tej samej funkcji `synthesize_ddl` (różni się tylko zbiór tabel);
zapisywana jest liczba tokenów samego DDL i całego promptu.

Faza 2 (LLM): SQL generowany jest wyłącznie dla k = generation_k (5) oraz dla wariantów przy
porównywalnej liczbie tabel (equal_budget: BM25 i RRF przy k = 8 lub k = |T_final| per zapytanie),
dla każdego profilu modelu z konfiguracji. Wyniki dla innych k NIE są kopiowane.

Pliki wynikowe (w katalogu wyników):
  schema_quality_dataset.jsonl          pytania, SQL wzorcowy, tabele GT
  schema_retrieval.parquet/.csv         metryki selekcji per (zapytanie, strategia, k)
  schema_quality_generation.parquet/.csv  rekordy generacji per (profil, zapytanie, strategia, k)
  schema_quality_summary.csv            agregaty generacji
  schema_retrieval_summary.csv          agregaty selekcji
  schema_error_decomposition.csv        taksonomia błędów (k = 5 i equal_budget), suma = liczba EX = False
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
import numpy as np
import pandas as pd
import tiktoken

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.common import ollama_client  # noqa: E402
from benchmarks.common.config import add_common_args, db_params, load_config  # noqa: E402
from benchmarks.common.db import create_pool, discard_all  # noqa: E402
from benchmarks.common.ddl_format import SchemaCatalog, build_prompt, synthesize_ddl  # noqa: E402
from benchmarks.common.metrics_text2sql import (  # noqa: E402
    assert_taxonomy_consistent, classify_error, evaluate_sql, extract_clean_sql,
)
from benchmarks.common.provenance import record_scenario_note  # noqa: E402
from benchmarks.scenarios.text2sql_dataset import SyntheticQueryBenchmarkGenerator, TestQueryCase, dataset_to_records  # noqa: E402
from src.mcp_server.schema_engine import SchemaEngine  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", stream=sys.stdout)
logger = logging.getLogger("bench_schema_quality")

STRATEGIES = ["DomainScoped", "BM25", "Dense", "RRF", "Graph"]
GRAPH_M_EXP = 3
_ENC = tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str) -> int:
    return len(_ENC.encode(text or ""))


def column_metrics(retrieved: Set[str], required_columns: List[str], table_columns: Dict[str, List[str]]) -> Tuple[float, float, float]:
    """Column-Precision, Column-Recall, Column-F1: kolumny dostarczone = wszystkie kolumny wybranych tabel."""
    if not required_columns:
        return 1.0, 1.0, 1.0
    req = {c.lower() for c in required_columns}
    delivered = {f"{t.lower()}.{c.lower()}" for t in retrieved for c in table_columns.get(t, [])}
    if not delivered:
        return 0.0, 0.0, 0.0
    inter = req & delivered
    rec = len(inter) / len(req)
    prec = len(inter) / len(delivered)
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    return prec, rec, f1


class Retriever:
    def __init__(self, engine: SchemaEngine, pool: Any, n_domain_tables: int) -> None:
        self.engine = engine
        self.pool = pool
        self.n_domain_tables = n_domain_tables
        self._domain_cache: Dict[str, List[str]] = {}

    async def tables(self, strategy: str, case: TestQueryCase, k: int) -> Tuple[List[str], Dict[str, Any]]:
        q = case.natural_language_query
        extra: Dict[str, Any] = {}
        if strategy == "DomainScoped":
            if case.primary_domain not in self._domain_cache:
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT table_name FROM public.schema_embeddings WHERE domain = $1 ORDER BY table_name LIMIT $2",
                        case.primary_domain, self.n_domain_tables)
                self._domain_cache[case.primary_domain] = [r["table_name"] for r in rows]
            return list(self._domain_cache[case.primary_domain]), extra
        if strategy == "BM25":
            res = await self.engine.retrieve_bm25(query=q, top_k=k)
        elif strategy == "Dense":
            res = await self.engine.retrieve_dense(query=q, top_k=k)
        elif strategy == "RRF":
            res = await self.engine.retrieve_hybrid_rrf(query=q, top_k=k)
        elif strategy == "Graph":
            ddl = await self.engine.discover_schema_with_graph_pruning(query=q, top_k=k, max_expanded_tables=GRAPH_M_EXP)
            tabs = re.findall(r"CREATE TABLE public\.(\w+)", ddl)
            bridges = re.findall(r"\[TABELA MOSTKUJĄCA N:M \(BRIDGE\)\] public\.(\w+)", ddl)
            extra = {"bridges": bridges, **{f"graph_{k_}": v for k_, v in self.engine.last_graph_timings.items()}}
            return tabs, extra
        else:
            raise ValueError(strategy)
        seen: List[str] = []
        for r in res:
            if r["table_name"] not in seen:
                seen.append(r["table_name"])
        return seen, extra


async def run(args: argparse.Namespace) -> None:
    cfg = load_config(args.config, args.mode)
    sc = cfg["scenarios"]["schema_quality"]
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    db = db_params(cfg)
    pool = await create_pool(db, max_size=max(4, sc["llm_concurrency"] + 2))
    llm = cfg["llm"]
    profiles = sc["profiles"]
    models = {p: llm["profiles"][p]["model"] for p in profiles}
    digests = await ollama_client.require_models(llm["base_url"], sorted(set(models.values())))
    record_scenario_note(out, "schema_quality", "model_digests", digests)
    record_scenario_note(out, "schema_quality", "models", models)
    record_scenario_note(out, "schema_quality", "ex_comparison", sc["ex_comparison"])
    record_scenario_note(out, "schema_quality", "equal_budget", sc["equal_budget"])
    record_scenario_note(out, "schema_quality", "graph_m_exp", GRAPH_M_EXP)

    try:
        gen = SyntheticQueryBenchmarkGenerator(pool, seed=cfg["seeds"]["query_generator"])
        await gen.initialize()
        dataset = gen.generate_1000_query_dataset(total_queries=sc["n_queries"])
        with open(out / "schema_quality_dataset.jsonl", "w", encoding="utf-8") as f:
            for r in dataset_to_records(dataset):
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        catalog = await SchemaCatalog.load(pool)
        table_cols = catalog.table_columns_flat()
        engine = SchemaEngine(pool=pool)
        engine.set_pool(pool)
        retriever = Retriever(engine, pool, sc["domain_scoped_tables"])

        # Objętość pełnego zrzutu DDL wszystkich tabel schematu w tym samym formacie (punkt odniesienia, bez LLM)
        all_tables = sorted(catalog.descriptions)
        full_ddl = synthesize_ddl(catalog, all_tables)
        full_dump = {"n_tables": len(all_tables), "ddl_tokens_cl100k": count_tokens(full_ddl),
                     "ddl_chars": len(full_ddl), "format": "synthesize_ddl (jednolity format Scenariusza A)"}
        (out / "schema_full_dump_tokens.json").write_text(json.dumps(full_dump, ensure_ascii=False, indent=2), encoding="utf-8")
        record_scenario_note(out, "schema_quality", "full_schema_dump", full_dump)

        # Wyniki GT (raz na zapytanie)
        gt_rows: Dict[int, List[Dict[str, Any]]] = {}
        async with pool.acquire() as conn:
            await conn.execute(f"SET statement_timeout = {int(sc['statement_timeout_ms'])}")
            for case in dataset:
                gt_rows[case.query_id] = [dict(r) for r in await conn.fetch(case.ground_truth_sql)]
        n_gt_empty = sum(1 for v in gt_rows.values() if not v)
        record_scenario_note(out, "schema_quality", "gt_empty_queries", n_gt_empty)
        if n_gt_empty:
            logger.error("%d zapytań GT zwraca pusty wynik - dane w tabelach są niewystarczające.", n_gt_empty)

        # ---------------- Faza 1: selekcja (bez LLM) ----------------
        await discard_all(pool, "schema_quality/retrieval")
        retrieval: List[Dict[str, Any]] = []
        prompts: Dict[Tuple[int, str, int], Tuple[str, List[str], int, int]] = {}
        gen_k = sc["generation_k"]
        eb = sc["equal_budget"]
        for i, case in enumerate(dataset, 1):
            gt = set(case.ground_truth_tables)
            plan: List[Tuple[str, int, str]] = [(s, k, "k_sweep") for k in sc["k_values"] for s in STRATEGIES if s != "DomainScoped"]
            plan.append(("DomainScoped", sc["domain_scoped_tables"], "k_independent"))
            if gen_k not in sc["k_values"]:
                plan += [(s, gen_k, "k_sweep") for s in STRATEGIES if s != "DomainScoped"]
            t_final_graph: Optional[int] = None
            for strategy, k, variant in plan:
                tabs, extra = await retriever.tables(strategy, case, k)
                if strategy == "Graph" and k == gen_k:
                    t_final_graph = len(tabs)
                retrieval.append(_retrieval_record(case, strategy, k, variant, tabs, extra, gt, catalog, table_cols, prompts))
            if eb["mode"] == "fixed":
                kb_list = [int(eb["k"])]
            elif eb["mode"] == "tfinal":
                kb_list = [int(t_final_graph or gen_k)]
            else:
                raise ValueError(f"equal_budget.mode: {eb['mode']}")
            for strategy in eb["strategies"]:
                for kb in kb_list:
                    tabs, extra = await retriever.tables(strategy, case, kb)
                    retrieval.append(_retrieval_record(case, strategy, kb, "equal_budget", tabs, extra, gt, catalog, table_cols, prompts))
            if i % 50 == 0 or i == len(dataset):
                logger.info("Selekcja: %d / %d zapytań", i, len(dataset))
        df_r = pd.DataFrame(retrieval)
        _save(df_r, out / "schema_retrieval")
        df_r.groupby(["strategy", "k", "budget_variant"]).agg(
            n=("query_id", "count"), table_recall=("table_recall", "mean"), table_precision=("table_precision", "mean"),
            column_recall=("column_recall", "mean"), column_f1=("column_f1", "mean"), n_tables=("n_tables", "mean"),
            ddl_tokens=("ddl_tokens", "mean"), prompt_tokens=("prompt_tokens", "mean"),
        ).reset_index().to_csv(out / "schema_retrieval_summary.csv", index=False)

        # ---------------- Faza 2: generacja (LLM) ----------------
        gen_strats = sc.get("generation_strategies", STRATEGIES)
        gen_plan = [(s, gen_k, "k_sweep") for s in STRATEGIES if s != "DomainScoped" and s in gen_strats]
        if "DomainScoped" in gen_strats:
            gen_plan.append(("DomainScoped", sc["domain_scoped_tables"], "k_independent"))
        records: List[Dict[str, Any]] = []
        fk_pairs = {(a, c) for a, _, c, _, _ in catalog.foreign_keys} | {(c, a) for a, _, c, _, _ in catalog.foreign_keys}
        sem = asyncio.Semaphore(sc["llm_concurrency"])
        async with httpx.AsyncClient(timeout=llm["request_timeout_s"]) as client:
            for profile in profiles:
                await discard_all(pool, f"schema_quality/generation/{profile}")
                options = ollama_client.build_options(cfg, profile)
                model = models[profile]
                tasks = []
                for case in dataset:
                    items = list(gen_plan)
                    items += [(r["strategy"], r["k"], "equal_budget") for r in retrieval
                              if r["query_id"] == case.query_id and r["budget_variant"] == "equal_budget"]
                    for strategy, k, variant in items:
                        tasks.append(_generate_one(client, pool, sem, cfg, sc, profile, model, digests[model], options, case,
                                                   strategy, k, variant, prompts, gt_rows, fk_pairs))
                t0 = time.time()
                for j, coro in enumerate(asyncio.as_completed(tasks), 1):
                    records.append(await coro)
                    if j % 100 == 0 or j == len(tasks):
                        logger.info("Generacja [%s]: %d / %d (%.0f s)", profile, j, len(tasks), time.time() - t0)
        df_g = pd.DataFrame(records)
        cats = assert_taxonomy_consistent(df_g.to_dict("records"))
        logger.info("Taksonomia spójna: %s", cats)
        _save(df_g, out / "schema_quality_generation")
        _summaries(df_g, out)
        trunc = float(df_g["possible_truncation"].mean()) if len(df_g) else 0.0
        record_scenario_note(out, "schema_quality", "possible_truncation_share", trunc)
    finally:
        await pool.close()


def _retrieval_record(case: TestQueryCase, strategy: str, k: int, variant: str, tabs: List[str], extra: Dict[str, Any],
                      gt: Set[str], catalog: SchemaCatalog, table_cols: Dict[str, List[str]],
                      prompts: Dict[Tuple[int, str, int], Tuple[str, List[str], int, int]]) -> Dict[str, Any]:
    tset = set(tabs)
    inter = tset & gt
    cp, cr, cf1 = column_metrics(tset, case.required_columns, table_cols)
    ddl = synthesize_ddl(catalog, tabs)
    prompt = build_prompt(ddl, case.natural_language_query)
    ddl_tok, prompt_tok = count_tokens(ddl), count_tokens(prompt)
    prompts[(case.query_id, strategy, k)] = (prompt, tabs, ddl_tok, prompt_tok)
    rec = {
        "query_id": case.query_id, "difficulty": case.difficulty, "primary_domain": case.primary_domain,
        "strategy": strategy, "k": k, "budget_variant": variant,
        "retrieved_tables": json.dumps(tabs), "gt_tables": json.dumps(case.ground_truth_tables),
        "n_tables": len(tset), "n_gt_tables": len(gt),
        "table_recall": len(inter) / len(gt) if gt else 0.0,
        "table_precision": len(inter) / len(tset) if tset else 0.0,
        "column_precision": cp, "column_recall": cr, "column_f1": cf1,
        "schema_complete": gt.issubset(tset), "ddl_tokens": ddl_tok, "prompt_tokens": prompt_tok,
    }
    if extra:
        rec["bridges"] = json.dumps(extra.get("bridges", []))
        rec.update({k_: v for k_, v in extra.items() if k_.startswith("graph_")})
    return rec


async def _generate_one(client: httpx.AsyncClient, pool: Any, sem: asyncio.Semaphore, cfg: Dict[str, Any], sc: Dict[str, Any],
                        profile: str, model: str, digest: str, options: Dict[str, Any], case: TestQueryCase, strategy: str,
                        k: int, variant: str, prompts: Dict[Tuple[int, str, int], Tuple[str, List[str], int, int]],
                        gt_rows: Dict[int, List[Dict[str, Any]]], fk_pairs: Set[Tuple[str, str]]) -> Dict[str, Any]:
    prompt, tabs, ddl_tok, prompt_tok = prompts[(case.query_id, strategy, k)]
    async with sem:
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            try:
                resp = await ollama_client.generate(client, cfg["llm"]["base_url"], model, prompt, options,
                                                    truncation_ratio=cfg["llm"]["possible_truncation_ratio"])
                break
            except Exception as exc:  # błąd transportu: ponowienie, potem przerwanie scenariusza
                last_exc = exc
                await asyncio.sleep(2 * (attempt + 1))
        else:
            raise RuntimeError(f"LLM niedostępny dla zapytania {case.query_id}/{strategy}: {last_exc}")
    sql = extract_clean_sql(resp["response_text"])
    async with pool.acquire() as conn:
        await conn.execute(f"SET statement_timeout = {int(sc['statement_timeout_ms'])}")
        ev = await evaluate_sql(conn, sql, case.ground_truth_sql, gt_rows.get(case.query_id), sc["ex_comparison"])
    cat = classify_error(sql, ev["valid_sql"], ev["ex_match"], ev["gt_empty"], ev["exec_error"], resp.get("done_reason"),
                         resp.get("eval_count"), options["num_predict"], set(tabs), fk_pairs)
    rec = {
        "profile": profile, "model": model, "model_digest": digest, "query_id": case.query_id,
        "difficulty": case.difficulty, "primary_domain": case.primary_domain, "strategy": strategy, "k": k,
        "budget_variant": variant, "question": case.natural_language_query, "gt_sql": case.ground_truth_sql,
        "gt_tables": json.dumps(case.ground_truth_tables), "retrieved_tables": json.dumps(tabs),
        "n_tables": len(set(tabs)), "ddl_tokens": ddl_tok, "prompt_tokens_cl100k": prompt_tok,
        "raw_response": resp["response_text"], "generated_sql": sql, "error_category": cat,
    }
    rec.update(ev)
    rec.update({key: resp.get(key) for key in ollama_client.COUNTERS + ["done_reason", "client_wall_ms", "possible_truncation",
                                                                         "num_ctx", "num_predict", "seed", "temperature"]})
    return rec


def _summaries(df: pd.DataFrame, out: Path) -> None:
    keys = ["profile", "model", "strategy", "k", "budget_variant"]
    df.groupby(keys).agg(
        n=("query_id", "count"), valid_sql_pct=("valid_sql", lambda x: 100 * x.mean()),
        execution_accuracy_pct=("ex_match", lambda x: 100 * x.mean()),
        execution_accuracy_strict_pct=("ex_match_strict", lambda x: 100 * x.mean()),
        avg_prompt_eval_count=("prompt_eval_count", "mean"), avg_eval_count=("eval_count", "mean"),
        avg_prompt_tokens_cl100k=("prompt_tokens_cl100k", "mean"), avg_ddl_tokens=("ddl_tokens", "mean"),
        median_total_duration_ms=("total_duration", lambda x: float(np.median(x)) / 1e6),
        possible_truncation_n=("possible_truncation", "sum"),
    ).reset_index().to_csv(out / "schema_quality_summary.csv", index=False)
    fail = df[~df["ex_match"]]
    dec = fail.groupby(keys + ["error_category"]).size().reset_index(name="count")
    dec["ex_false_total"] = dec.groupby(keys)["count"].transform("sum")
    dec["percentage"] = 100 * dec["count"] / dec["ex_false_total"]
    n_false = fail.groupby(keys).size()
    assert (dec.groupby(keys)["count"].sum() == n_false.reindex(dec.groupby(keys)["count"].sum().index)).all()
    dec.to_csv(out / "schema_error_decomposition.csv", index=False)


def _save(df: pd.DataFrame, stem: Path) -> None:
    df.to_parquet(stem.with_suffix(".parquet"), index=False)
    df.to_csv(stem.with_suffix(".csv"), index=False)


def main() -> None:
    ap = add_common_args(argparse.ArgumentParser(description=__doc__))
    args = ap.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
