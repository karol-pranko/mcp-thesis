#!/usr/bin/env python3
"""
Ablacja M_exp (liczba dołączanych tabel mostkujących) w discover_schema_with_graph_pruning, bez LLM.

Dla każdego zapytania i M_exp zapisywane są: Table-Recall@k, zbiór tabel, liczba tokenów DDL
(format bramy oraz jednolity format synthesize_ddl używany w Scenariuszu A) oraz składowe czasu
wywołania z instrumentacji SchemaEngine.last_graph_timings (RRF, pobranie grafu, pętla mostków,
synteza DDL) mierzone perf_counter_ns.

Pliki: ablation_m_exp_calls.parquet/.csv (per wywołanie), ablation_m_exp_results.csv (agregaty),
ablation_m_exp_sweet_spot.json (wynik reguły z benchmarks/common/rules.py).
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

import numpy as np
import pandas as pd
import tiktoken

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.common.config import add_common_args, db_params, load_config  # noqa: E402
from benchmarks.common.db import create_pool, discard_all  # noqa: E402
from benchmarks.common.ddl_format import SchemaCatalog, synthesize_ddl  # noqa: E402
from benchmarks.common.provenance import record_scenario_note  # noqa: E402
from benchmarks.common.rules import SWEET_SPOT_RULE, sweet_spot  # noqa: E402
from benchmarks.scenarios.text2sql_dataset import SyntheticQueryBenchmarkGenerator  # noqa: E402
from src.mcp_server.schema_engine import SchemaEngine  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", stream=sys.stdout)
logger = logging.getLogger("bench_ablation_mexp")
_ENC = tiktoken.get_encoding("cl100k_base")


async def run(args: argparse.Namespace) -> None:
    cfg = load_config(args.config, args.mode)
    ab = cfg["scenarios"]["ablation"]
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    pool = await create_pool(db_params(cfg))
    try:
        gen = SyntheticQueryBenchmarkGenerator(pool, seed=cfg["seeds"]["query_generator"])
        await gen.initialize()
        dataset = gen.generate_1000_query_dataset(total_queries=ab["n_queries"])
        catalog = await SchemaCatalog.load(pool)
        engine = SchemaEngine(pool=pool)
        engine.set_pool(pool)
        # Rozgrzewka indeksów (BM25, graf, model osadzeń) poza pomiarem
        await engine.discover_schema_with_graph_pruning(query=dataset[0].natural_language_query, top_k=ab["top_k"], max_expanded_tables=0)
        rows = []
        for m in ab["m_exp_values"]:
            await discard_all(pool, f"ablation/m_exp={m}")
            for case in dataset:
                gt = set(case.ground_truth_tables)
                t0 = time.perf_counter_ns()
                ddl = await engine.discover_schema_with_graph_pruning(query=case.natural_language_query, top_k=ab["top_k"], max_expanded_tables=m)
                wall_ns = time.perf_counter_ns() - t0
                tabs = re.findall(r"CREATE TABLE public\.(\w+)", ddl)
                tm = dict(engine.last_graph_timings)
                rows.append({
                    "m_exp": m, "query_id": case.query_id, "top_k": ab["top_k"], "tables": json.dumps(tabs),
                    "n_tables": len(set(tabs)), "table_recall": len(set(tabs) & gt) / len(gt) if gt else 0.0,
                    "ddl_tokens_gateway_format": len(_ENC.encode(ddl)),
                    "ddl_tokens_uniform_format": len(_ENC.encode(synthesize_ddl(catalog, tabs))),
                    "wall_ns": wall_ns, **{f"t_{k}": v for k, v in tm.items()},
                })
            logger.info("M_exp=%d gotowe (%d zapytań)", m, len(dataset))
        df = pd.DataFrame(rows)
        df.to_parquet(out / "ablation_m_exp_calls.parquet", index=False)
        df.to_csv(out / "ablation_m_exp_calls.csv", index=False)
        agg = df.groupby("m_exp").agg(
            num_queries=("query_id", "count"), table_recall_at_5=("table_recall", "mean"),
            avg_ddl_tokens_gateway_format=("ddl_tokens_gateway_format", "mean"),
            avg_ddl_tokens_uniform_format=("ddl_tokens_uniform_format", "mean"),
            median_total_ms=("t_total_ns", lambda x: np.median(x) / 1e6), p95_total_ms=("t_total_ns", lambda x: np.percentile(x, 95) / 1e6),
            median_rrf_ms=("t_rrf_ns", lambda x: np.median(x) / 1e6), median_graph_load_ms=("t_graph_load_ns", lambda x: np.median(x) / 1e6),
            median_bridge_search_ms=("t_bridge_search_ns", lambda x: np.median(x) / 1e6),
            p95_bridge_search_ms=("t_bridge_search_ns", lambda x: np.percentile(x, 95) / 1e6),
            median_ddl_synthesis_ms=("t_ddl_synthesis_ns", lambda x: np.median(x) / 1e6),
        ).reset_index()
        agg.to_csv(out / "ablation_m_exp_results.csv", index=False)
        ss = sweet_spot(agg["m_exp"].tolist(), (agg["table_recall_at_5"] * 100).tolist(), ab["sweet_spot_threshold_pp"])
        (out / "ablation_m_exp_sweet_spot.json").write_text(json.dumps(ss, ensure_ascii=False, indent=2), encoding="utf-8")
        record_scenario_note(out, "ablation", "sweet_spot", ss)
        record_scenario_note(out, "ablation", "sweet_spot_rule", SWEET_SPOT_RULE)
        logger.info("Sweet spot: %s", ss)
    finally:
        await pool.close()


def main() -> None:
    args = add_common_args(argparse.ArgumentParser(description=__doc__)).parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
