#!/usr/bin/env python3
"""
Scenariusz ttft_scaling: czas do pierwszego fragmentu odpowiedzi (TTFT) i czas całkowity w funkcji
długości DDL w prompcie, dla profili modeli z konfiguracji.

- DDL budowany jest z tabel losowanych ziarnem seeds.ttft_table_sampling (ten sam format
  synthesize_ddl co w Scenariuszu A), dodawanych aż liczba tokenów DDL (cl100k_base) osiągnie cel
  (np. 1k, 2k, 4k, 8k, 16k, 28k). Pytanie jest stałe.
- Wywołania ze `stream: true`; TTFT = czas od wysłania żądania do pierwszej linii strumienia.
  Zapisywane są także liczniki serwera Ollama (prompt_eval_duration itd.).
- num_ctx dobierany do długości promptu z zapasem (ctx_margin_tokens), nie większy niż okno modelu.
- Cache prefiksów: każde wywołanie zaczyna się unikalnym znacznikiem (nonce), więc wspólny prefiks
  z poprzednim wywołaniem ma długość ~0 tokenów; pole prefix_cache opisuje tę metodę.
- Przed seriami danego (profil, num_ctx) wykonywane jest jedno wywołanie rozgrzewkowe (warmup = True),
  które wczytuje model z danym num_ctx; nie wchodzi do statystyk.

Pliki: ttft_scaling.parquet/.csv, ttft_scaling_summary.csv.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import math
import random
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List

import httpx
import numpy as np
import pandas as pd
import tiktoken

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.common import ollama_client  # noqa: E402
from benchmarks.common.config import add_common_args, db_params, load_config  # noqa: E402
from benchmarks.common.db import create_pool  # noqa: E402
from benchmarks.common.ddl_format import SchemaCatalog, build_prompt, synthesize_ddl  # noqa: E402
from benchmarks.common.provenance import record_scenario_note  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", stream=sys.stdout)
logger = logging.getLogger("bench_ttft_scaling")
_ENC = tiktoken.get_encoding("cl100k_base")
PREFIX_CACHE_METHOD = "defeated_by_unique_leading_nonce"


def build_ddl_for_target(catalog: SchemaCatalog, order: List[str], target: int) -> Dict[str, Any]:
    tables: List[str] = []
    ddl = ""
    for t in order:
        tables.append(t)
        ddl = synthesize_ddl(catalog, tables)
        if len(_ENC.encode(ddl)) >= target:
            break
    return {"tables": tables, "ddl": ddl, "ddl_tokens_cl100k": len(_ENC.encode(ddl))}


async def model_context_length(base_url: str, model: str) -> int:
    async with httpx.AsyncClient(timeout=30) as c:
        info = (await c.post(f"{base_url}/api/show", json={"model": model})).json().get("model_info", {})
    vals = [int(v) for k, v in info.items() if k.endswith("context_length")]
    return max(vals) if vals else 32768


async def run(args: argparse.Namespace) -> None:
    cfg = load_config(args.config, args.mode)
    tt = cfg["scenarios"]["ttft_scaling"]
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    if not tt.get("enabled", True):
        logger.info("ttft_scaling wyłączony w konfiguracji.")
        return
    llm = cfg["llm"]
    models = {p: llm["profiles"][p]["model"] for p in tt["profiles"]}
    digests = await ollama_client.require_models(llm["base_url"], sorted(set(models.values())))
    record_scenario_note(out, "ttft_scaling", "model_digests", digests)
    record_scenario_note(out, "ttft_scaling", "prefix_cache", PREFIX_CACHE_METHOD)

    pool = await create_pool(db_params(cfg))
    try:
        catalog = await SchemaCatalog.load(pool)
    finally:
        await pool.close()
    order = sorted(catalog.columns)
    order = [t for t in order if t in catalog.descriptions]
    random.Random(cfg["seeds"]["ttft_table_sampling"]).shuffle(order)
    points = {t: build_ddl_for_target(catalog, order, t) for t in tt["ddl_token_targets"]}

    rows: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=llm["request_timeout_s"]) as client:
        for profile in tt["profiles"]:
            model = models[profile]
            max_ctx = await model_context_length(llm["base_url"], model)
            for target, pt in points.items():
                need = int(math.ceil((pt["ddl_tokens_cl100k"] * 1.15 + tt["num_predict"] + tt["ctx_margin_tokens"]) / 1024.0) * 1024)
                num_ctx = min(need, max_ctx)
                options = ollama_client.build_options(cfg, profile, num_ctx=num_ctx, num_predict=tt["num_predict"])
                for rep in range(-1, tt["repetitions"]):  # rep = -1: rozgrzewka
                    nonce = uuid.uuid4().hex
                    prompt = f"-- request nonce: {nonce}\n" + build_prompt(pt["ddl"], tt["question"])
                    r = await ollama_client.generate(client, llm["base_url"], model, prompt, options, stream=True,
                                                     truncation_ratio=llm["possible_truncation_ratio"])
                    rows.append({
                        "profile": profile, "model": model, "model_digest": digests[model], "target_ddl_tokens": target,
                        "ddl_tokens_cl100k": pt["ddl_tokens_cl100k"], "n_tables": len(pt["tables"]), "repetition": rep,
                        "warmup": rep < 0, "num_ctx": num_ctx, "model_max_context": max_ctx, "num_ctx_capped": need > max_ctx,
                        "prefix_cache": PREFIX_CACHE_METHOD, "ttft_ms": r["client_ttft_ms"], "total_ms": r["client_wall_ms"],
                        "stream_chunks": r["stream_chunks"], "possible_truncation": r["possible_truncation"],
                        "done_reason": r["done_reason"], **{k: r.get(k) for k in ollama_client.COUNTERS},
                    })
                logger.info("[%s] DDL %d tok (cel %d), num_ctx=%d: gotowe", profile, pt["ddl_tokens_cl100k"], target, num_ctx)
    df = pd.DataFrame(rows)
    df.to_parquet(out / "ttft_scaling.parquet", index=False)
    df.to_csv(out / "ttft_scaling.csv", index=False)
    m = df[~df["warmup"]]
    m.groupby(["profile", "model", "target_ddl_tokens"]).agg(
        n=("ttft_ms", "count"), ddl_tokens_cl100k=("ddl_tokens_cl100k", "first"), prompt_eval_count=("prompt_eval_count", "median"),
        num_ctx=("num_ctx", "first"), ttft_median_ms=("ttft_ms", "median"), ttft_p95_ms=("ttft_ms", lambda x: np.percentile(x, 95)),
        total_median_ms=("total_ms", "median"), prompt_eval_median_ms=("prompt_eval_duration", lambda x: np.median(x) / 1e6),
        possible_truncation_n=("possible_truncation", "sum"),
    ).reset_index().to_csv(out / "ttft_scaling_summary.csv", index=False)


def main() -> None:
    args = add_common_args(argparse.ArgumentParser(description=__doc__)).parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
