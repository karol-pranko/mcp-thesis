#!/usr/bin/env python3
"""
Scenariusz C1: współbieżność walidacji AST a opóźnienie pętli zdarzeń asyncio.

Model klientów (pole client_model): M niezależnych korutyn w JEDNYM procesie (zadania asyncio),
w modelu zamkniętym - każda wysyła żądanie walidacji, czeka na wynik i wysyła kolejne. Po każdej
odpowiedzi klient oddaje sterowanie pętli (await asyncio.sleep(0)), jak klient sieciowy.
Każdy poziom M trwa stały czas (level_duration_s) poprzedzony rozgrzewką (level_warmup_s).

Warianty wykonania tej samej funkcji bramy `_worker_validate_query` (sanitizer bez zmian):
  SYNC   - wywołanie bezpośrednio w pętli zdarzeń,
  THREAD - ThreadPoolExecutor(workers) przez loop.run_in_executor,
  POOL   - ProcessPoolExecutor(workers, kontekst forkserver).

Sonda pętli zdarzeń (interwał lag_probe_interval_ms) działa przez cały poziom; zapisywana jest każda
próbka (znacznik czasu, lag). Pamięć: suma PSS (fallback USS, RSS) procesu głównego i wszystkich
procesów potomnych (rekurencyjnie) oraz liczba procesów potomnych, próbkowane co 0,5 s.

H3: test Manna-Whitneya na próbkach lagu POOL vs SYNC dla poziomów h3_levels oraz bootstrapowy CI
różnicy median (ziarno seeds.bootstrap) - plik concurrency_h3_tests.json.

Pliki: concurrency_summary.csv, concurrency_lag_samples.parquet, concurrency_request_latencies.parquet,
concurrency_memory_samples.csv, concurrency_h3_tests.json.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import multiprocessing as mp
import os
import random
import sys
import time
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import psutil

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.common.config import add_common_args, load_config  # noqa: E402
from benchmarks.common.provenance import record_scenario_note  # noqa: E402
from benchmarks.common.stats import bootstrap_median_diff_ci, mann_whitney  # noqa: E402
from src.mcp_server.security import _worker_validate_query  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", stream=sys.stdout)
logger = logging.getLogger("bench_concurrency")

CLIENT_MODEL = "closed_loop_asyncio_tasks_single_process"

BENCHMARK_QUERIES: List[str] = [
    "SELECT id, amount, currency FROM orders WHERE status = 'COMPLETED' AND amount > 100.0;",
    """WITH regional_summary AS (SELECT region, dept, sum(amount) AS total_sales FROM sales_records
       GROUP BY region, dept HAVING sum(amount) > 10000)
       SELECT r.*, rank() OVER (PARTITION BY region ORDER BY total_sales DESC) AS rank FROM regional_summary r;""",
    """SELECT e.id, e.name, d.department_name, AVG(salary) OVER (PARTITION BY department_id) AS dept_avg,
       COUNT(*) OVER (PARTITION BY department_id) AS dept_count FROM employees e
       JOIN departments d ON e.department_id = d.id WHERE e.status = 'ACTIVE';""",
    "SELECT product_id, sum(quantity) AS sold_qty FROM warehouse_transactions GROUP BY CUBE (product_id) HAVING sum(quantity) >= 50;",
    """SELECT client_id FROM enterprise_clients UNION ALL SELECT client_id FROM partner_accounts
       INTERSECT SELECT client_id FROM active_subscriptions;""",
]


def tree_memory() -> Tuple[float, int, str]:
    """Suma PSS [MB] procesu głównego i wszystkich procesów potomnych (rekurencyjnie) oraz liczba potomków."""
    me = psutil.Process(os.getpid())
    procs = [me] + me.children(recursive=True)
    total, metric = 0.0, "pss"
    for p in procs:
        try:
            mi = p.memory_full_info()
            val = getattr(mi, "pss", None) or getattr(mi, "uss", None)
            if val is None:
                val, metric = mi.rss, "rss"
            total += val
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            try:
                total += p.memory_info().rss
                metric = "mixed_rss_fallback"
            except psutil.NoSuchProcess:
                pass
    return total / (1024 * 1024), len(procs) - 1, metric


async def run_level(variant: str, workers: Optional[int], m: int, sc: Dict[str, Any], seed: int,
                    executor: Optional[Executor]) -> Tuple[Dict[str, Any], List[Tuple[int, float, str]], List[float], List[Dict[str, Any]]]:
    loop = asyncio.get_running_loop()
    stop_at = [0.0]
    phase = ["warmup"]
    latencies: List[float] = []
    ok_count = [0]
    lag_samples: List[Tuple[int, float, str]] = []
    mem_samples: List[Dict[str, Any]] = []
    interval = sc["lag_probe_interval_ms"] / 1000.0

    async def probe() -> None:
        while True:
            t_exp = time.perf_counter() + interval
            await asyncio.sleep(interval)
            lag = max(0.0, time.perf_counter() - t_exp) * 1000.0
            lag_samples.append((time.time_ns(), lag, phase[0]))

    async def memory_sampler() -> None:
        while True:
            mb, n_children, metric = tree_memory()
            mem_samples.append({"ts_ns": time.time_ns(), "tree_mem_mb": mb, "n_children": n_children, "metric": metric, "phase": phase[0]})
            await asyncio.sleep(0.5)

    async def client(cid: int) -> None:
        rnd = random.Random(seed * 100003 + cid)
        while time.perf_counter() < stop_at[0]:
            q = BENCHMARK_QUERIES[rnd.randrange(len(BENCHMARK_QUERIES))]
            t0 = time.perf_counter()
            if variant == "SYNC":
                res = _worker_validate_query(q, "postgres", 1000)
            else:
                res = await loop.run_in_executor(executor, _worker_validate_query, q, "postgres", 1000)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            if phase[0] == "measure":
                latencies.append(dt_ms)
                ok_count[0] += int(bool(res.is_valid))
            await asyncio.sleep(0)

    t_start = time.perf_counter()
    stop_at[0] = t_start + sc["level_warmup_s"] + sc["level_duration_s"]
    tasks = [asyncio.create_task(probe()), asyncio.create_task(memory_sampler())]
    clients = [asyncio.create_task(client(i)) for i in range(m)]
    await asyncio.sleep(sc["level_warmup_s"])
    phase[0] = "measure"
    t_meas = time.perf_counter()
    await asyncio.gather(*clients)
    t_end = time.perf_counter()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    lags = np.array([l for _, l, ph in lag_samples if ph == "measure"]) if lag_samples else np.array([])
    lat = np.array(latencies) if latencies else np.array([np.nan])
    mem_meas = [s for s in mem_samples if s["phase"] == "measure"] or mem_samples
    duration = t_end - t_meas
    summary = {
        "variant": variant, "workers": workers, "concurrency_level": m, "client_model": CLIENT_MODEL,
        "measure_duration_s": duration, "warmup_s": sc["level_warmup_s"], "completed_requests": len(latencies),
        "successful_requests": ok_count[0], "requests_per_second": len(latencies) / duration if duration > 0 else np.nan,
        "latency_p50_ms": float(np.nanpercentile(lat, 50)), "latency_p95_ms": float(np.nanpercentile(lat, 95)),
        "latency_p99_ms": float(np.nanpercentile(lat, 99)), "lag_samples_n": int(len(lags)),
        "lag_median_ms": float(np.median(lags)) if len(lags) else np.nan,
        "lag_p95_ms": float(np.percentile(lags, 95)) if len(lags) else np.nan,
        "lag_p99_ms": float(np.percentile(lags, 99)) if len(lags) else np.nan,
        "lag_max_ms": float(np.max(lags)) if len(lags) else np.nan,
        "tree_mem_peak_mb": max(s["tree_mem_mb"] for s in mem_meas), "tree_mem_mean_mb": float(np.mean([s["tree_mem_mb"] for s in mem_meas])),
        "children_max": max(s["n_children"] for s in mem_meas), "memory_metric": mem_meas[-1]["metric"],
        "lag_probe_interval_ms": sc["lag_probe_interval_ms"],
    }
    return summary, lag_samples, latencies, mem_samples


def make_executor(variant: str, workers: int) -> Optional[Executor]:
    if variant == "THREAD":
        return ThreadPoolExecutor(max_workers=workers)
    if variant == "POOL":
        ex = ProcessPoolExecutor(max_workers=workers, mp_context=mp.get_context("forkserver"))
        list(ex.map(_worker_validate_query, BENCHMARK_QUERIES * workers))  # start i rozgrzanie procesów poza pomiarem
        return ex
    return None


async def run(args: argparse.Namespace) -> None:
    cfg = load_config(args.config, args.mode)
    sc = cfg["scenarios"]["concurrency"]
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    seed = cfg["seeds"]["concurrency_query_order"]
    plan: List[Tuple[str, Optional[int], int]] = []
    for variant in sc["variants"]:
        for m in sc["levels"]:
            plan.append((variant, None if variant == "SYNC" else sc["default_workers"], m))
        if variant != "SYNC":
            for w in sc["workers_grid"]:
                if w != sc["default_workers"]:
                    plan.append((variant, w, sc["workers_grid_level"]))
    record_scenario_note(out, "concurrency", "client_model", CLIENT_MODEL)
    record_scenario_note(out, "concurrency", "plan", plan)
    record_scenario_note(out, "concurrency", "process_pool_start_method", "forkserver")

    summaries, lag_rows, lat_rows, mem_rows = [], [], [], []
    for variant, workers, m in plan:
        ex = make_executor(variant, workers or 0)
        try:
            s, lags, lats, mems = await run_level(variant, workers, m, sc, seed, ex)
        finally:
            if ex is not None:
                ex.shutdown(wait=True, cancel_futures=True)
        summaries.append(s)
        lag_rows += [{"variant": variant, "workers": workers, "concurrency_level": m, "ts_ns": ts, "lag_ms": l, "phase": ph} for ts, l, ph in lags]
        lat_rows += [{"variant": variant, "workers": workers, "concurrency_level": m, "latency_ms": x} for x in lats]
        mem_rows += [{"variant": variant, "workers": workers, "concurrency_level": m, **x} for x in mems]
        logger.info("%s w=%s M=%d: %.1f RPS, lag med %.2f ms (n=%d), mem %.1f MB",
                    variant, workers, m, s["requests_per_second"], s["lag_median_ms"], s["lag_samples_n"], s["tree_mem_peak_mb"])

    df = pd.DataFrame(summaries)
    df["workers"] = df["workers"].astype("Int64")
    df.to_csv(out / "concurrency_summary.csv", index=False)
    lag_df = pd.DataFrame(lag_rows)
    if len(lag_df):
        lag_df["workers"] = lag_df["workers"].astype("Int64")
    lag_df.to_parquet(out / "concurrency_lag_samples.parquet", index=False)
    pd.DataFrame(lat_rows).to_parquet(out / "concurrency_request_latencies.parquet", index=False)
    pd.DataFrame(mem_rows).to_csv(out / "concurrency_memory_samples.csv", index=False)

    tests = []
    meas = lag_df[lag_df["phase"] == "measure"] if len(lag_df) else lag_df
    for m in sc["h3_levels"]:
        a = meas[(meas.variant == "POOL") & (meas.workers == sc["default_workers"]) & (meas.concurrency_level == m)]["lag_ms"]
        b = meas[(meas.variant == "SYNC") & (meas.concurrency_level == m)]["lag_ms"]
        mw = mann_whitney(a, b)
        ci = bootstrap_median_diff_ci(a, b, seed=cfg["seeds"]["bootstrap"], n_resamples=sc["bootstrap_resamples"])
        tests.append({"hypothesis": "H3", "comparison": "POOL vs SYNC (lag pętli zdarzeń)", "concurrency_level": m,
                      "workers": sc["default_workers"], **mw, "median_diff_ci": ci})
    (out / "concurrency_h3_tests.json").write_text(json.dumps(tests, ensure_ascii=False, indent=2, default=float), encoding="utf-8")


def main() -> None:
    args = add_common_args(argparse.ArgumentParser(description=__doc__)).parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
