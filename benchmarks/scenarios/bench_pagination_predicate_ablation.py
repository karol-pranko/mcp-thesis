#!/usr/bin/env python3
"""
Ablacja postaci predykatu kursorowego keyset na relacji public.analytics_orders (1 mln rekordów).

Porównuje w jednej sesji, na tej samej instancji PostgreSQL:
- "DNF (OR)": pojedynczy predykat w postaci alternatywy dysjunktywnej (use_union_all=False),
- "UNION ALL": rozłączne gałęzie z prostymi predykatami (use_union_all=True).

Dla offsetów scenarios.pagination.explain_offsets zapisywany jest pełny plan
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) obu postaci predykatu (pagination_explain_plans.json).
Kursor: zob. CURSOR_CONSTRUCTION w bench_pagination_advanced.py (wiersz na pozycji M-1, poza pomiarem).

Pliki: pagination_predicate_ablation.csv, pagination_explain_plans.json.
"""

import argparse
import asyncio
import json
from pathlib import Path
import sys
import time

import asyncpg
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.common.config import add_common_args, db_params, load_config  # noqa: E402
from benchmarks.common.db import discard_all  # noqa: E402
from src.mcp_server.pagination import build_keyset_query, normalize_order_by  # noqa: E402

TABLE = "public.analytics_orders"
ORDER_BY = "created_at DESC NULLS LAST, amount ASC NULLS LAST, id ASC"
VARIANTS = (("DNF (OR)", False), ("UNION ALL", True))


async def main() -> None:
    args = add_common_args(argparse.ArgumentParser(description="Ablacja postaci predykatu keyset (DNF vs UNION ALL).")).parse_args()
    cfg = load_config(args.config, args.mode)
    sc = cfg["scenarios"]["pagination"]
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    pool = await asyncpg.create_pool(min_size=1, max_size=2, **db_params(cfg))
    specs = normalize_order_by(ORDER_BY)
    rows_out, plans = [], []
    try:
        await discard_all(pool, "pagination/predicate_ablation")
        async with pool.acquire() as conn:
            cursors = {}
            for off in sc["offsets"]:
                if off == 0:
                    cursors[off] = None
                    continue
                row = await conn.fetchrow(f"SELECT id, created_at, amount FROM {TABLE} ORDER BY {ORDER_BY} OFFSET $1 LIMIT 1;", off - 1)
                cursors[off] = {"last_values": [row["created_at"], row["amount"], row["id"]], "last_id": row["id"]}
            for off in sc["offsets"]:
                for variant, union in VARIANTS:
                    sql, params = build_keyset_query(safe_table=TABLE, columns="id, created_at, amount, status", order_by=specs,
                                                     cursor_data=cursors[off], fetch_limit=sc["page_size"], use_union_all=union)
                    for _ in range(sc["warmup"]):
                        await conn.fetch(sql, *params)
                    for rep in range(1, sc["repetitions"] + 1):
                        t0 = time.perf_counter()
                        res = await conn.fetch(sql, *params)
                        rows_out.append({"variant": variant, "offset_rows": off, "iteration": rep,
                                         "latency_ms": round((time.perf_counter() - t0) * 1000.0, 3), "records_retrieved": len(res)})
                    if off in sc["explain_offsets"]:
                        plan = await conn.fetchval(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}", *params)
                        plans.append({"variant": variant, "offset_rows": off, "sql": sql,
                                      "params": [str(p) for p in params], "plan": json.loads(plan) if isinstance(plan, str) else plan})
    finally:
        await pool.close()
    df = pd.DataFrame(rows_out)
    df.to_csv(out / "pagination_predicate_ablation.csv", index=False)
    (out / "pagination_explain_plans.json").write_text(json.dumps(plans, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(df.groupby(["offset_rows", "variant"])["latency_ms"].agg(["median", "min", "max"]).round(3).to_string())


if __name__ == "__main__":
    asyncio.run(main())
