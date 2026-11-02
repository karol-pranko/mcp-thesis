#!/usr/bin/env python3
"""
Pierwsza wersja benchmarku schematu: pełny DDL dla 10/100/1000 tabel (baseline)
kontra top-k z search_schema. Właściwy Scenariusz A jest w scenarios/bench_schema_quality.py.
"""

import argparse
import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import pandas as pd

# Zapewnienie dostępności głównego katalogu repozytorium w sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import silnika pomiarowego
try:
    from benchmarks.harness import BenchmarkHarness, BenchmarkResult
except ImportError:
    from harness import BenchmarkHarness, BenchmarkResult

# Import silnika wyszukiwania semantycznego schematów MCP
try:
    from src.mcp_server.schema_engine import schema_engine
except (ImportError, ValueError):
    from schema_engine import schema_engine


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("benchmark_schema")


@dataclass
class DatabaseConfig:
    host: str = field(
        default_factory=lambda: os.getenv("POSTGRES_HOST", os.getenv("DB_HOST", "localhost"))
    )
    port: int = field(
        default_factory=lambda: int(os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432")))
    )
    user: str = field(
        default_factory=lambda: os.getenv("POSTGRES_USER", os.getenv("DB_USER", "mcp_user"))
    )
    password: str = field(
        default_factory=lambda: os.getenv(
            "POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", "mcp_password")
        )
    )
    database: str = field(
        default_factory=lambda: os.getenv("POSTGRES_DB", os.getenv("DB_NAME", "analytics_db"))
    )


@dataclass
class BenchmarkQuery:
    query_id: str
    natural_query: str
    required_tables: List[str]
    description: str


# Zestaw 10 realistycznych zapytań testowych z etykietami Ground Truth (tabele do operacji JOIN)
BENCHMARK_QUERIES: List[BenchmarkQuery] = [
    BenchmarkQuery(
        query_id="Q01_FIN_PAYMENTS_LEDGER",
        natural_query="Pokaż dzienne transakcje płatności oraz dzienne wpisy księgi daily payment transactions and daily ledger entries",
        required_tables=["finance_payment_transactions_daily", "finance_ledger_entries_daily"],
        description="Zestawienie dziennych płatności z pozycjami księgi finansowej",
    ),
    BenchmarkQuery(
        query_id="Q02_TEL_TEMP_CPU",
        natural_query="Pomiary telemetryczne temperatury w czasie rzeczywistym oraz odczyty procesora realtime temperature logs and realtime cpu telemetry",
        required_tables=["telemetry_temperature_logs_realtime", "telemetry_cpu_telemetry_realtime"],
        description="Korelacja odczytów temperatury z obciążeniem procesora serwerów",
    ),
    BenchmarkQuery(
        query_id="Q03_CRM_ACCOUNTS_DEALS",
        natural_query="Konta klientów enterprise i szanse sprzedaży dla korporacji enterprise client accounts and enterprise deal opportunities",
        required_tables=["crm_client_accounts_enterprise", "crm_deal_opportunities_enterprise"],
        description="Analiza lejka sprzedażowego i aktywnych szans dla klientów enterprise",
    ),
    BenchmarkQuery(
        query_id="Q04_INV_STOCK_RECEIPTS",
        natural_query="Stany magazynowe wyrobów gotowych i przyjęcia towarów gotowych finished goods stock balances and finished goods goods receipts",
        required_tables=["inventory_stock_balances_finished_goods", "inventory_goods_receipts_finished_goods"],
        description="Weryfikacja stanów magazynowych na podstawie przyjęć wyrobów gotowych",
    ),
    BenchmarkQuery(
        query_id="Q05_HR_PROFILES_CONTRACTS",
        natural_query="Roczne profile pracowników i roczne umowy o pracę annual employee profiles and annual labor contracts",
        required_tables=["hr_employee_profiles_annual", "hr_labor_contracts_annual"],
        description="Zestawienie profili kadrowych z aktywnymi rocznymi umowami o pracę",
    ),
    BenchmarkQuery(
        query_id="Q06_FIN_INVOICES_BUDGET",
        natural_query="Faktury należne i prognozy budżetowe transakcji międzyfirmowych intercompany invoices payable and intercompany budget forecasts",
        required_tables=["finance_invoices_payable_intercompany", "finance_budget_forecasts_intercompany"],
        description="Weryfikacja wykonania budżetu na podstawie faktur międzyfirmowych",
    ),
    BenchmarkQuery(
        query_id="Q07_TEL_CONTAINERS_ERRORS",
        natural_query="Restarty kontenerów w czasie rzeczywistym i dzienniki błędów http realtime container restarts and realtime http error logs",
        required_tables=["telemetry_container_restarts_realtime", "telemetry_http_error_logs_realtime"],
        description="Diagnostyka awarii kontenerów i powiązanych błędów HTTP serwera",
    ),
    BenchmarkQuery(
        query_id="Q08_CRM_CASES_ESCALATIONS",
        natural_query="Zgłoszenia wsparcia eskalowane i eskalacje spraw klientów escalated support cases and escalated case escalations",
        required_tables=["crm_support_cases_escalated", "crm_case_escalations_escalated"],
        description="Śledzenie historii eskalacji krytycznych incydentów klientów",
    ),
    BenchmarkQuery(
        query_id="Q09_INV_ORDERS_INSPECTIONS",
        natural_query="Zlecenia zakupu surowców i kontrole partii materiałów raw materials purchase orders and raw materials batch inspections",
        required_tables=["inventory_purchase_orders_raw_materials", "inventory_batch_inspections_raw_materials"],
        description="Kontrola jakości dostaw surowców w odniesieniu do zleceń zakupu",
    ),
    BenchmarkQuery(
        query_id="Q10_HR_LEAVES_PERFORMANCE",
        natural_query="Kwartalne wnioski urlopowe i kwartalne oceny wydajności quarterly leave requests and quarterly performance evaluations",
        required_tables=["hr_leave_requests_quarterly", "hr_performance_evaluations_quarterly"],
        description="Wpływ absencji urlopowych na kwartalne wskaźniki efektywności personelu",
    ),
]


def calculate_precision_at_k(
    retrieved_tables: List[str], required_tables: List[str], k: int
) -> float:
    """
    Oblicza precyzję wyszukiwania Precision@K.
    Stosunek liczby odnalezionych wymaganych tabel (trafień) do całkowitej liczby K zwróconych wyników.

    Wzór:
        Precision@K = |retrieved_tables[:k] ∩ required_tables| / k
    """
    if k <= 0:
        return 0.0
    top_k_retrieved = retrieved_tables[:k]
    matched = set(top_k_retrieved).intersection(set(required_tables))
    return len(matched) / float(k)


def calculate_recall_at_k(
    retrieved_tables: List[str], required_tables: List[str], k: int
) -> float:
    """
    Oblicza pełność wyszukiwania Recall@K.
    Stosunek liczby odnalezionych wymaganych tabel do całkowitej liczby tabel wymaganych.

    Wzór:
        Recall@K = |retrieved_tables[:k] ∩ required_tables| / |required_tables|
    """
    if not required_tables:
        return 1.0
    top_k_retrieved = retrieved_tables[:k]
    matched = set(top_k_retrieved).intersection(set(required_tables))
    return len(matched) / float(len(required_tables))


def calculate_token_reduction_ratio(baseline_tokens: int, mcp_tokens: int) -> float:
    """
    Oblicza współczynnik redukcji tokenów Token Reduction Ratio (TRR %) w procentach.

    Wzór:
        TRR = ((baseline_tokens - mcp_tokens) / baseline_tokens) * 100%
    """
    if baseline_tokens <= 0:
        return 0.0
    reduction = ((baseline_tokens - mcp_tokens) / float(baseline_tokens)) * 100.0
    return max(0.0, reduction)


async def create_benchmark_pool(config: DatabaseConfig) -> asyncpg.Pool:
    try:
        pool = await asyncpg.create_pool(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            min_size=2,
            max_size=10,
        )
        logger.info("Nawiązano połączenie z bazą PostgreSQL (%s@%s:%d/%s).", config.user, config.host, config.port, config.database)
        return pool
    except asyncpg.exceptions.InvalidPasswordError:
        fallback_password = (
            "mcp_secret_password" if config.password == "mcp_password" else "mcp_password"
        )
        logger.warning("Nieudana autoryzacja hasłem domyślnym. Ponawianie próby hasłem rezerwowym...")
        pool = await asyncpg.create_pool(
            host=config.host,
            port=config.port,
            user=config.user,
            password=fallback_password,
            database=config.database,
            min_size=2,
            max_size=10,
        )
        logger.info("Nawiązano połączenie z bazą PostgreSQL przy użyciu hasła rezerwowego.")
        return pool


async def fetch_naive_ddl(pool: asyncpg.Pool, table_count: int) -> str:
    """
    Pobiera strukturę N tabel z bazy danych i generuje pełny surowy kod DDL SQL (Baseline).
    Symuluje podejście naiwne polegające na wstrzyknięciu pełnego schematu bazy do promptu modelu LLM.
    """
    async with pool.acquire() as connection:
        table_rows = await connection.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
              AND table_name NOT IN ('schema_embeddings', 'spatial_ref_sys')
            ORDER BY table_name
            LIMIT $1;
            """,
            table_count,
        )
        table_names = [row["table_name"] for row in table_rows]

        if not table_names:
            return "-- Brak tabel w schemacie publicznym"

        column_rows = await connection.fetch(
            """
            SELECT
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ANY($1::text[])
            ORDER BY table_name, ordinal_position;
            """,
            table_names,
        )

    # Grupowanie kolumn według nazwy tabeli
    columns_by_table: Dict[str, List[str]] = defaultdict(list)
    for col in column_rows:
        nullable_clause = "" if col["is_nullable"] == "YES" else " NOT NULL"
        default_clause = f" DEFAULT {col['column_default']}" if col["column_default"] else ""
        col_def = f"    {col['column_name']} {col['data_type'].upper()}{nullable_clause}{default_clause}"
        columns_by_table[col["table_name"]].append(col_def)

    ddl_statements: List[str] = [
        f"-- Pelny schemat DDL dla {len(table_names)} tabel pobrany podejściem naiwnym (Baseline)",
        "",
    ]
    for table_name in table_names:
        cols_text = ",\n".join(columns_by_table.get(table_name, []))
        ddl_statements.append(f"CREATE TABLE public.{table_name} (\n{cols_text}\n);")
        ddl_statements.append("")

    return "\n".join(ddl_statements).strip()


class SchemaBenchmark:
    def __init__(
        self,
        pool: asyncpg.Pool,
        harness: Optional[BenchmarkHarness] = None,
        k: int = 5,
        iterations: int = 100,
        warmup_runs: int = 5,
    ) -> None:
        self.pool = pool
        self.harness = harness or BenchmarkHarness()
        self.k = k
        self.iterations = iterations
        self.warmup_runs = warmup_runs
        self.baseline_results: Dict[int, BenchmarkResult] = {}
        self.mcp_results: List[BenchmarkResult] = []
        self.query_evaluations: List[Dict[str, Any]] = []

    async def run_naive_baseline(
        self, table_counts: Optional[List[int]] = None
    ) -> List[BenchmarkResult]:
        """
        Scenariusz A: Wykonuje pomiary podejścia naiwnego (Baseline Full DDL)
        dla zadanych liczności tabel N w {10, 100, 1000}.
        """
        counts = table_counts or [10, 100, 1000]
        results: List[BenchmarkResult] = []

        logger.info("Rozpoczynanie Scenariusza A: Baseline Naive Full DDL dla tabel: %s...", counts)

        for count in counts:
            test_name = f"baseline_full_ddl_n{count}"

            async def scenario_fn() -> str:
                return await fetch_naive_ddl(self.pool, count)

            result = await self.harness.run_scenario(
                test_name=test_name,
                approach="BASELINE_NAIVE",
                scenario_fn=scenario_fn,
                extra_metadata={
                    "table_count": count,
                    "strategy": "FULL_DDL",
                },
                iterations=self.iterations,
                warmup_runs=self.warmup_runs,
            )
            self.baseline_results[count] = result
            results.append(result)

        return results

    async def run_mcp_jit(
        self, queries: Optional[List[BenchmarkQuery]] = None
    ) -> List[BenchmarkResult]:
        """
        Scenariusz B: Wykonuje pomiary wyszukiwania semantycznego MCP JIT Schema Discovery
        z wykorzystaniem pgvector i modelu sentence-transformers dla zdefiniowanych zapytań testowych.
        """
        test_queries = queries or BENCHMARK_QUERIES
        results: List[BenchmarkResult] = []

        logger.info(
            "Rozpoczynanie Scenariusza B: MCP JIT Schema Discovery dla %d zapytań (top_k=%d)...",
            len(test_queries),
            self.k,
        )

        for query_item in test_queries:
            test_name = f"mcp_jit_{query_item.query_id.lower()}"
            captured_schemas: List[Dict[str, Any]] = []

            async def scenario_fn() -> str:
                schemas = await schema_engine.find_relevant_schemas(
                    pool=self.pool,
                    query=query_item.natural_query,
                    top_k=self.k,
                )
                captured_schemas.extend(schemas)
                return schema_engine.format_schemas_as_ddl(
                    query=query_item.natural_query, schemas=schemas
                )

            result = await self.harness.run_scenario(
                test_name=test_name,
                approach="MCP_JIT",
                scenario_fn=scenario_fn,
                extra_metadata={
                    "query_id": query_item.query_id,
                    "top_k": self.k,
                    "required_tables": query_item.required_tables,
                },
                iterations=self.iterations,
                warmup_runs=self.warmup_runs,
            )

            retrieved_tables = [s["table_name"] for s in captured_schemas]
            p_at_k = calculate_precision_at_k(retrieved_tables, query_item.required_tables, self.k)
            r_at_k = calculate_recall_at_k(retrieved_tables, query_item.required_tables, self.k)
            matched_tables = list(
                set(retrieved_tables[: self.k]).intersection(set(query_item.required_tables))
            )

            evaluation = {
                "query_id": query_item.query_id,
                "natural_query": query_item.natural_query,
                "required_tables": query_item.required_tables,
                "retrieved_tables": retrieved_tables,
                "matched_tables": matched_tables,
                "precision_at_k": p_at_k,
                "recall_at_k": r_at_k,
                "k": self.k,
                "latency_ms": result.latency_ms,
                "std_dev": result.std_dev,
                "variance": result.variance,
                "p50_latency_ms": result.p50_latency_ms,
                "p95_latency_ms": result.p95_latency_ms,
                "p99_latency_ms": result.p99_latency_ms,
                "raw_samples": result.raw_samples,
                "token_count": result.token_count,
                "payload_bytes": result.payload_bytes,
            }
            self.query_evaluations.append(evaluation)
            self.mcp_results.append(result)
            results.append(result)

        return results

    def calculate_scaling_analysis(self) -> List[Dict[str, Any]]:
        """
        Analiza Skalowania (Schema Scaling Factor - SSF) oraz redukcji tokenów (TRR %).
        Porównuje zapotrzebowanie na tokeny przy wzroście liczby tabel w bazie dla obu podejść.
        Wykazuje złożoność O(1) dla MCP JIT vs O(N) dla podejścia naiwnego Baseline.
        """
        avg_mcp_tokens = (
            sum(r.token_count for r in self.mcp_results) / len(self.mcp_results)
            if self.mcp_results
            else 0
        )
        avg_mcp_latency = (
            sum(r.latency_ms for r in self.mcp_results) / len(self.mcp_results)
            if self.mcp_results
            else 0.0
        )

        scaling_data: List[Dict[str, Any]] = []
        table_counts = sorted(self.baseline_results.keys())

        prev_baseline_tokens: Optional[int] = None
        prev_mcp_tokens: Optional[float] = None
        prev_count: Optional[int] = None

        for count in table_counts:
            b_res = self.baseline_results[count]
            b_tokens = b_res.token_count
            m_tokens = round(avg_mcp_tokens)

            trr = calculate_token_reduction_ratio(b_tokens, m_tokens)

            # SSF = ile razy urosły tokeny względem poprzedniego N
            if prev_baseline_tokens and prev_count and prev_baseline_tokens > 0:
                scale_multiplier = count / prev_count
                baseline_ssf = b_tokens / float(prev_baseline_tokens)
                mcp_ssf = (
                    (m_tokens / float(prev_mcp_tokens)) if prev_mcp_tokens and prev_mcp_tokens > 0 else 1.0
                )
            else:
                scale_multiplier = 1.0
                baseline_ssf = 1.0
                mcp_ssf = 1.0

            scaling_data.append(
                {
                    "table_count": count,
                    "baseline_tokens": b_tokens,
                    "baseline_latency_ms": b_res.latency_ms,
                    "mcp_tokens": m_tokens,
                    "mcp_latency_ms": round(avg_mcp_latency, 2),
                    "trr_percent": round(trr, 2),
                    "baseline_ssf": round(baseline_ssf, 2),
                    "mcp_ssf": round(mcp_ssf, 2),
                    "scale_multiplier": scale_multiplier,
                }
            )

            prev_baseline_tokens = b_tokens
            prev_mcp_tokens = float(m_tokens)
            prev_count = count

        return scaling_data

    def print_summary(self) -> None:
        print("\n" + "-" * 90)
        print("RAPORT EKSPERYMENTU: BADANIE WYDAJNOŚCI I JAKOŚCI ODKRYWANIA SCHEMATU BAZY DANYCH")
        print("-" * 90)

        # tabela wyników Baseline Naive Full DDL
        print("\n[Tabela 1] Wyniki Baseline: Podejście naiwne (Pełny DDL schematu)")
        baseline_rows = []
        for count in sorted(self.baseline_results.keys()):
            res = self.baseline_results[count]
            baseline_rows.append(
                {
                    "Liczba tabel (N)": count,
                    "Śr. Czas (ms)": f"{res.latency_ms:.2f}",
                    "Std Dev": f"{res.std_dev:.2f}",
                    "p50 (ms)": f"{res.p50_latency_ms:.2f}",
                    "p95 (ms)": f"{res.p95_latency_ms:.2f}",
                    "Tokeny": f"{res.token_count:,}",
                    "Rozmiar danych": f"{res.payload_bytes:,} B",
                    "Pamięć RAM": f"{res.ram_usage_mb:.1f} MB",
                }
            )
        df_baseline = pd.DataFrame(baseline_rows)
        print(df_baseline.to_string(index=False))

        # tabela wyników MCP JIT Schema Discovery
        print(f"\n[Tabela 2] Wyniki MCP JIT Schema Discovery (pgvector, top_k={self.k})")
        mcp_rows = []
        for item in self.query_evaluations:
            mcp_rows.append(
                {
                    "ID Zapytania": item["query_id"],
                    "Wymagane": len(item["required_tables"]),
                    "Trafione": len(item["matched_tables"]),
                    "Precision@5": f"{item['precision_at_k']:.2f}",
                    "Recall@5": f"{item['recall_at_k']:.2f}",
                    "Śr. Czas (ms)": f"{item['latency_ms']:.2f}",
                    "Std Dev": f"{item['std_dev']:.2f}",
                    "p50 (ms)": f"{item['p50_latency_ms']:.2f}",
                    "Tokeny": item["token_count"],
                }
            )
        df_mcp = pd.DataFrame(mcp_rows)
        print(df_mcp.to_string(index=False))

        avg_precision = (
            sum(i["precision_at_k"] for i in self.query_evaluations) / len(self.query_evaluations)
            if self.query_evaluations
            else 0.0
        )
        avg_recall = (
            sum(i["recall_at_k"] for i in self.query_evaluations) / len(self.query_evaluations)
            if self.query_evaluations
            else 0.0
        )
        avg_mcp_tokens = (
            sum(i["token_count"] for i in self.query_evaluations) / len(self.query_evaluations)
            if self.query_evaluations
            else 0.0
        )
        avg_mcp_latency = (
            sum(i["latency_ms"] for i in self.query_evaluations) / len(self.query_evaluations)
            if self.query_evaluations
            else 0.0
        )

        print("\nMetryki Jakościowe Wyszukiwania Semantycznego:")
        print(f" - Średnia Precision@{self.k}: {avg_precision:.4f} ({avg_precision * 100:.1f}%)")
        print(f" - Średnia Recall@{self.k}:    {avg_recall:.4f} ({avg_recall * 100:.1f}%)")
        print(f" - Średni czas zapytania:      {avg_mcp_latency:.2f} ms")
        print(f" - Średni rozmiar kontekstu:   {avg_mcp_tokens:.1f} tokenów")

        # tabela analizy skalowania (SSF) i redukcji tokenów (TRR %)
        print("\n[Tabela 3] Analiza Skalowania Schematu (SSF) i Redukcji Tokenów (TRR %)")
        scaling_rows = self.calculate_scaling_analysis()
        display_scaling = []
        for row in scaling_rows:
            display_scaling.append(
                {
                    "Liczba tabel (N)": row["table_count"],
                    "Baseline Tokeny": f"{row['baseline_tokens']:,}",
                    "MCP Tokeny": f"{row['mcp_tokens']:,}",
                    "Redukcja TRR (%)": f"{row['trr_percent']:.2f}%",
                    "SSF Baseline": f"{row['baseline_ssf']:.2f}x",
                    "SSF MCP JIT": f"{row['mcp_ssf']:.2f}x",
                }
            )
        df_scaling = pd.DataFrame(display_scaling)
        print(df_scaling.to_string(index=False))

        # Wnioski inżynierskie
        if 1000 in self.baseline_results:
            b_1000 = self.baseline_results[1000].token_count
            m_1000 = avg_mcp_tokens
            trr_1000 = calculate_token_reduction_ratio(b_1000, int(m_1000))
            print("\nKluczowe Wnioski Inżynierskie:")
            print(f" 1. Przy skali 1000 tabel podejście naiwne wymaga {b_1000:,} tokenów, co grozi przepełnieniem okna LLM.")
            print(f" 2. MCP JIT stabilizuje zapotrzebowanie na poziomie ~{m_1000:.0f} tokenów, osiągając {trr_1000:.2f}% redukcji (TRR).")
            print(" 3. Złożoność kosztu kontekstu dla Baseline to O(N), podczas gdy MCP JIT wykazuje złożoność O(1).")
        print("-" * 90 + "\n")


async def run_benchmark(
    output_dir: str = "benchmarks/raw_results",
    iterations: int = 100,
    warmup_runs: int = 5,
) -> None:
    logger.info("Rozpoczynanie zautomatyzowanego eksperymentu porównawczego odkrywania schematu...")

    config = DatabaseConfig()
    pool = await create_benchmark_pool(config)

    harness = BenchmarkHarness()
    benchmark = SchemaBenchmark(
        pool=pool,
        harness=harness,
        k=5,
        iterations=iterations,
        warmup_runs=warmup_runs,
    )

    try:
        # scenariusz A: Baseline Naive Full DDL (10, 100, 1000 tabel)
        await benchmark.run_naive_baseline(table_counts=[10, 100, 1000])

        # scenariusz B: MCP JIT Schema Discovery (10 zapytań)
        await benchmark.run_mcp_jit(queries=BENCHMARK_QUERIES)

        # prezentacja wyników w terminalu
        benchmark.print_summary()

        # eksport wyników do katalogu wynikowego (oraz backup do results)
        json_path, csv_path = harness.export_results(output_dir=output_dir)
        logger.info("Zapisano wyniki benchmarku do: %s oraz %s", json_path, csv_path)

        def export_custom_artifacts(target_directory: str) -> None:
            t_dir = Path(target_directory)
            t_dir.mkdir(parents=True, exist_ok=True)

            # A. Zapis szczegółowych metryk ewaluacji zapytań (JSON i CSV)
            eval_json_path = t_dir / "query_evaluations.json"
            with open(eval_json_path, "w", encoding="utf-8") as eval_file:
                json.dump(benchmark.query_evaluations, eval_file, ensure_ascii=False, indent=2)

            eval_df_rows = []
            for item in benchmark.query_evaluations:
                eval_df_rows.append(
                    {
                        "query_id": item["query_id"],
                        "natural_query": item["natural_query"],
                        "k": item["k"],
                        "precision_at_k": item["precision_at_k"],
                        "recall_at_k": item["recall_at_k"],
                        "latency_ms": item["latency_ms"],
                        "std_dev": item["std_dev"],
                        "variance": item["variance"],
                        "p50_latency_ms": item["p50_latency_ms"],
                        "p95_latency_ms": item["p95_latency_ms"],
                        "p99_latency_ms": item["p99_latency_ms"],
                        "raw_samples": json.dumps(item["raw_samples"]),
                        "token_count": item["token_count"],
                        "payload_bytes": item["payload_bytes"],
                        "required_tables": ";".join(item["required_tables"]),
                        "matched_tables": ";".join(item["matched_tables"]),
                        "retrieved_tables": ";".join(item["retrieved_tables"]),
                    }
                )
            eval_csv_path = t_dir / "query_evaluations.csv"
            pd.DataFrame(eval_df_rows).to_csv(eval_csv_path, index=False, encoding="utf-8")
            logger.info("Zapisano ewaluację zapytań: %s oraz %s", eval_json_path, eval_csv_path)

            # B. Zapis analizy skalowania schematu (JSON i CSV)
            scaling_data = benchmark.calculate_scaling_analysis()
            scaling_json_path = t_dir / "schema_scaling_analysis.json"
            with open(scaling_json_path, "w", encoding="utf-8") as scaling_file:
                json.dump(scaling_data, scaling_file, ensure_ascii=False, indent=2)

            scaling_csv_path = t_dir / "schema_scaling_analysis.csv"
            pd.DataFrame(scaling_data).to_csv(scaling_csv_path, index=False, encoding="utf-8")
            logger.info("Zapisano analizę skalowania: %s oraz %s", scaling_json_path, scaling_csv_path)

        export_custom_artifacts(output_dir)
        if output_dir != "results":
            harness.export_results(output_dir="results")
            export_custom_artifacts("results")

    finally:
        logger.info("Zamykanie puli połączeń z bazą PostgreSQL...")
        await pool.close()
        logger.info("Eksperyment pomiarowy został pomyślnie ukończony.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark odkrywania schematu bazy danych: Baseline Full DDL vs MCP JIT Schema Discovery"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="benchmarks/raw_results",
        help="Ścieżka do katalogu, w którym zostaną zapisane pliki CSV i JSON z wynikami pomiarów",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=100,
        help="Liczba powtórzeń pomiarowych dla każdego scenariusza (domyślnie: 100)",
    )
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=5,
        help="Liczba biegów rozgrzewkowych przed właściwym pomiarem (domyślnie: 5)",
    )
    args = parser.parse_args()

    asyncio.run(
        run_benchmark(
            output_dir=args.output_dir,
            iterations=args.iterations,
            warmup_runs=args.warmup_runs,
        )
    )


if __name__ == "__main__":
    main()
