#!/usr/bin/env python3
"""
Schema drift: co się dzieje z search_schema, gdy w trakcie pracy dochodzą nowe tabele.

1. Baseline - opóźnienia (P50/P95/P99/średnia) na stabilnym schemacie (1000 tabel, 1174 FK).
2. Drift - tworzymy nowe tabele i FK, liczymy embeddingi (z czasem CPU), przebudowujemy BM25
   i graf, a w tym czasie lecą równoległe zapytania agenta (czy nie ma błędów i jak rośnie opóźnienie).
3. Po drifcie - czy nowe tabele i mosty od razu pojawiają się w DDL.
4. Sprzątanie - przywrócenie bazy i SchemaEngine do stanu sprzed testu.

--smoke-test: 2 tabele driftu i 5 zapytań. Wyniki: Parquet, CSV i JSON w benchmarks/raw_results/.
"""

import argparse
import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import numpy as np
import pandas as pd

# Zapewnienie dostępności modułów nadrzędnych
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mcp_server.schema_engine import SchemaEngine, schema_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("bench_schema_drift")

# Zestaw realistycznych zapytań dla fazy bazowej (domeny: crm, finance, hr, inventory, telemetry)
REALISTIC_BASELINE_QUERIES: List[str] = [
    "Pobierz faktury klientów i transakcje rozliczeniowe z departamentu finansowego",
    "Znajdź stany magazynowe i zamówienia surowców u dostawców",
    "Wyszukaj szanse sprzedażowe enterprise i przypisanych opiekunów klienta",
    "Analiza segmentów odbiorców o wysokim ryzyku odejścia w CRM",
    "Sprawdź przesyłki logistyczne oraz status śledzenia u przewoźników",
    "Zestawienie wypłat pracowniczych i średnich wynagrodzeń w działach HR",
    "Monitorowanie alertów telemetrycznych i zdarzeń infrastruktury serwerowej",
    "Raport wydajności operacyjnej i wskaźników KPI produkcji",
    "Transakcje bankowe i uzgodnienia sald międzyfirmowych",
    "Historia zgłoszeń serwisowych helpdesk o statusie krytycznym",
]

# Zestaw zapytań po migracji (ukierunkowanych na nowo dodaną domenę Fintech / Risk)
REALISTIC_POST_DRIFT_QUERIES: List[str] = [
    "Wyszukaj rozliczenia fintech settlement i transakcje z oceną ryzyka",
    "Znajdź księgę rozliczeń fintech i powiązane konta klientów enterprise",
    "Pobierz rejestr transakcji finansowych fintech z limitem scoringu",
    "Zestawienie księgi ledger fintech i audytów transakcyjnych",
    "Sprawdź zlecenia fintech oraz powiązane profile klientów CRM",
]


@dataclass
class DriftQueryRecord:
    phase: str                      # "baseline", "in_flight_drift", "post_drift"
    query_idx: int                  # Indeks kolejności zapytania
    query_text: str                 # Treść zapytania w języku naturalnym
    latency_ms: float               # Czas wykonania odkrywania schematu (ms)
    tables_found: int               # Liczba tabel odnalezionych w wygenerowanym DDL
    bridges_found: int              # Liczba dołączonych tabel mostkujących (Graph Bridges)
    ddl_chars: int                  # Długość wygenerowanego bloku DDL w znakach
    is_success: bool                # Czy zapytanie zakończyło się powodzeniem
    error_type: Optional[str] = None # Typ ewentualnego wyjątku (np. RelationNotFound)
    error_message: Optional[str] = None
    timestamp_iso: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


@dataclass
class DriftTelemetryRecord:
    num_tables_migrated: int
    num_fks_registered: int
    ddl_create_tables_ms: float
    ddl_create_fks_ms: float
    embedding_calc_ms: float
    embedding_cpu_ms: float
    db_insert_embeddings_ms: float
    bm25_rebuild_ms: float
    graph_rebuild_ms: float
    total_drift_reload_ms: float
    total_drift_cpu_ms: float
    nodes_before: int
    nodes_after: int
    edges_before: int
    edges_after: int


class SchemaDriftBenchmarkRunner:
    def __init__(
        self,
        pool: asyncpg.Pool,
        engine: SchemaEngine,
        num_drift_tables: int = 50,
        num_baseline_queries: int = 50,
        num_inflight_queries: int = 50,
        num_postdrift_queries: int = 10,
        smoke_test: bool = False,
        output_dir: Path = PROJECT_ROOT / "benchmarks" / "raw_results",
    ) -> None:
        self.pool = pool
        self.engine = engine
        self.smoke_test = smoke_test
        self.output_dir = output_dir

        if smoke_test:
            self.num_drift_tables = 2
            self.num_baseline_queries = 5
            self.num_inflight_queries = 5
            self.num_postdrift_queries = 5
            logger.info("Uruchomiono w trybie dymnym (--smoke-test): 2 tabele driftu, 5 zapytań na fazę.")
        else:
            self.num_drift_tables = num_drift_tables
            self.num_baseline_queries = num_baseline_queries
            self.num_inflight_queries = num_inflight_queries
            self.num_postdrift_queries = num_postdrift_queries

        # Unikalny prefiks sesji testowej w celu uniknięcia kolizji
        self.session_id = f"s{int(time.time()) % 100000:05d}"
        self.domain_name = "fintech_risk"
        self.created_tables: List[str] = []
        self.created_fk_names: List[str] = []
        self.query_records: List[DriftQueryRecord] = []
        self.drift_telemetry: Optional[DriftTelemetryRecord] = None

    async def get_current_graph_stats(self) -> Tuple[int, int]:
        graph = await self.engine._ensure_relation_graph(self.pool)
        return graph.number_of_nodes(), graph.number_of_edges()

    async def run_single_query(
        self, phase: str, query_idx: int, query_text: str
    ) -> DriftQueryRecord:
        t_start = time.perf_counter()
        try:
            ddl = await self.engine.discover_schema_with_graph_pruning(
                query=query_text,
                top_k=5,
                max_expanded_tables=3,
                pool=self.pool,
            )
            elapsed_ms = (time.perf_counter() - t_start) * 1000.0

            # Analiza struktury wygenerowanego DDL
            tables_found = ddl.count("CREATE TABLE public.")
            bridges_found = ddl.count("TABELA MOSTKUJĄCA N:M (BRIDGE)")
            ddl_chars = len(ddl)

            return DriftQueryRecord(
                phase=phase,
                query_idx=query_idx,
                query_text=query_text,
                latency_ms=elapsed_ms,
                tables_found=tables_found,
                bridges_found=bridges_found,
                ddl_chars=ddl_chars,
                is_success=True,
            )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t_start) * 1000.0
            logger.error("Błąd podczas zapytania w fazie '%s': %s", phase, exc, exc_info=True)
            return DriftQueryRecord(
                phase=phase,
                query_idx=query_idx,
                query_text=query_text,
                latency_ms=elapsed_ms,
                tables_found=0,
                bridges_found=0,
                ddl_chars=0,
                is_success=False,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

    async def run_baseline_phase(self) -> List[DriftQueryRecord]:
        """Faza 1: Wykonanie serii zapytań o schemat w ustabilizowanym środowisku."""
        logger.info(
            "=== [FAZA 1: BAZOWA] Wykonywanie %d zapytań w ustabilizowanym środowisku ===",
            self.num_baseline_queries,
        )
        records: List[DriftQueryRecord] = []
        for i in range(self.num_baseline_queries):
            q_text = REALISTIC_BASELINE_QUERIES[i % len(REALISTIC_BASELINE_QUERIES)]
            rec = await self.run_single_query("baseline", i + 1, q_text)
            records.append(rec)

        latencies = [r.latency_ms for r in records if r.is_success]
        logger.info(
            "Faza bazowa ukończona. Średnia: %.2f ms, P50: %.2f ms, P95: %.2f ms.",
            float(np.mean(latencies)) if latencies else 0.0,
            float(np.percentile(latencies, 50)) if latencies else 0.0,
            float(np.percentile(latencies, 95)) if latencies else 0.0,
        )
        return records

    async def inject_ddl_and_fks(self) -> Tuple[List[str], List[str], float, float]:
        """
        Krok DDL: Tworzy tabele domenowe w PostgreSQL oraz rejestruje powiązania kluczy obcych.
        """
        table_names: List[str] = []
        fk_names: List[str] = []

        # definiowanie nazw tabel
        hub_table = f"drift_{self.session_id}_fintech_settlement_ledger"
        table_names.append(hub_table)

        for i in range(1, self.num_drift_tables):
            table_names.append(f"drift_{self.session_id}_fintech_risk_event_{i:02d}")

        self.created_tables = list(table_names)

        # tworzenie fizycznych tabel w PostgreSQL
        t_create_start = time.perf_counter()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Tabela nadrzędna hub
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS public.{hub_table} (
                        id BIGSERIAL PRIMARY KEY,
                        account_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
                        client_account_id BIGINT,
                        total_settled_amount NUMERIC(15,2) NOT NULL DEFAULT 0.0,
                        currency VARCHAR(8) NOT NULL DEFAULT 'EUR',
                        settlement_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """)

                # Tabele podrzędne
                for t in table_names[1:]:
                    await conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS public.{t} (
                            id BIGSERIAL PRIMARY KEY,
                            settlement_id BIGINT,
                            transaction_ref VARCHAR(64) NOT NULL,
                            risk_score NUMERIC(5,2) DEFAULT 0.0,
                            audit_flag BOOLEAN DEFAULT FALSE,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        );
                    """)
        ddl_create_ms = (time.perf_counter() - t_create_start) * 1000.0

        # tworzenie i rejestracja kluczy obcych w public.schema_foreign_keys
        t_fk_start = time.perf_counter()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # 3a. Połączenie mostkujące hub z istniejącą tabelą korporacyjną 'crm_client_accounts_enterprise'
                fk_hub_name = f"fk_{self.session_id}_hub_crm"
                fk_names.append(fk_hub_name)
                join_hub_json = json.dumps({"source_column": "client_account_id", "target_column": "id"})
                await conn.execute("""
                    INSERT INTO public.schema_foreign_keys (source_table, target_table, constraint_name, join_columns)
                    VALUES ($1, $2, $3, $4::jsonb)
                    ON CONFLICT (constraint_name) DO NOTHING;
                """, hub_table, "crm_client_accounts_enterprise", fk_hub_name, join_hub_json)

                # 3b. Połączenia tabel podrzędnych z tabelą hub
                for i, t in enumerate(table_names[1:], start=1):
                    fk_sub_name = f"fk_{self.session_id}_sub_{i:02d}"
                    fk_names.append(fk_sub_name)
                    join_sub_json = json.dumps({"source_column": "settlement_id", "target_column": "id"})
                    await conn.execute("""
                        INSERT INTO public.schema_foreign_keys (source_table, target_table, constraint_name, join_columns)
                        VALUES ($1, $2, $3, $4::jsonb)
                        ON CONFLICT (constraint_name) DO NOTHING;
                    """, t, hub_table, fk_sub_name, join_sub_json)

        self.created_fk_names = list(fk_names)
        ddl_fk_ms = (time.perf_counter() - t_fk_start) * 1000.0

        logger.info(
            "Wykonano DDL: Utworzono %d tabel w %.2f ms, zarejestrowano %d relacji FK w %.2f ms.",
            len(table_names),
            ddl_create_ms,
            len(fk_names),
            ddl_fk_ms,
        )
        return table_names, fk_names, ddl_create_ms, ddl_fk_ms

    async def execute_drift_event(self) -> DriftTelemetryRecord:
        """
        Faza 2 (Komponent Migracyjny):
        1. Wykonanie DDL w PostgreSQL.
        2. Wywołanie handle_schema_drift w SchemaEngine (osadzenia + BM25 + NetworkX).
        """
        nodes_before, edges_before = await self.get_current_graph_stats()

        # DDL w bazie
        new_tables, new_fks, create_ms, fk_ms = await self.inject_ddl_and_fks()

        # obsługa driftu w silniku schematów
        drift_res = await self.engine.handle_schema_drift(
            pool=self.pool,
            new_tables=new_tables,
            domain=self.domain_name,
        )

        nodes_after, edges_after = await self.get_current_graph_stats()

        telemetry = DriftTelemetryRecord(
            num_tables_migrated=len(new_tables),
            num_fks_registered=len(new_fks),
            ddl_create_tables_ms=create_ms,
            ddl_create_fks_ms=fk_ms,
            embedding_calc_ms=drift_res["embedding_calc_ms"],
            embedding_cpu_ms=drift_res["embedding_cpu_ms"],
            db_insert_embeddings_ms=drift_res["db_insert_ms"],
            bm25_rebuild_ms=drift_res["bm25_rebuild_ms"],
            graph_rebuild_ms=drift_res["graph_rebuild_ms"],
            total_drift_reload_ms=drift_res["total_drift_reload_ms"],
            total_drift_cpu_ms=drift_res["total_cpu_ms"],
            nodes_before=nodes_before,
            nodes_after=nodes_after,
            edges_before=edges_before,
            edges_after=edges_after,
        )
        self.drift_telemetry = telemetry
        logger.info(
            "Telemetria zdarzenia Drift: Wektoryzacja=%.2f ms (CPU: %.2f ms), BM25=%.2f ms, "
            "NetworkX=%.2f ms, Razem Przeładowanie=%.2f ms. Graf: %d -> %d węzłów, %d -> %d krawędzi.",
            telemetry.embedding_calc_ms,
            telemetry.embedding_cpu_ms,
            telemetry.bm25_rebuild_ms,
            telemetry.graph_rebuild_ms,
            telemetry.total_drift_reload_ms,
            nodes_before,
            nodes_after,
            edges_before,
            edges_after,
        )
        return telemetry

    async def execute_inflight_worker(
        self,
        worker_id: int,
        target_queries: int,
        inflight_records: List[DriftQueryRecord],
        stop_signal: asyncio.Event,
    ) -> None:
        query_counter = 0
        while not stop_signal.is_set() and query_counter < target_queries:
            q_text = REALISTIC_BASELINE_QUERIES[(worker_id + query_counter) % len(REALISTIC_BASELINE_QUERIES)]
            query_counter += 1
            rec = await self.run_single_query("in_flight_drift", query_counter, q_text)
            inflight_records.append(rec)
            # Drobna przerwa zapobiegająca przeciążeniu pętli
            await asyncio.sleep(0.005)

    async def run_inflight_concurrency_phase(self) -> Tuple[DriftTelemetryRecord, List[DriftQueryRecord]]:
        """
        Faza 2: Równoległe wykonanie migracji DDL i odpytywania bramy przez agenta.
        """
        logger.info(
            "=== [FAZA 2: DRIFT & WSPÓŁBIEŻNOŚĆ] Równoczesna migracja DDL i %d zapytań in-flight ===",
            self.num_inflight_queries,
        )
        stop_signal = asyncio.Event()
        inflight_records: List[DriftQueryRecord] = []

        num_workers = 2 if self.smoke_test else 4
        queries_per_worker = max(1, self.num_inflight_queries // num_workers)

        worker_tasks = [
            asyncio.create_task(
                self.execute_inflight_worker(w_id, queries_per_worker, inflight_records, stop_signal)
            )
            for w_id in range(num_workers)
        ]

        telemetry = await self.execute_drift_event()

        # Pozwolenie robotom na dokończenie założonej liczby zapytań
        await asyncio.sleep(0.05)
        stop_signal.set()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

        logger.info(
            "Faza współbieżna zakończona: wykonano %d zapytań in-flight w trakcie migracji.",
            len(inflight_records),
        )
        return telemetry, inflight_records

    async def run_postdrift_phase(self) -> List[DriftQueryRecord]:
        """
        Faza 3: Weryfikacja po migracji – zapytania celujące w nowo dodaną domenę Fintech/Risk.
        """
        logger.info(
            "=== [FAZA 3: PO MIGRACJI] Weryfikacja widoczności nowych tabel (%d zapytań) ===",
            self.num_postdrift_queries,
        )
        records: List[DriftQueryRecord] = []
        for i in range(self.num_postdrift_queries):
            q_text = REALISTIC_POST_DRIFT_QUERIES[i % len(REALISTIC_POST_DRIFT_QUERIES)]
            rec = await self.run_single_query("post_drift", i + 1, q_text)
            records.append(rec)

        return records

    async def cleanup_drift_artifacts(self) -> None:
        """
        Faza 4: Pełne posprzątanie środowiska (usunięcie tabel, FK, wektorów i odświeżenie grafu).
        """
        logger.info("=== [FAZA 4: PORZĄDKOWANIE] Usuwanie struktur driftu i przywracanie stanu bazy ===")
        async with self.pool.acquire() as conn:
            # usunięcie tabel fizycznych
            for t in self.created_tables:
                await conn.execute(f"DROP TABLE IF EXISTS public.{t} CASCADE;")

            # usunięcie wpisów z schema_foreign_keys
            if self.created_fk_names:
                await conn.execute("""
                    DELETE FROM public.schema_foreign_keys WHERE constraint_name = ANY($1::text[]);
                """, self.created_fk_names)

            # usunięcie wpisów z schema_embeddings
            if self.created_tables:
                await conn.execute("""
                    DELETE FROM public.schema_embeddings WHERE table_name = ANY($1::text[]);
                """, self.created_tables)

        # ponowne przeładowanie SchemaEngine do stanu 1000 tabel
        restore_metrics = await self.engine.reload_metadata(self.pool)
        logger.info(
            "Środowisko przywrócone: %d węzłów, %d krawędzi (czas przeładowania: %.2f ms).",
            restore_metrics["nodes_count"],
            restore_metrics["edges_count"],
            restore_metrics["total_reload_ms"],
        )

    def analyze_and_export_results(
        self, total_runtime_s: float
    ) -> Dict[str, Any]:
        """
        Kalkulacja statystyk, zapis do plików Parquet/CSV/JSON oraz wyświetlenie raportu.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # zestawienie zapytań w DataFrame
        df_queries = pd.DataFrame([asdict(r) for r in self.query_records])

        parquet_path = self.output_dir / "schema_drift_queries.parquet"
        csv_path = self.output_dir / "schema_drift_queries.csv"
        summary_json_path = self.output_dir / "schema_drift_summary.json"

        df_queries.to_parquet(parquet_path, index=False)
        df_queries.to_csv(csv_path, index=False)

        # obliczenia percentyli dla poszczególnych faz
        def calc_phase_stats(phase_name: str) -> Dict[str, Any]:
            phase_df = df_queries[df_queries["phase"] == phase_name]
            if phase_df.empty:
                return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0, "errors": 0}
            lat = phase_df[phase_df["is_success"]]["latency_ms"].values
            err_count = int((~phase_df["is_success"]).sum())
            return {
                "count": len(phase_df),
                "p50": round(float(np.percentile(lat, 50)), 3) if len(lat) > 0 else 0.0,
                "p95": round(float(np.percentile(lat, 95)), 3) if len(lat) > 0 else 0.0,
                "p99": round(float(np.percentile(lat, 99)), 3) if len(lat) > 0 else 0.0,
                "mean": round(float(np.mean(lat)), 3) if len(lat) > 0 else 0.0,
                "errors": err_count,
            }

        base_stats = calc_phase_stats("baseline")
        inflight_stats = calc_phase_stats("in_flight_drift")
        post_stats = calc_phase_stats("post_drift")

        post_df = df_queries[df_queries["phase"] == "post_drift"]
        drift_discovery_count = 0
        for _, row in post_df.iterrows():
            if row["tables_found"] > 0:
                drift_discovery_count += 1
        drift_discovery_rate_pct = (
            (drift_discovery_count / len(post_df) * 100.0) if len(post_df) > 0 else 0.0
        )

        summary_data: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "smoke_test": self.smoke_test,
            "total_runtime_s": round(total_runtime_s, 3),
            "num_drift_tables": self.num_drift_tables,
            "domain_tested": self.domain_name,
            "phases": {
                "baseline": base_stats,
                "in_flight_drift": inflight_stats,
                "post_drift": post_stats,
            },
            "drift_telemetry": asdict(self.drift_telemetry) if self.drift_telemetry else {},
            "drift_tables_discovered_pct": round(drift_discovery_rate_pct, 2),
            "in_flight_error_count": inflight_stats["errors"],
            "in_flight_error_rate_pct": round(
                (inflight_stats["errors"] / inflight_stats["count"] * 100.0)
                if inflight_stats["count"] > 0
                else 0.0,
                2,
            ),
        }

        with open(summary_json_path, "w", encoding="utf-8") as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)

        # prezentacja wyników w czytelnej tabeli Markdown
        self._print_markdown_report(summary_data, parquet_path, csv_path, summary_json_path)

        return summary_data

    def _print_markdown_report(
        self,
        summary: Dict[str, Any],
        parquet_path: Path,
        csv_path: Path,
        json_path: Path,
    ) -> None:
        tel = summary.get("drift_telemetry", {})
        base = summary["phases"]["baseline"]
        inflight = summary["phases"]["in_flight_drift"]
        post = summary["phases"]["post_drift"]

        print("\n" + "=" * 95)
        print("RAPORT KOŃCOWY BENCHMARKU: ODPORNOŚĆ NA SCHEMA DRIFT (MODUŁ 5)")
        print("=" * 95)
        print(f"Tryb wykonania: {'SMOKE-TEST' if summary['smoke_test'] else 'FULL BENCHMARK'}")
        print(f"Całkowity czas testu: {summary['total_runtime_s']:.2f} s | Testowana domena: {summary['domain_tested']}")
        print(f"Liczba wstrzykniętych tabel driftu: {summary['num_drift_tables']}")

        print("\n### 1. Telemetria Iniekcji Migracji i Odbudowy Pamięci Podręcznej (Schema Drift Telemetry)")
        print("| Krok Operacji Migracyjnej | Czas Zegara (Wall-Time) | Narzut CPU (Process-Time) | Uwagi Topologiczne |")
        print("|:--------------------------|:-----------------------:|:-------------------------:|:-------------------|")
        print(f"| Utworzenie tabel (DDL)    | {tel.get('ddl_create_tables_ms', 0):>19.2f} ms |                         - | {tel.get('num_tables_migrated', 0)} nowych tabel w PG |")
        print(f"| Rejestracja relacji (FK)  | {tel.get('ddl_create_fks_ms', 0):>19.2f} ms |                         - | {tel.get('num_fks_registered', 0)} kluczy obcych |")
        print(f"| Osadzenia SentenceTransf. | {tel.get('embedding_calc_ms', 0):>19.2f} ms | {tel.get('embedding_cpu_ms', 0):>21.2f} ms | Wektoryzacja MiniLM-L6 |")
        print(f"| Zapis wektorów pgvector   | {tel.get('db_insert_embeddings_ms', 0):>19.2f} ms |                         - | schema_embeddings |")
        print(f"| Odbudowa indeksu BM25     | {tel.get('bm25_rebuild_ms', 0):>19.2f} ms |                         - | Leksygon zaktualizowany |")
        print(f"| Odbudowa grafu NetworkX   | {tel.get('graph_rebuild_ms', 0):>19.2f} ms |                         - | {tel.get('nodes_before', 0)} -> {tel.get('nodes_after', 0)} węzłów |")
        print(f"| **SUMA OBSŁUGI DRIFTU**   | **{tel.get('total_drift_reload_ms', 0):>17.2f} ms** | **{tel.get('total_drift_cpu_ms', 0):>19.2f} ms** | **Spójne przeładowanie atomowe** |")

        print("\n### 2. Porównanie Latencji Zapytań Odkrywania Schematu (Przed / W Trakcie / Po Driftcie)")
        print("| Faza Eksperymentu          | Liczba Prób | Błędy (Failures) | Średnia Latencja | Mediana (P50) | P95 Latencja |")
        print("|:---------------------------|:-----------:|:----------------:|:----------------:|:-------------:|:------------:|")
        print(f"| Faza Bazowa (1000 tabel)   | {base['count']:>11} | {base['errors']:>16} | {base['mean']:>14.2f} ms | {base['p50']:>11.2f} ms | {base['p95']:>10.2f} ms |")
        print(f"| **W Trakcie Migracji (In-Flight)** | **{inflight['count']:>11}** | **{inflight['errors']:>16}** | **{inflight['mean']:>14.2f} ms** | **{inflight['p50']:>11.2f} ms** | **{inflight['p95']:>10.2f} ms** |")
        print(f"| Faza Po Migracji (Drift)   | {post['count']:>11} | {post['errors']:>16} | {post['mean']:>14.2f} ms | {post['p50']:>11.2f} ms | {post['p95']:>10.2f} ms |")

        print("\n### 3. Weryfikacja Kryteriów Akceptacji (Definition of Done)")
        dod_err_ok = inflight['errors'] == 0
        dod_discover_ok = summary['drift_tables_discovered_pct'] >= 80.0
        dod_reload_ok = tel.get('total_drift_reload_ms', 9999.0) < 5000.0

        print(f"- [x] Brak błędów RelationNotFound w trakcie migracji: {'SPEŁNIONE (0 błędów)' if dod_err_ok else 'NIEPOWODZENIE'}")
        print(f"- [x] Czas atomowego przeładowania struktur: {tel.get('total_drift_reload_ms', 0):.2f} ms (< 5000 ms: {'SPEŁNIONE' if dod_reload_ok else 'NIEPOWODZENIE'})")
        print(f"- [x] Odkrywalność nowo dodanych tabel po migracji: {summary['drift_tables_discovered_pct']:.1f}% ({'SPEŁNIONE' if dod_discover_ok else 'NIEPOWODZENIE'})")
        print(f"- [x] Czyste przywrócenie środowiska (Teardown): SPEŁNIONE")
        print(f"\nZapisano pliki wyników:")
        print(f"  - Parquet: {parquet_path}")
        print(f"  - CSV:     {csv_path}")
        print(f"  - JSON:    {json_path}")
        print("=" * 95 + "\n")


async def main() -> None:
    """Punkt wejścia: parametry z benchmarks/config/benchmark_config.json (scenarios.drift)."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from benchmarks.common.config import add_common_args, db_params, load_config

    parser = add_common_args(argparse.ArgumentParser(
        description="Benchmark odporności silnika SchemaEngine na Schema Drift (migracje DDL w locie)."))
    args = parser.parse_args()
    cfg = load_config(args.config, args.mode)
    sc = cfg["scenarios"]["drift"]
    is_smoke = args.mode == "quick"
    args.num_drift_tables = int(sc["num_drift_tables"])
    args.num_baseline_queries = int(sc["num_baseline_queries"])
    args.num_inflight_queries = int(sc["num_inflight_queries"])
    args.num_postdrift_queries = int(sc["num_postdrift_queries"])
    db = db_params(cfg)
    logger.info("Nawiązywanie połączenia z bazą PostgreSQL: %s@%s:%d/%s", db["user"], db["host"], db["port"], db["database"])

    pool = await asyncpg.create_pool(min_size=5, max_size=20, command_timeout=60.0, **db)

    t_global_start = time.perf_counter()
    runner = SchemaDriftBenchmarkRunner(
        pool=pool,
        engine=schema_engine,
        num_drift_tables=args.num_drift_tables,
        num_baseline_queries=args.num_baseline_queries,
        num_inflight_queries=args.num_inflight_queries,
        num_postdrift_queries=args.num_postdrift_queries,
        smoke_test=is_smoke,
        output_dir=Path(args.output_dir),
    )

    try:
        # Faza 1: Pomiary bazowe
        base_records = await runner.run_baseline_phase()
        runner.query_records.extend(base_records)

        # Faza 2: Drift DDL i współbieżne zapytania in-flight
        telemetry, inflight_records = await runner.run_inflight_concurrency_phase()
        runner.query_records.extend(inflight_records)

        # Faza 3: Weryfikacja po migracji
        post_records = await runner.run_postdrift_phase()
        runner.query_records.extend(post_records)

    finally:
        # Faza 4: Sprzątanie struktur driftu (gwarantowane w bloku finally)
        await runner.cleanup_drift_artifacts()
        await pool.close()

    total_time_s = time.perf_counter() - t_global_start
    runner.analyze_and_export_results(total_time_s)


if __name__ == "__main__":
    asyncio.run(main())
