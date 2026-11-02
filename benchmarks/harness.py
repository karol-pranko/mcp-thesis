#!/usr/bin/env python3
"""
Wspólna "uprząż" pomiarowa: rozgrzewka, N powtórzeń, czas (latency_ms), tokeny (cl100k_base),
rozmiar ładunku w bajtach oraz CPU/RAM z psutil. Wyniki eksportowane do JSON i CSV.
"""

import argparse
import asyncio
from dataclasses import asdict, dataclass, field
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import psutil
import tiktoken


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("benchmark_harness")


@dataclass
class BenchmarkResult:
    """Jeden pomiar (zagregowany po iteracjach) + surowe próbki w raw_samples."""

    test_name: str
    approach: str
    latency_ms: float
    token_count: int
    payload_bytes: int
    cpu_usage_pct: float
    ram_usage_mb: float
    success: bool
    error_message: Optional[str] = None
    extra_metadata: Dict[str, Any] = field(default_factory=dict)
    std_dev: float = 0.0
    variance: float = 0.0
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    raw_samples: List[float] = field(default_factory=list)
    engine_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_flat_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if "extra_metadata" in data and isinstance(data["extra_metadata"], dict):
            data["extra_metadata"] = json.dumps(data["extra_metadata"], ensure_ascii=False)
        if "raw_samples" in data and isinstance(data["raw_samples"], list):
            data["raw_samples"] = json.dumps(data["raw_samples"])
        return data


class SystemResourceProfiler:
    """CPU i RSS procesu razem z wszystkimi potomkami (np. pulą procesów AST)."""

    def __init__(self, target_pid: Optional[int] = None) -> None:
        self.target_pid = target_pid or os.getpid()
        try:
            self.process = psutil.Process(self.target_pid)
        except psutil.NoSuchProcess as exc:
            logger.error("Nie odnaleziono procesu o PID %d: %s", self.target_pid, exc)
            raise

        self._start_time: float = 0.0
        self._start_ram_bytes: int = 0

    def start_profiling(self) -> None:
        self._start_time = time.perf_counter()
        try:
            # pierwsze cpu_percent(None) zawsze zwraca 0 i tylko ustawia punkt odniesienia
            self.process.cpu_percent(interval=None)
            self._start_ram_bytes = self.process.memory_info().rss

            for child in self.process.children(recursive=True):
                try:
                    child.cpu_percent(interval=None)
                    self._start_ram_bytes += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
            logger.warning("Błąd odczytu parametrów początkowych procesu: %s", exc)

    def stop_profiling(self) -> Tuple[float, float]:
        """Zwraca (cpu_%, ram_MB). CPU liczone od start_profiling, RAM to stan na teraz."""
        total_cpu_pct = 0.0
        total_ram_bytes = 0

        try:
            total_cpu_pct += self.process.cpu_percent(interval=None)
            total_ram_bytes += self.process.memory_info().rss

            for child in self.process.children(recursive=True):
                try:
                    total_cpu_pct += child.cpu_percent(interval=None)
                    total_ram_bytes += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
            logger.warning("Błąd odczytu parametrów końcowych procesu: %s", exc)

        ram_usage_mb = total_ram_bytes / (1024.0 * 1024.0)
        return round(total_cpu_pct, 2), round(ram_usage_mb, 2)


class BenchmarkHarness:
    def __init__(self, target_pid: Optional[int] = None) -> None:
        self.profiler = SystemResourceProfiler(target_pid=target_pid)
        self.tokenizer = tiktoken.get_encoding("cl100k_base")
        self.results: List[BenchmarkResult] = []

    def count_tokens(self, payload: Any) -> int:
        """
        Wylicza dokładną liczbę tokenów dla danego ładunku za pomocą enkodera cl100k_base.
        Obsługuje obiekty tekstowe, słowniki i listy (poprzez serializację JSON).
        """
        if payload is None:
            return 0

        if isinstance(payload, str):
            text_to_encode = payload
        elif isinstance(payload, (dict, list)):
            text_to_encode = json.dumps(payload, ensure_ascii=False)
        else:
            text_to_encode = str(payload)

        try:
            return len(self.tokenizer.encode(text_to_encode))
        except Exception as exc:
            logger.warning("Błąd tokenizacji ładunku: %s. Przyjęto estymację heurystyczną.", exc)
            return max(1, len(text_to_encode) // 4)

    def calculate_payload_bytes(self, payload: Any) -> int:
        if payload is None:
            return 0

        if isinstance(payload, bytes):
            return len(payload)
        elif isinstance(payload, str):
            return len(payload.encode("utf-8"))
        elif isinstance(payload, (dict, list)):
            return len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        else:
            return len(str(payload).encode("utf-8"))

    async def run_scenario(
        self,
        test_name: str,
        approach: str,
        scenario_fn: Callable[[], Awaitable[Any]],
        extra_metadata: Optional[Dict[str, Any]] = None,
        iterations: int = 100,
        warmup_runs: int = 5,
    ) -> BenchmarkResult:
        """
        warmup_runs przebiegów bez pomiaru (zimna pula, cache planów), potem iterations pomiarów.
        Tokeny i bajty liczymy z wyniku ostatniej iteracji (tylko gdy wszystkie przeszły).
        """
        metadata = dict(extra_metadata) if extra_metadata else {}
        logger.info(
            "Rozpoczęcie scenariusza '%s' (podejście: '%s', powtórzenia: %d, warmup: %d)...",
            test_name,
            approach,
            iterations,
            warmup_runs,
        )

        success = True
        error_message: Optional[str] = None

        if warmup_runs > 0:
            logger.info(
                "Wykonywanie %d biegów rozgrzewkowych (warmup) dla scenariusza '%s'...",
                warmup_runs,
                test_name,
            )
            for w_idx in range(warmup_runs):
                try:
                    await scenario_fn()
                except Exception as exc:
                    success = False
                    error_message = f"Błąd w fazie rozgrzewkowej (warmup): {exc}"
                    logger.warning(
                        "Błąd podczas biegu rozgrzewkowego '%s' (próba %d/%d): %s",
                        test_name,
                        w_idx + 1,
                        warmup_runs,
                        exc,
                    )
                    break

        raw_latencies: List[float] = []
        payload: Any = None

        if success:
            self.profiler.start_profiling()

            for it in range(iterations):
                start_ns = time.perf_counter_ns()
                try:
                    payload = await scenario_fn()
                    end_ns = time.perf_counter_ns()
                    sample_ms = (end_ns - start_ns) / 1_000_000.0
                    raw_latencies.append(round(sample_ms, 3))
                except Exception as exc:
                    end_ns = time.perf_counter_ns()
                    sample_ms = (end_ns - start_ns) / 1_000_000.0
                    raw_latencies.append(round(sample_ms, 3))
                    success = False
                    error_message = str(exc)
                    logger.error(
                        "Scenariusz '%s' zakończył się błędem w iteracji %d/%d: %s",
                        test_name,
                        it + 1,
                        iterations,
                        exc,
                        exc_info=True,
                    )
                    break

            cpu_usage_pct, ram_usage_mb = self.profiler.stop_profiling()
        else:
            cpu_usage_pct, ram_usage_mb = 0.0, 0.0

        if raw_latencies:
            latency_ms = round(float(np.mean(raw_latencies)), 3)
            p50_latency_ms = round(float(np.percentile(raw_latencies, 50)), 3)
            p95_latency_ms = round(float(np.percentile(raw_latencies, 95)), 3)
            p99_latency_ms = round(float(np.percentile(raw_latencies, 99)), 3)
            if len(raw_latencies) > 1:
                std_dev = round(float(np.std(raw_latencies, ddof=1)), 3)
                variance = round(float(np.var(raw_latencies, ddof=1)), 3)
            else:
                std_dev = 0.0
                variance = 0.0
        else:
            latency_ms = 0.0
            p50_latency_ms = 0.0
            p95_latency_ms = 0.0
            p99_latency_ms = 0.0
            std_dev = 0.0
            variance = 0.0

        token_count = self.count_tokens(payload) if success else 0
        payload_bytes = self.calculate_payload_bytes(payload) if success else 0

        result = BenchmarkResult(
            test_name=test_name,
            approach=approach,
            latency_ms=latency_ms,
            token_count=token_count,
            payload_bytes=payload_bytes,
            cpu_usage_pct=cpu_usage_pct,
            ram_usage_mb=ram_usage_mb,
            success=success,
            error_message=error_message,
            extra_metadata=metadata,
            std_dev=std_dev,
            variance=variance,
            p50_latency_ms=p50_latency_ms,
            p95_latency_ms=p95_latency_ms,
            p99_latency_ms=p99_latency_ms,
            raw_samples=raw_latencies,
        )

        self.results.append(result)
        logger.info(
            "Zakończono scenariusz '%s' [%s] (%d powtórzeń): śr = %.2f ms (std = %.2f, p50 = %.2f, p95 = %.2f, p99 = %.2f ms), tokeny = %d, rozmiar = %d B, CPU = %.1f%%, RAM = %.1f MB, status = %s",
            test_name,
            approach,
            len(raw_latencies),
            result.latency_ms,
            result.std_dev,
            result.p50_latency_ms,
            result.p95_latency_ms,
            result.p99_latency_ms,
            result.token_count,
            result.payload_bytes,
            result.cpu_usage_pct,
            result.ram_usage_mb,
            "SUKCES" if result.success else "BŁĄD",
        )

        return result

    def export_results(self, output_dir: str = "results") -> Tuple[Path, Path]:
        """Zapisuje benchmark_results.json i benchmark_results.csv do output_dir."""
        target_dir = Path(output_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        json_file_path = target_dir / "benchmark_results.json"
        csv_file_path = target_dir / "benchmark_results.csv"

        serialized_results = [r.to_dict() for r in self.results]
        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(serialized_results, json_file, ensure_ascii=False, indent=2)
        logger.info("Zapisano %d wyników pomiarów w formacie JSON: %s", len(self.results), json_file_path)

        # w CSV słowniki/listy lądują jako JSON w jednej kolumnie (to_flat_dict)
        flat_records = [r.to_flat_dict() for r in self.results]
        dataframe = pd.DataFrame(flat_records)
        dataframe.to_csv(csv_file_path, index=False, encoding="utf-8")
        logger.info("Zapisano %d wyników pomiarów w formacie CSV: %s", len(self.results), csv_file_path)

        return json_file_path, csv_file_path

    def clear_results(self) -> None:
        self.results.clear()

    async def verify_sql_in_postgres(
        self,
        pool_or_conn: Any,
        sql_query: str,
        ground_truth_sql: str,
    ) -> Tuple[bool, bool, Optional[str]]:
        return await verify_sql_in_postgres(pool_or_conn, sql_query, ground_truth_sql)


async def verify_sql_in_postgres(
    pool_or_conn: Any,
    sql_query: str,
    ground_truth_sql: str,
) -> Tuple[bool, bool, Optional[str]]:
    """
    Zwraca (valid_sql, exec_match, błąd).

    valid_sql - EXPLAIN i wykonanie przeszły bez wyjątku.
    exec_match - wiersze identyczne z GT, w tej samej kolejności (porównanie list słowników).
    Jeśli GT zwraca 0 wierszy, próbka jest odrzucana (VACUOUS_TRUTH_REJECTED) - pusty wynik
    "zgadza się" z każdym zapytaniem, które też nic nie zwraca, więc nic by nie mierzył.
    """
    valid_sql = False
    exec_acc = False
    error_msg = None

    is_pool = hasattr(pool_or_conn, "acquire")
    conn = None
    acq = None
    if is_pool:
        acq = pool_or_conn.acquire()
        conn = await acq if hasattr(acq, "__await__") else await acq.__aenter__()
    else:
        conn = pool_or_conn

    try:
        await conn.execute(f"EXPLAIN {sql_query}")
        test_rows = await conn.fetch(sql_query)
        valid_sql = True

        gt_rows = await conn.fetch(ground_truth_sql)

        if len(gt_rows) == 0:
            exec_acc = False
            error_msg = "VACUOUS_TRUTH_REJECTED: Ground truth query returned 0 rows (invalid evaluation sample)"
            return valid_sql, exec_acc, error_msg

        test_dicts = [dict(r) for r in test_rows]
        gt_dicts = [dict(r) for r in gt_rows]

        if test_dicts == gt_dicts:
            exec_acc = True
        else:
            exec_acc = False
            error_msg = f"SEMANTIC_MISMATCH: Model returned {len(test_rows)} rows, Ground Truth returned {len(gt_rows)} rows"

    except Exception as exc:
        error_msg = str(exc)
    finally:
        if is_pool and conn is not None and acq is not None:
            if hasattr(acq, "__aexit__"):
                await acq.__aexit__(None, None, None)
            elif hasattr(pool_or_conn, "release"):
                await pool_or_conn.release(conn)

    return valid_sql, exec_acc, error_msg


# demo: python benchmarks/harness.py --scenario ...
if __name__ == "__main__":
    async def main() -> None:
        parser = argparse.ArgumentParser(description="MCP Benchmark Harness CLI.")
        parser.add_argument(
            "--scenario",
            type=str,
            default="demo",
            choices=[
                "demo",
                "snapshot_pool_starvation",
                "schema_quality",
                "pagination",
                "concurrency",
                "oob_crossover",
                "security",
                "all",
            ],
            help="Scenariusz benchmarkowy do uruchomienia.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Szybki bieg testowy (dry-run / smoke-test).",
        )
        parser.add_argument(
            "--pagination-only",
            action="store_true",
            help="Uruchom wyłącznie rozszerzony benchmark paginacji (równoważne --scenario pagination).",
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            default=str(Path(__file__).resolve().parent / "raw_results"),
            help="Katalog zapisu wyników.",
        )
        args = parser.parse_args()
        if args.pagination_only:
            args.scenario = "pagination"

        output_path = Path(args.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        if args.scenario == "snapshot_pool_starvation":
            logger.info("Uruchamianie zarejestrowanego scenariusza: snapshot_pool_starvation...")
            from benchmarks.scenarios.bench_snapshot_pool_starvation import (
                SnapshotPoolStarvationBenchmark,
            )
            bench = SnapshotPoolStarvationBenchmark(is_dry_run=args.dry_run)
            await bench.run_benchmark(output_dir=output_path)
            return

        elif args.scenario == "demo":
            logger.info("Uruchamianie testowej demonstracji silnika pomiarowego BenchmarkHarness...")
            harness = BenchmarkHarness()

            # naiwnie: cały duży ładunek naraz
            async def scenario_naive() -> Dict[str, Any]:
                await asyncio.sleep(0.05)
                return {
                    "records": [
                        {"id": idx, "order_name": f"ORD_{idx:05d}", "amount": 100.5 * idx}
                        for idx in range(250)
                    ],
                    "description": "Naiwne pobranie pełnej tabeli bazodanowej bez paginacji",
                }

            # MCP: jedna strona
            async def scenario_optimized() -> Dict[str, Any]:
                await asyncio.sleep(0.02)
                return {
                    "records": [
                        {"id": idx, "order_name": f"ORD_{idx:05d}", "amount": 100.5 * idx}
                        for idx in range(25)
                    ],
                    "next_cursor": "eyJsYXN0X2lkIjoyNX0=",
                    "has_more": True,
                }

            await harness.run_scenario(
                test_name="pobieranie_zamowien",
                approach="BASELINE_NAIVE",
                scenario_fn=scenario_naive,
                extra_metadata={"batch_size": 250},
            )

            await harness.run_scenario(
                test_name="pobieranie_zamowien",
                approach="MCP_OPTIMIZED",
                scenario_fn=scenario_optimized,
                extra_metadata={"batch_size": 25, "cursor_enabled": True},
            )

            json_path, csv_path = harness.export_results(output_dir=str(output_path))
            print("\n--- Podsumowanie weryfikacji silnika pomiarowego ---")
            print(f"Wygenerowano plik JSON: {json_path}")
            print(f"Wygenerowano plik CSV:  {csv_path}")
            print(f"Liczba pomiarów:       {len(harness.results)}")
            for res in harness.results:
                print(
                    f" - [{res.approach}] czas: {res.latency_ms:.2f} ms | tokeny: {res.token_count} | bajty: {res.payload_bytes} B | RAM: {res.ram_usage_mb:.1f} MB"
                )

        elif args.scenario in ("all", "pagination", "concurrency", "oob_crossover", "security", "schema_quality"):
            logger.info("Wywołanie scenariusza '%s' w harness.py...", args.scenario)
            if args.scenario in ("snapshot_pool_starvation", "all"):
                from benchmarks.scenarios.bench_snapshot_pool_starvation import (
                    SnapshotPoolStarvationBenchmark,
                )
                b = SnapshotPoolStarvationBenchmark(is_dry_run=args.dry_run)
                await b.run_benchmark(output_dir=output_path)

            if args.scenario in ("pagination", "all"):
                from benchmarks.scenarios.bench_pagination_advanced import (
                    run_pagination_benchmark,
                )
                await run_pagination_benchmark(
                    output_dir=output_path,
                    is_dry_run=args.dry_run,
                    pagination_only=args.pagination_only,
                )

            if args.scenario in ("concurrency", "all"):
                from benchmarks.scenarios.bench_concurrency import ConcurrencyBenchmark
                b_conc = ConcurrencyBenchmark(is_dry_run=args.dry_run)
                await b_conc.run_benchmark(output_dir=output_path)

            if args.scenario in ("oob_crossover", "all"):
                from benchmarks.scenarios.bench_oob_crossover import OobCrossoverBenchmark
                b_oob = OobCrossoverBenchmark(is_dry_run=args.dry_run)
                b_oob.run_benchmark(output_dir=output_path)

            if args.scenario in ("security", "all"):
                from benchmarks.scenarios.bench_security_suite import (
                    SecurityBenchmarkSuiteRunner,
                    SecurityTestSuiteGenerator,
                )
                sec_gen = SecurityTestSuiteGenerator()
                if args.dry_run:
                    v = sec_gen.generate_500_analytical_queries()[:5] + sec_gen.generate_250_injection_queries()[:5] + sec_gen.generate_250_prompt_injection_vectors()[:5]
                else:
                    v = sec_gen.generate_500_analytical_queries() + sec_gen.generate_250_injection_queries() + sec_gen.generate_250_prompt_injection_vectors()
                sec_runner = SecurityBenchmarkSuiteRunner(v)
                sec_runner.run_suite(output_dir=output_path)

    asyncio.run(main())
