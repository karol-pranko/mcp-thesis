#!/usr/bin/env python3
"""
Głodzenie puli połączeń przez snapshoty (TransactionSnapshotManager).

Pula asyncpg max_size = 10, M w {5, 10, 15, 20} agentów. Każdy otwiera snapshot
(REPEATABLE READ + pg_export_snapshot, TTL = 300 s) i "myśli" 2 s, jak model między krokami.
Każdy snapshot trzyma jedno połączenie, więc przy M > 10 reszta czeka albo dostaje timeout.

Mierzymy: czas pobrania połączenia z puli, liczbę/odsetek timeoutów oraz liczbę sesji
'idle in transaction' w pg_stat_activity.

Wyniki: benchmarks/raw_results/snapshot_pool_starvation{.parquet,.csv,_summary.csv}
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
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import numpy as np
import pandas as pd

# Zapewnienie dostępności modułów nadrzędnych
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mcp_server.pagination import (
    ActiveSnapshot,
    SnapshotExpiredError,
    TransactionSnapshotManager,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("bench_snapshot_pool_starvation")


class PoolTimeoutError(asyncio.TimeoutError):
    """Wyjątek oznaczający wyczerpanie puli połączeń bazy danych (Pool Starvation)."""

    pass


@dataclass
class SnapshotStarvationSample:
    concurrency_m: int
    agent_id: int
    pool_acquire_latency_ms: float
    pool_timeout: bool
    idle_in_transaction_count: int
    think_time_s: float
    pool_max_size: int
    success: bool
    snapshot_id: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class SnapshotStarvationSummary:
    concurrency_m: int
    pool_max_size: int
    total_requests: int
    successful_requests: int
    timeout_requests: int
    pool_timeout_rate_pct: float
    mean_acquire_latency_ms: float
    p50_acquire_latency_ms: float
    p95_acquire_latency_ms: float
    p99_acquire_latency_ms: float
    max_idle_in_tx: int


class SnapshotPoolStarvationBenchmark:
    """
    Silnik badawczy testujący odporność puli połączeń asyncpg (max_size = 10)
    na blokowanie połączeń przez transakcje snapshotowe TransactionSnapshotManager.
    """

    def __init__(
        self,
        db_host: str = os.getenv("POSTGRES_HOST", "localhost"),
        db_port: int = int(os.getenv("POSTGRES_PORT", "5432")),
        db_user: str = os.getenv("POSTGRES_USER", "mcp_user"),
        db_pass: str = os.getenv("POSTGRES_PASSWORD", "mcp_password"),
        db_name: str = os.getenv("POSTGRES_DB", "analytics_db"),
        m_values: Optional[List[int]] = None,
        pool_max_size: int = 10,
        think_time_s: float = 2.0,
        acquire_timeout_s: float = 1.0,
        is_dry_run: bool = False,
    ) -> None:
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name

        self.pool_max_size = pool_max_size
        self.is_dry_run = is_dry_run

        if self.is_dry_run:
            self.m_values = [2, 5]
            self.think_time_s = 0.05
            self.acquire_timeout_s = 0.10
        else:
            self.m_values = m_values or [5, 10, 15, 20]
            self.think_time_s = think_time_s
            self.acquire_timeout_s = acquire_timeout_s

    async def _create_dedicated_pool(self) -> asyncpg.Pool:
        """Tworzy dedykowaną pulę połączeń asyncpg o zadanym stałym limicie max_size = 10."""
        try:
            return await asyncpg.create_pool(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_pass,
                database=self.db_name,
                min_size=2,
                max_size=self.pool_max_size,
                command_timeout=15.0,
            )
        except asyncpg.exceptions.InvalidPasswordError:
            fallback = "mcp_secret_password" if self.db_pass == "mcp_password" else "mcp_password"
            return await asyncpg.create_pool(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=fallback,
                database=self.db_name,
                min_size=2,
                max_size=self.pool_max_size,
                command_timeout=15.0,
            )

    async def _create_monitor_connection(self) -> asyncpg.Connection:
        try:
            return await asyncpg.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_pass,
                database=self.db_name,
            )
        except asyncpg.exceptions.InvalidPasswordError:
            fallback = "mcp_secret_password" if self.db_pass == "mcp_password" else "mcp_password"
            return await asyncpg.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=fallback,
                database=self.db_name,
            )

    async def _monitor_idle_in_tx(
        self,
        monitor_conn: asyncpg.Connection,
        stop_event: asyncio.Event,
        peak_container: List[int],
    ) -> None:
        """Asynchroniczna sonda próbkująca liczbę sesji w stanie 'idle in transaction' co 50 ms."""
        query = (
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE state = 'idle in transaction' AND usename = $1;"
        )
        while not stop_event.is_set():
            try:
                cnt = await monitor_conn.fetchval(query, self.db_user)
                if cnt is not None and cnt > peak_container[0]:
                    peak_container[0] = int(cnt)
            except Exception as exc:
                logger.debug("Błąd monitora pg_stat_activity: %s", exc)
            await asyncio.sleep(0.05)

    async def _agent_worker(
        self,
        agent_id: int,
        concurrency_m: int,
        pool: asyncpg.Pool,
        manager: TransactionSnapshotManager,
        peak_container: List[int],
    ) -> SnapshotStarvationSample:
        """
        Emuluje pojedynczy proces agentowy:
        1. Próbuje pobrać połączenie i otworzyć snapshot (REPEATABLE READ + pg_export_snapshot).
        2. Mierzy opóźnienie pobrania połączenia z puli (pool_acquire_latency_ms).
        3. Emuluje czas wnioskowania modelu LLM (t_think = 2.0 s).
        4. Zwalnia snapshot i zwraca połączenie do puli.
        """
        t0 = time.perf_counter()
        snapshot_id: Optional[str] = None
        is_timeout = False
        error_msg: Optional[str] = None
        success = False

        try:
            snapshot_id = await manager.create_snapshot(
                pool=pool,
                ttl_seconds=300.0,
                table_name="analytics_orders",
                acquire_timeout=self.acquire_timeout_s,
            )
            lat_ms = (time.perf_counter() - t0) * 1000.0

            # Emulacja czasu wnioskowania LLM (blokada połączenia w puli)
            await asyncio.sleep(self.think_time_s)
            success = True

        except (asyncio.TimeoutError, Exception) as exc:
            lat_ms = (time.perf_counter() - t0) * 1000.0
            error_str = str(exc).lower()
            if isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or "timeout" in error_str:
                is_timeout = True
                error_msg = f"PoolTimeoutError: przekroczono limit czasu puli ({self.acquire_timeout_s:.2f} s)"
            else:
                error_msg = f"{type(exc).__name__}: {exc}"

        finally:
            if snapshot_id:
                try:
                    await manager.release_snapshot(snapshot_id)
                except Exception as rel_exc:
                    logger.debug("Błąd zwalniania snapshotu %s: %s", snapshot_id, rel_exc)

        return SnapshotStarvationSample(
            concurrency_m=concurrency_m,
            agent_id=agent_id,
            pool_acquire_latency_ms=round(lat_ms, 3),
            pool_timeout=is_timeout,
            idle_in_transaction_count=peak_container[0],
            think_time_s=self.think_time_s,
            pool_max_size=self.pool_max_size,
            success=success,
            snapshot_id=snapshot_id,
            error_message=error_msg,
        )

    async def run_benchmark(self, output_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
        output_dir.mkdir(parents=True, exist_ok=True)
        t_global_start = time.perf_counter()

        logger.info(
            "════════════════════════════════════════════════════════════════════════"
        )
        logger.info(
            "START: Test obciążeniowy puli snapshotów (Snapshot Pool Starvation)"
        )
        logger.info(
            "  Poziomy współbieżności M: %s, max_size puli: %d, t_think: %.2f s, timeout: %.2f s",
            self.m_values,
            self.pool_max_size,
            self.think_time_s,
            self.acquire_timeout_s,
        )
        logger.info(
            "════════════════════════════════════════════════════════════════════════"
        )

        all_samples: List[SnapshotStarvationSample] = []
        summaries: List[SnapshotStarvationSummary] = []

        monitor_conn = await self._create_monitor_connection()

        try:
            for m in self.m_values:
                logger.info("\n>>> Badanie poziomu współbieżności M = %d agentów...", m)
                pool = await self._create_dedicated_pool()
                manager = TransactionSnapshotManager()

                stop_event = asyncio.Event()
                peak_container = [0]
                monitor_task = asyncio.create_task(
                    self._monitor_idle_in_tx(monitor_conn, stop_event, peak_container)
                )

                # Równoległe uruchomienie M agentów
                workers = [
                    self._agent_worker(
                        agent_id=i + 1,
                        concurrency_m=m,
                        pool=pool,
                        manager=manager,
                        peak_container=peak_container,
                    )
                    for i in range(m)
                ]

                batch_samples = await asyncio.gather(*workers)
                stop_event.set()
                await monitor_task

                await manager.close_all()
                await pool.close()

                all_samples.extend(batch_samples)

                latencies = [s.pool_acquire_latency_ms for s in batch_samples]
                timeouts = [s for s in batch_samples if s.pool_timeout]
                successes = [s for s in batch_samples if s.success]

                mean_lat = float(np.mean(latencies)) if latencies else 0.0
                p50_lat = float(np.percentile(latencies, 50)) if latencies else 0.0
                p95_lat = float(np.percentile(latencies, 95)) if latencies else 0.0
                p99_lat = float(np.percentile(latencies, 99)) if latencies else 0.0
                timeout_rate = (len(timeouts) / float(m)) * 100.0

                summary = SnapshotStarvationSummary(
                    concurrency_m=m,
                    pool_max_size=self.pool_max_size,
                    total_requests=m,
                    successful_requests=len(successes),
                    timeout_requests=len(timeouts),
                    pool_timeout_rate_pct=round(timeout_rate, 2),
                    mean_acquire_latency_ms=round(mean_lat, 2),
                    p50_acquire_latency_ms=round(p50_lat, 2),
                    p95_acquire_latency_ms=round(p95_lat, 2),
                    p99_acquire_latency_ms=round(p99_lat, 2),
                    max_idle_in_tx=peak_container[0],
                )
                summaries.append(summary)

                logger.info(
                    "M=%2d: Sukcesy=%d/%d, Timeouty=%d (%.1f%%), Śr. latency=%.1f ms, P95=%.1f ms, Szczyt idle_in_tx=%d",
                    m,
                    len(successes),
                    m,
                    len(timeouts),
                    timeout_rate,
                    mean_lat,
                    p95_lat,
                    peak_container[0],
                )

        finally:
            await monitor_conn.close()

        df_raw = pd.DataFrame([asdict(s) for s in all_samples])
        df_summary = pd.DataFrame([asdict(sm) for sm in summaries])

        parquet_path = output_dir / "snapshot_pool_starvation.parquet"
        csv_path = output_dir / "snapshot_pool_starvation.csv"
        summary_path = output_dir / "snapshot_pool_starvation_summary.csv"

        df_raw.to_parquet(parquet_path, index=False)
        df_raw.to_csv(csv_path, index=False)
        df_summary.to_csv(summary_path, index=False)

        total_elapsed = time.perf_counter() - t_global_start
        logger.info(
            "\n════════════════════════════════════════════════════════════════════════"
        )
        logger.info(
            "ZAKOŃCZONO TEST SNAPSHOTÓW W CZASIE %.2f s", total_elapsed
        )
        logger.info("Zapisano surowe próbki:   %s oraz %s", parquet_path, csv_path)
        logger.info("Zapisano podsumowanie:    %s", summary_path)
        logger.info(
            "════════════════════════════════════════════════════════════════════════"
        )

        print("\n=== PODSUMOWANIE GŁODZENIA PULI POŁĄCZEŃ (SNAPSHOT POOL STARVATION) ===")
        print(df_summary.to_string(index=False))

        return df_raw, df_summary


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test obciążeniowy puli połączeń TransactionSnapshotManager w PostgreSQL."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(PROJECT_ROOT / "benchmarks" / "raw_results"),
        help="Katalog zapisu wyników.",
    )
    parser.add_argument(
        "--pool-max-size",
        type=int,
        default=10,
        help="Maksymalny rozmiar puli asyncpg (domyślnie: 10).",
    )
    parser.add_argument(
        "--think-time",
        type=float,
        default=2.0,
        help="Czas wnioskowania LLM emulowany w transakcji [s] (domyślnie: 2.0).",
    )
    parser.add_argument(
        "--acquire-timeout",
        type=float,
        default=1.0,
        help="Limit czasu pobrania połączenia z puli [s] (domyślnie: 1.0).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Szybki bieg testowy (M=[2, 5], think_time=0.05s, timeout=0.1s).",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Synonim dla --dry-run.",
    )
    parser.add_argument("--db-host", type=str, default=os.getenv("POSTGRES_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")))
    parser.add_argument("--db-user", type=str, default=os.getenv("POSTGRES_USER", "mcp_user"))
    parser.add_argument("--db-pass", type=str, default=os.getenv("POSTGRES_PASSWORD", "mcp_password"))
    parser.add_argument("--db-name", type=str, default=os.getenv("POSTGRES_DB", "analytics_db"))

    return parser.parse_args()


async def main() -> None:
    args = parse_arguments()
    is_dry = args.dry_run or args.smoke_test

    bench = SnapshotPoolStarvationBenchmark(
        db_host=args.db_host,
        db_port=args.db_port,
        db_user=args.db_user,
        db_pass=args.db_pass,
        db_name=args.db_name,
        pool_max_size=args.pool_max_size,
        think_time_s=args.think_time,
        acquire_timeout_s=args.acquire_timeout,
        is_dry_run=is_dry,
    )

    await bench.run_benchmark(output_dir=Path(args.output_dir))


if __name__ == "__main__":
    asyncio.run(main())
