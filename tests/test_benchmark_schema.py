#!/usr/bin/env python3
"""Precision@K, Recall@K, TRR, zestaw zapytań testowych i liczenie SSF w SchemaBenchmark."""

from pathlib import Path
import sys
from typing import List

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import pytest
from benchmarks.benchmark_schema import (
    BENCHMARK_QUERIES,
    BenchmarkQuery,
    calculate_precision_at_k,
    calculate_recall_at_k,
    calculate_token_reduction_ratio,
    SchemaBenchmark,
)
from benchmarks.harness import BenchmarkHarness, BenchmarkResult


class TestQualityMetrics:
    def test_precision_at_k_exact_match(self):
        retrieved = ["t1", "t2", "t3", "t4", "t5"]
        required = ["t1", "t2"]
        # 2 trafienia na 5
        assert calculate_precision_at_k(retrieved, required, k=5) == pytest.approx(0.40)

    def test_precision_at_k_partial_match(self):
        retrieved = ["t1", "t99", "t98", "t97", "t96"]
        required = ["t1", "t2"]
        assert calculate_precision_at_k(retrieved, required, k=5) == pytest.approx(0.20)

    def test_precision_at_k_no_match(self):
        retrieved = ["t99", "t98", "t97", "t96", "t95"]
        required = ["t1", "t2"]
        assert calculate_precision_at_k(retrieved, required, k=5) == 0.0

    def test_precision_at_k_edge_cases(self):
        assert calculate_precision_at_k([], ["t1"], k=5) == 0.0
        assert calculate_precision_at_k(["t1"], [], k=5) == 0.0
        assert calculate_precision_at_k(["t1"], ["t1"], k=0) == 0.0
        assert calculate_precision_at_k(["t1"], ["t1"], k=-1) == 0.0

    def test_recall_at_k_exact_match(self):
        retrieved = ["t1", "t2", "t3", "t4", "t5"]
        required = ["t1", "t2"]
        assert calculate_recall_at_k(retrieved, required, k=5) == pytest.approx(1.0)

    def test_recall_at_k_partial_match(self):
        retrieved = ["t1", "t99", "t98", "t97", "t96"]
        required = ["t1", "t2"]
        assert calculate_recall_at_k(retrieved, required, k=5) == pytest.approx(0.50)

    def test_recall_at_k_empty_required(self):
        assert calculate_recall_at_k(["t1"], [], k=5) == 1.0


class TestTokenMetrics:
    def test_token_reduction_ratio_standard(self):
        assert calculate_token_reduction_ratio(1000, 100) == pytest.approx(90.0)
        assert calculate_token_reduction_ratio(100000, 1000) == pytest.approx(99.0)

    def test_token_reduction_ratio_zero_or_negative(self):
        assert calculate_token_reduction_ratio(0, 100) == 0.0
        assert calculate_token_reduction_ratio(50, 100) == 0.0


class TestBenchmarkQueries:
    def test_queries_count_and_uniqueness(self):
        assert len(BENCHMARK_QUERIES) == 10
        query_ids = [q.query_id for q in BENCHMARK_QUERIES]
        assert len(set(query_ids)) == 10

    def test_queries_structure(self):
        for query in BENCHMARK_QUERIES:
            assert isinstance(query, BenchmarkQuery)
            assert len(query.natural_query.strip()) > 10
            assert len(query.required_tables) >= 2
            assert len(query.description.strip()) > 5
            for tbl in query.required_tables:
                assert tbl.startswith(("finance_", "telemetry_", "crm_", "inventory_", "hr_"))


class TestScalingAnalysis:
    def test_scaling_calculation_logic(self):
        harness = BenchmarkHarness()
        benchmark = SchemaBenchmark(pool=None, harness=harness, k=5)

        # baseline: tokeny rosną z liczbą tabel
        benchmark.baseline_results = {
            10: BenchmarkResult(
                test_name="baseline_10",
                approach="BASELINE_NAIVE",
                latency_ms=10.0,
                token_count=1000,
                payload_bytes=4000,
                cpu_usage_pct=1.0,
                ram_usage_mb=100.0,
                success=True,
            ),
            100: BenchmarkResult(
                test_name="baseline_100",
                approach="BASELINE_NAIVE",
                latency_ms=25.0,
                token_count=10000,
                payload_bytes=40000,
                cpu_usage_pct=2.0,
                ram_usage_mb=102.0,
                success=True,
            ),
            1000: BenchmarkResult(
                test_name="baseline_1000",
                approach="BASELINE_NAIVE",
                latency_ms=120.0,
                token_count=100000,
                payload_bytes=400000,
                cpu_usage_pct=5.0,
                ram_usage_mb=105.0,
                success=True,
            ),
        }

        # JIT: stała liczba tokenów niezależnie od N
        benchmark.mcp_results = [
            BenchmarkResult(
                test_name=f"mcp_{i}",
                approach="MCP_JIT",
                latency_ms=15.0,
                token_count=500,
                payload_bytes=2000,
                cpu_usage_pct=1.0,
                ram_usage_mb=110.0,
                success=True,
            )
            for i in range(10)
        ]

        scaling_rows = benchmark.calculate_scaling_analysis()
        assert len(scaling_rows) == 3

        # N=10 to punkt odniesienia (SSF = 1)
        row_10 = scaling_rows[0]
        assert row_10["table_count"] == 10
        assert row_10["baseline_tokens"] == 1000
        assert row_10["mcp_tokens"] == 500
        assert row_10["trr_percent"] == pytest.approx(50.0)
        assert row_10["baseline_ssf"] == 1.0
        assert row_10["mcp_ssf"] == 1.0

        row_100 = scaling_rows[1]
        assert row_100["table_count"] == 100
        assert row_100["baseline_tokens"] == 10000
        assert row_100["baseline_ssf"] == pytest.approx(10.0)
        assert row_100["mcp_ssf"] == pytest.approx(1.0)
        assert row_100["trr_percent"] == pytest.approx(95.0)

        row_1000 = scaling_rows[2]
        assert row_1000["table_count"] == 1000
        assert row_1000["baseline_tokens"] == 100000
        assert row_1000["baseline_ssf"] == pytest.approx(10.0)
        assert row_1000["mcp_ssf"] == pytest.approx(1.0)
        assert row_1000["trr_percent"] == pytest.approx(99.5)
