#!/usr/bin/env python3
"""Wzory z benchmark_pagination.py: PTE, narzut JSON-RPC, redukcja heavy tail."""

from pathlib import Path
import sys

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import pytest
from benchmarks.benchmark_pagination import (
    HeavyTailResult,
    PaginationBudgetResult,
    ProtocolOverheadResult,
    calculate_protocol_overhead,
    calculate_pte,
)


class TestPaginationMetrics:
    def test_calculate_pte_standard(self):
        # 20 wierszy dla budżetu 2000 tokenów = 0.0100 wierszy/token
        assert calculate_pte(20, 2000) == pytest.approx(0.0100)
        # 41 wierszy dla budżetu 4000 tokenów = 0.01025 wierszy/token
        assert calculate_pte(41, 4000) == pytest.approx(0.01025)

    def test_calculate_pte_edge_cases(self):
        assert calculate_pte(10, 0) == 0.0
        assert calculate_pte(10, -500) == 0.0
        assert calculate_pte(0, 1000) == 0.0


class TestProtocolOverheadMetrics:
    def test_calculate_protocol_overhead_standard(self):
        # 1000 B danych w ramce 1500 B -> 50% narzutu
        assert calculate_protocol_overhead(1000, 1500) == pytest.approx(50.0)
        assert calculate_protocol_overhead(1000, 2000) == pytest.approx(100.0)

    def test_calculate_protocol_overhead_edge_cases(self):
        assert calculate_protocol_overhead(0, 500) == 0.0
        assert calculate_protocol_overhead(-100, 500) == 0.0


class TestDataStructures:
    def test_pagination_budget_result(self):
        res = PaginationBudgetResult(
            token_budget=2000,
            rows_retrieved=20,
            tokens_used=1907,
            latency_ms=4.97,
            pte=0.0100,
            has_more=True,
            cursor="eyJsYXN0X2lkIjoyMH0=",
            mode="LEAN_ANALYTICS",
        )
        assert res.token_budget == 2000
        assert res.rows_retrieved == 20
        assert res.pte == 0.0100
        assert res.has_more is True

    def test_protocol_overhead_result(self):
        res = ProtocolOverheadResult(
            batch_size=50,
            raw_data_bytes=12558,
            json_rpc_bytes=26471,
            overhead_bytes=13913,
            rho_overhead_pct=110.79,
            latency_ms=2.51,
        )
        assert res.batch_size == 50
        assert res.overhead_bytes == 13913
        assert res.rho_overhead_pct == 110.79

    def test_heavy_tail_result(self):
        res = HeavyTailResult(
            blob_label="1MB",
            raw_bytes=1048576,
            inband_bytes=1398326,
            inband_tokens=1002893,
            inband_latency_ms=1.84,
            inband_ram_mb=1048.3,
            inband_std_dev=0.1,
            inband_p50_ms=1.8,
            inband_p95_ms=1.9,
            oob_bytes=281,
            oob_tokens=126,
            oob_latency_ms=0.48,
            oob_ram_mb=1052.2,
            oob_std_dev=0.05,
            oob_p50_ms=0.48,
            oob_p95_ms=0.52,
            token_reduction_pct=99.99,
            payload_reduction_pct=99.98,
            speedup_factor=3.8,
        )
        assert res.blob_label == "1MB"
        assert res.token_reduction_pct > 99.9
        assert res.payload_reduction_pct > 99.9
        assert res.speedup_factor > 1.0
