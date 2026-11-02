#!/usr/bin/env python3
"""BenchmarkHarness: rozgrzewka, powtórzenia, statystyki, eksport JSON/CSV, profiler psutil."""

from pathlib import Path
import sys

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import asyncio
import json
import pytest
from benchmarks.harness import BenchmarkHarness, BenchmarkResult, SystemResourceProfiler


class TestBenchmarkResult:
    def test_result_conversion_methods(self):
        result = BenchmarkResult(
            test_name="test_schema_discovery",
            approach="MCP_OPTIMIZED",
            latency_ms=12.345,
            token_count=150,
            payload_bytes=800,
            cpu_usage_pct=5.5,
            ram_usage_mb=120.0,
            success=True,
            error_message=None,
            extra_metadata={"k": 5, "domain": "telemetry"},
            std_dev=0.55,
            variance=0.3025,
            p50_latency_ms=12.3,
            p95_latency_ms=13.1,
            p99_latency_ms=13.5,
            raw_samples=[11.8, 12.3, 12.9],
        )

        nested = result.to_dict()
        assert nested["test_name"] == "test_schema_discovery"
        assert nested["extra_metadata"] == {"k": 5, "domain": "telemetry"}
        assert nested["std_dev"] == 0.55
        assert nested["p50_latency_ms"] == 12.3
        assert nested["raw_samples"] == [11.8, 12.3, 12.9]

        flat = result.to_flat_dict()
        assert flat["test_name"] == "test_schema_discovery"
        assert isinstance(flat["extra_metadata"], str)
        parsed_meta = json.loads(flat["extra_metadata"])
        assert parsed_meta["k"] == 5
        assert isinstance(flat["raw_samples"], str)
        parsed_samples = json.loads(flat["raw_samples"])
        assert parsed_samples == [11.8, 12.3, 12.9]


class TestSystemResourceProfiler:
    def test_profiler_metrics_output(self):
        profiler = SystemResourceProfiler()
        profiler.start_profiling()
        # trochę roboty, żeby CPU nie było 0
        _ = sum(i * i for i in range(100_000))
        cpu_pct, ram_mb = profiler.stop_profiling()

        assert isinstance(cpu_pct, float)
        assert isinstance(ram_mb, float)
        assert ram_mb > 0.0
        assert cpu_pct >= 0.0


@pytest.mark.asyncio
class TestBenchmarkHarness:
    async def test_run_scenario_success_with_iterations_and_stats(self, tmp_path):
        harness = BenchmarkHarness()

        call_count = 0

        async def sample_task():
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.005)
            return {"status": "ok", "items": ["a", "b", "c"]}

        result = await harness.run_scenario(
            test_name="sample_async_task",
            approach="MCP_OPTIMIZED",
            scenario_fn=sample_task,
            extra_metadata={"iteration_test": True},
            iterations=10,
            warmup_runs=3,
        )

        assert result.success is True
        assert call_count == 13  # 3 warmup + 10 iterations
        assert len(result.raw_samples) == 10
        assert result.latency_ms >= 4.0
        assert result.p50_latency_ms >= 4.0
        assert result.p95_latency_ms >= result.p50_latency_ms
        assert result.p99_latency_ms >= result.p95_latency_ms
        assert result.std_dev >= 0.0
        assert result.variance >= 0.0
        assert result.token_count > 0
        assert result.payload_bytes > 0
        assert result.error_message is None
        assert len(harness.results) == 1

        out_dir = str(tmp_path / "test_results")
        json_path, csv_path = harness.export_results(out_dir)

        assert json_path.exists()
        assert csv_path.exists()

        with open(json_path, encoding="utf-8") as jf:
            loaded_json = json.load(jf)
            assert len(loaded_json) == 1
            item = loaded_json[0]
            assert item["test_name"] == "sample_async_task"
            assert "std_dev" in item
            assert "p50_latency_ms" in item
            assert len(item["raw_samples"]) == 10

    async def test_run_scenario_failure_handling(self):
        harness = BenchmarkHarness()

        async def failing_task():
            await asyncio.sleep(0.005)
            raise RuntimeError("Symulowany błąd połączenia z bazą")

        result = await harness.run_scenario(
            test_name="failing_task",
            approach="BASELINE",
            scenario_fn=failing_task,
            iterations=5,
            warmup_runs=2,
        )

        assert result.success is False
        assert "Symulowany błąd" in (result.error_message or "")
        assert result.token_count == 0
        assert result.payload_bytes == 0

    async def test_run_scenario_default_iterations_and_warmup(self):
        harness = BenchmarkHarness()
        call_count = 0

        async def fast_task():
            nonlocal call_count
            call_count += 1
            return {"status": "ok"}

        result = await harness.run_scenario(
            test_name="default_params_task",
            approach="TEST",
            scenario_fn=fast_task,
        )

        assert call_count == 105  # 5 warmup + 100 iterations
        assert len(result.raw_samples) == 100
        assert result.success is True
        assert result.std_dev >= 0.0
