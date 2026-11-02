"""
Testy aparatury pomiarowej, bez bazy i bez LLM: metryki Text-to-SQL, taksonomia błędów,
wyznaczanie S*, statystyka, deduplikacja wektorów ataku, reguły i konfiguracja trybów,
zachowanie skryptów przy braku danych oraz tryb netem (TCP po lo, bez tc).
"""
import asyncio
import json
import multiprocessing as mp
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmarks.common import metrics_text2sql as m  # noqa: E402
from benchmarks.common import stats  # noqa: E402
from benchmarks.common.config import ConfigError, deep_merge, load_config  # noqa: E402
from benchmarks.common.rules import sweet_spot  # noqa: E402


# --------------------------------------------------------------------------- metryki Text-to-SQL
def test_extract_clean_sql_variants():
    assert m.extract_clean_sql("Here:\n```sql\nSELECT a FROM t;\n```\nThanks") == "SELECT a FROM t;"
    assert m.extract_clean_sql("```sql\nSELECT a FROM t WHERE x = 1") == "SELECT a FROM t WHERE x = 1;"
    assert m.extract_clean_sql("WITH c AS (SELECT 1) SELECT * FROM c") == "WITH c AS (SELECT 1) SELECT * FROM c;"


def test_results_equal_multiset_and_order():
    gt = [{"a": 1}, {"a": 2}]
    assert m.results_equal([{"x": 2}, {"x": 1}], gt, "SELECT a FROM t")  # bez ORDER BY: multizbiór, bez nazw kolumn
    assert not m.results_equal([{"x": 2}, {"x": 1}], gt, "SELECT a FROM t ORDER BY a")  # ORDER BY: kolejność ma znaczenie
    assert not m.results_equal([{"x": 1}, {"x": 2}], gt, "SELECT a FROM t", rule="strict_dict_list")
    assert m.results_equal([{"a": 1}, {"a": 2}], gt, "SELECT a FROM t", rule="strict_dict_list")
    assert not m.results_equal([{"a": 1}], gt, "SELECT a FROM t")


class FakeConn:
    def __init__(self, rows=None, fail=None):
        self.rows, self.fail = rows or [], fail

    async def execute(self, sql):
        if self.fail:
            raise RuntimeError(self.fail)

    async def fetch(self, sql):
        return self.rows


def test_evaluate_sql_valid_and_invalid():
    ok = asyncio.run(m.evaluate_sql(FakeConn([{"a": 1}]), "SELECT a FROM t;", "SELECT a FROM t", [{"a": 1}], "multiset_unless_order_by"))
    assert ok["valid_sql"] and ok["ex_match"] and ok["ex_match_strict"]
    bad = asyncio.run(m.evaluate_sql(FakeConn(fail='column "x" does not exist'), "SELECT x FROM t;", "SELECT a FROM t", [{"a": 1}], "multiset_unless_order_by"))
    assert not bad["valid_sql"] and not bad["ex_match"] and "does not exist" in bad["exec_error"]
    empty = asyncio.run(m.evaluate_sql(FakeConn([]), "SELECT a FROM t;", "SELECT a FROM t", [], "multiset_unless_order_by"))
    assert empty["gt_empty"] and not empty["ex_match"]


@pytest.mark.parametrize("kw,expected", [
    (dict(valid_sql=True, ex_match=True), "SUCCESS"),
    (dict(valid_sql=True, ex_match=False), "SEMANTIC_MISMATCH"),
    (dict(gt_empty=True), "GT_EMPTY"),
    (dict(done_reason="length"), "TRUNCATION"),
    (dict(error_message='relation "foo" does not exist'), "TABLE_MISSING"),
    (dict(error_message='column t_a.zz does not exist', sql="SELECT zz FROM t_a"), "COLUMN_HALLUCINATION"),
    (dict(error_message='missing FROM-clause entry for table "b"', sql="SELECT b.x FROM t_a"), "JOIN_CONDITION_MISMATCH"),
    (dict(error_message="syntax error at end of input", sql="SELECT FROM t_a"), "SYNTAX_ERROR"),
])
def test_classify_error_single_category(kw, expected):
    args = dict(generated_sql=kw.get("sql", "SELECT a FROM t_a"), valid_sql=kw.get("valid_sql", False), ex_match=kw.get("ex_match", False),
                gt_empty=kw.get("gt_empty", False), error_message=kw.get("error_message"), done_reason=kw.get("done_reason", "stop"),
                eval_count=10, num_predict=1024, retrieved_tables={"t_a"}, foreign_keys_pairs=set())
    assert m.classify_error(**args) == expected


def test_classify_cte_is_not_missing_table():
    sql = "WITH base_summary AS (SELECT id FROM t_a) SELECT * FROM base_summary"
    cat = m.classify_error(sql, False, False, False, "syntax error", "stop", 10, 1024, {"t_a"}, set())
    assert cat == "SYNTAX_ERROR"


def test_taxonomy_assertion():
    recs = [{"ex_match": True, "error_category": "SUCCESS"}, {"ex_match": False, "error_category": "TRUNCATION"}]
    assert m.assert_taxonomy_consistent(recs) == {"TRUNCATION": 1}
    with pytest.raises(AssertionError):
        m.assert_taxonomy_consistent([{"ex_match": False, "error_category": "SUCCESS"}])
    with pytest.raises(AssertionError):
        m.assert_taxonomy_consistent([{"ex_match": True, "error_category": "TRUNCATION"}])


# --------------------------------------------------------------------------- statystyka
def test_mann_whitney_and_bootstrap():
    a, b = np.arange(1, 21, dtype=float), np.arange(101, 121, dtype=float)
    r = stats.mann_whitney(a, b)
    assert r["reported"] and r["U"] == 0.0 and r["p_value"] < 1e-6 and r["rank_biserial_r"] == pytest.approx(1.0)
    c1 = stats.bootstrap_median_diff_ci(a, b, seed=1, n_resamples=500)
    c2 = stats.bootstrap_median_diff_ci(a, b, seed=1, n_resamples=500)
    assert c1 == c2 and c1["ci_low"] <= c1["estimate"] <= c1["ci_high"] and c1["estimate"] == -100.0
    assert not stats.mann_whitney([1.0], [2.0])["reported"]


def test_wilcoxon_and_mcnemar():
    w = stats.wilcoxon_paired([2.0, 3.0, 4.0, 5.0, 6.0, 7.0], [1.0, 1.0, 1.0, 1.0, 1.0, 1.0])
    assert w["n_eff"] == 6 and w["W"] == 0.0 and w["rank_biserial_r"] == 1.0
    mc = stats.mcnemar_exact([True, True, False, True], [False, True, False, False])
    assert mc["only_a"] == 2 and mc["only_b"] == 0 and mc["p_value"] == pytest.approx(0.5)


# --------------------------------------------------------------------------- S* (B1)
def test_crossover_estimate():
    from benchmarks.scenarios.bench_oob_crossover import crossover

    sizes = [1000, 5000, 10000, 20000, 40000, 80000]
    rows = []
    for s in sizes:
        for rep in range(5):
            rows.append({"transport_mode": "IN_BAND_BASE64", "payload_size_bytes": s, "e2e_ms": 0.1 + 2e-5 * s + rep * 1e-4})
            rows.append({"transport_mode": "OOB_POSIX_SHM", "payload_size_bytes": s, "e2e_ms": 0.5 + 1e-6 * s + rep * 1e-4})
    res = crossover(pd.DataFrame(rows), "OOB_POSIX_SHM", neighbours=2, n_boot=200, seed=3)
    assert res["reported"]
    assert res["s_star_bytes"] == pytest.approx(0.4 / 1.9e-5, rel=1e-3)
    lo, hi = res["ci95_bytes"]
    assert lo <= res["s_star_bytes"] <= hi


def test_crossover_no_sign_change():
    from benchmarks.scenarios.bench_oob_crossover import crossover

    rows = [{"transport_mode": t, "payload_size_bytes": s, "e2e_ms": v} for s in (1, 2, 3)
            for t, v in (("IN_BAND_BASE64", 1.0), ("OOB_POSIX_SHM", 2.0))]
    assert not crossover(pd.DataFrame(rows), "OOB_POSIX_SHM", 2, 10, 1)["reported"]


def _server(port, storage, ready):
    from benchmarks.scenarios.bench_oob_crossover import tcp_server

    tcp_server(port, storage, ready)


def test_tcp_transport_without_netem(tmp_path):
    """Ścieżka trybu netem (transfer TCP przez lo) bez kolejki tc: poprawność danych i składowe czasu."""
    from benchmarks.scenarios.bench_oob_crossover import _request, measure_tcp

    ctx = mp.get_context("spawn")
    ready = ctx.Event()
    port = 18977
    p = ctx.Process(target=_server, args=(port, str(tmp_path), ready), daemon=True)
    p.start()
    try:
        assert ready.wait(60)
        r1 = measure_tcp("IN_BAND_BASE64", 4096, port)
        r2 = measure_tcp("OOB_HTTP_STREAM", 4096, port)
        assert r1["sha256_verified"] and r2["sha256_verified"]
        assert r1["transfer_ns"] > 0 and "server_base64_encode_ns" in r1
    finally:
        _request(port, {"op": "stop"})
        p.join(10)


# --------------------------------------------------------------------------- bezpieczeństwo
def test_vector_deduplication(tmp_path):
    from benchmarks.scenarios.bench_security_suite import load_unique_vectors, normalize_payload

    assert normalize_payload("SELECT * FROM sales_12 WHERE x > 250; (Wektor #3)") == normalize_payload("select *  from sales_7 where x > 9;")
    doc = {"vectors": [
        {"vector_id": 1, "category": "SQL_INJECTION", "sub_category": "A", "payload": "DROP TABLE users_1;", "expected_allowed": False},
        {"vector_id": 2, "category": "SQL_INJECTION", "sub_category": "A", "payload": "DROP TABLE users_2;", "expected_allowed": False},
        {"vector_id": 3, "category": "SQL_INJECTION", "sub_category": "B", "payload": "TRUNCATE x;", "expected_allowed": False},
    ]}
    f = tmp_path / "v.json"
    f.write_text(json.dumps(doc))
    assert [v["vector_id"] for v in load_unique_vectors(f)] == [1, 3]


def test_shipped_vectors_are_unique():
    from benchmarks.scenarios.bench_security_suite import VECTORS_FILE, load_unique_vectors

    raw = json.loads(VECTORS_FILE.read_text())["vectors"]
    assert len(load_unique_vectors()) == len(raw)


# --------------------------------------------------------------------------- reguły i konfiguracja
def test_sweet_spot_rule():
    assert sweet_spot([0, 1, 2, 3, 5], [39.2, 52.4, 54.5, 54.5, 54.5], 0.5)["m_exp"] == 2
    assert sweet_spot([0, 1], [10.0, 20.0], 0.5)["m_exp"] == 1


def test_config_quick_substitution_only_in_quick(tmp_path):
    q = load_config(mode="quick")
    assert q["llm"]["smoke_test_model_substitution"] is True
    full = load_config(mode="full")
    assert full["llm"]["smoke_test_model_substitution"] is False
    assert full["llm"]["profiles"]["large"]["model"] == "qwen2.5-coder:7b"
    raw = json.loads((ROOT / "benchmarks/config/benchmark_config.json").read_text())
    raw = deep_merge(raw, {"llm": {"smoke_test_model_substitution": {"large": "x"}}})
    bad = tmp_path / "c.json"
    bad.write_text(json.dumps(raw))
    with pytest.raises(ConfigError):
        load_config(bad, mode="full")


# --------------------------------------------------------------------------- brak danych => błąd
def test_plot_script_fails_without_data(tmp_path):
    r = subprocess.run([sys.executable, str(ROOT / "benchmarks/plot_generators/generate_thesis_plots.py"), "--raw-dir", str(tmp_path)],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode != 0
    assert not list((tmp_path / "plots").glob("*.pdf"))


@pytest.mark.parametrize("method", ["schema_recall_precision", "text2sql_accuracy", "error_decomposition", "ablation", "ttft_scaling",
                                    "oob", "pagination", "concurrency", "streaming"])
def test_each_plot_raises_on_missing_input(tmp_path, method):
    from benchmarks.plot_generators.generate_thesis_plots import MissingInput, Plotter

    with pytest.raises(MissingInput):
        getattr(Plotter(tmp_path, tmp_path / "plots"), method)()


def test_statistics_without_data_reports_nothing(tmp_path):
    r = subprocess.run([sys.executable, str(ROOT / "benchmarks/statistical_analysis.py"), "--mode", "quick", "--output-dir", str(tmp_path)],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0
    t = pd.read_csv(tmp_path / "statistical_hypothesis_tests.csv")
    assert (t.status == "not_reported").all()
