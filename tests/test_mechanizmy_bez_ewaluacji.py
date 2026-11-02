"""
[M] Testy poprawności mechanizmów, które nie miały ewaluacji wydajnościowej w rundzie pomiarowej
(kursor HMAC, IndexCatalogGuard, estymator Welforda). Testy dotyczą kodu w wersji z rundy (774b75a);
nie mierzą wydajności i nie wymagają bazy danych (pula połączeń jest atrapą).

Testy oznaczone xfail(strict=True) dokumentują wady wykryte podczas przeglądu: przechodzą (jako xfail) na kodzie
z rundy, a po poprawce na gałęzi post-review-fixes zaczynają przechodzić normalnie (wtedy xfail należy usunąć).
"""
import asyncio
import base64
import json

import pytest

from src.mcp_server import pagination as pag


# ----------------------------------------------------------------------------- kursor HMAC
def _cursor(table="public.analytics_orders"):
    specs = pag.normalize_order_by(order_by="created_at DESC NULLS LAST, id ASC")
    import hashlib
    qh = hashlib.sha256(f"{table}:{[s.column for s in specs]}:{[s.direction for s in specs]}".encode()).hexdigest()[:8]
    return pag.encode_opaque_cursor(order_specs=specs, last_values=["2026-01-01 00:00:00+00:00", 10], query_hash=qh)


def test_cursor_roundtrip():
    data = pag.decode_opaque_cursor(_cursor())
    assert data["last_values"][1] == 10
    assert [s["col"] for s in data["order_specs"]] == ["created_at", "id"]


def test_cursor_modified_payload_rejected():
    payload, sig = _cursor().split(".")
    raw = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    raw["last_values"][1] = 999999
    forged = base64.urlsafe_b64encode(json.dumps(raw).encode()).decode() + "." + sig
    with pytest.raises(ValueError, match="HMAC"):
        pag.decode_opaque_cursor(forged)


def test_cursor_modified_signature_rejected():
    payload, sig = _cursor().split(".")
    bad = sig[:-1] + ("0" if sig[-1] != "0" else "1")
    with pytest.raises(ValueError, match="HMAC"):
        pag.decode_opaque_cursor(payload + "." + bad)


@pytest.mark.xfail(strict=True, reason="wersja 774b75a akceptuje niepodpisany kursor starszego formatu {'last_id': n}")
def test_unsigned_legacy_cursor_rejected():
    legacy = base64.urlsafe_b64encode(json.dumps({"last_id": 123}).encode()).decode()
    with pytest.raises(ValueError):
        pag.decode_opaque_cursor(legacy)


# ----------------------------------------------------------------------------- atrapa puli połączeń
class _Conn:
    def __init__(self, log):
        self.log = log

    async def fetch(self, sql, *params):
        self.log.append((sql, params))
        return []


class _Acq:
    def __init__(self, log):
        self.c = _Conn(log)

    async def __aenter__(self):
        return self.c

    async def __aexit__(self, *a):
        return False


class _Pool:
    def __init__(self):
        self.log = []

    def acquire(self, timeout=None):
        return _Acq(self.log)


def _run(monkeypatch, compatible, **kw):
    async def fake_verify(conn, table_identifier, order_specs):
        return pag.IndexCheckResult(is_compatible=compatible, matching_index_name="idx" if compatible else None,
                                    warning=None if compatible else "brak indeksu",
                                    checked_columns=[s.column for s in order_specs])
    monkeypatch.setattr(pag.index_catalog_guard, "verify_index_compatibility", fake_verify)
    pool = _Pool()
    res = asyncio.run(pag.fetch_paginated_data(pool=pool, table_name="analytics_orders", **kw))
    return pool.log, res


def test_index_guard_throttles_to_50_rows_without_index(monkeypatch):
    log, _ = _run(monkeypatch, compatible=False, token_budget=10**9, max_records=500)
    sql, params = log[-1]
    assert params[-1] == 51  # 50 wierszy + 1 do wykrycia kolejnej strony


def test_no_throttling_with_index(monkeypatch):
    log, _ = _run(monkeypatch, compatible=True, token_budget=10**9, max_records=500)
    assert log[-1][1][-1] > 51


def test_strict_index_check_rejects(monkeypatch):
    with pytest.raises(pag.IndexMissingError):
        _run(monkeypatch, compatible=False, strict_index_check=True)


@pytest.mark.xfail(strict=True, reason="wersja 774b75a nie weryfikuje query_hash kursora względem tabeli zapytania")
def test_cursor_from_other_table_rejected(monkeypatch):
    other = _cursor(table="public.other_table")
    with pytest.raises(ValueError):
        _run(monkeypatch, compatible=True, cursor=other, order_by="created_at DESC NULLS LAST, id ASC")


# ----------------------------------------------------------------------------- estymator Welforda
def test_welford_matches_sample_statistics():
    import statistics
    xs = [80.0, 95.5, 120.0, 60.25, 101.0]
    e = pag.AdaptiveTokenBudgetEstimator(initial_tokens_per_row=85.0)
    for x in xs:
        e.update(x)
    assert e.mean == pytest.approx(statistics.mean(xs))
    assert e.std_dev == pytest.approx(statistics.stdev(xs))


def test_welford_batch_size_rule():
    e = pag.AdaptiveTokenBudgetEstimator()
    for x in (100.0, 110.0, 90.0):
        e.update(x)
    expected = int((2000 * 0.95) // (e.mean + 0.5 * e.std_dev))
    assert e.estimate_batch_size(2000, max_records=500) == expected
    assert e.estimate_batch_size(2000, max_records=5) == 5
    assert e.estimate_batch_size(10, max_records=500) == 1
