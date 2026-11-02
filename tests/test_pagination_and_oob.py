#!/usr/bin/env python3
"""
Paginacja keyset (zapytania, kursor HMAC, Welford, indeksy, snapshoty) i transfer OOB.
Baza jest atrapą, SHM i bloby są prawdziwe (na /dev/shm i w katalogu tymczasowym).
"""

from datetime import datetime, timezone
from decimal import Decimal
import json
import math
from multiprocessing import shared_memory
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.mcp_server.out_of_band import (
    DataTransferDispatcher,
    TransferMode,
    data_dispatcher,
    read_posix_shm,
    write_http_stream_blob,
    write_in_band,
    write_posix_shm,
)
from src.mcp_server.pagination import (
    AdaptiveTokenBudgetEstimator,
    IndexCatalogGuard,
    IndexCheckResult,
    IndexMissingError,
    SnapshotExpiredError,
    SortColumnSpec,
    TransactionSnapshotManager,
    build_keyset_query,
    decode_cursor,
    decode_opaque_cursor,
    encode_cursor,
    encode_opaque_cursor,
    estimate_tokens,
    fetch_paginated_data,
    index_catalog_guard,
    normalize_order_by,
    sanitize_table_name,
    snapshot_manager,
    token_budget_estimator,
    validate_identifier,
)


class TestKeysetQueryBuilder:
    def test_keyset_simple_id_asc(self):
        query, params = build_keyset_query(
            safe_table='"public"."orders"',
            columns="*",
            sort_col="id",
            direction="ASC",
            cursor_data=None,
            fetch_limit=10,
        )
        assert 'ORDER BY "id" ASC LIMIT $1' in query
        assert params == [10]

    def test_keyset_simple_id_with_cursor(self):
        cursor = {"last_id": 42}
        query, params = build_keyset_query(
            safe_table='"public"."orders"',
            columns="id, amount",
            sort_col="id",
            direction="ASC",
            cursor_data=cursor,
            fetch_limit=20,
        )
        assert '"id" > $1' in query
        assert 'ORDER BY "id" ASC LIMIT $2' in query
        assert params == [42, 20]

    def test_keyset_composite_low_cardinality_not_null(self):
        cursor = {"sort_col": "status", "last_val": "PENDING", "last_id": 105}
        query, params = build_keyset_query(
            safe_table='"public"."orders"',
            columns="*",
            sort_col="status",
            direction="ASC",
            cursor_data=cursor,
            fetch_limit=50,
        )
        assert '"status" > $1' in query
        assert '"status" = $1 AND "id" > $2' in query
        assert '"status" IS NULL' in query
        assert 'ORDER BY "status" ASC NULLS LAST, "id" ASC LIMIT $3' in query
        assert params == ["PENDING", 105, 50]

    def test_keyset_composite_low_cardinality_with_null(self):
        cursor = {"sort_col": "description", "last_val": None, "last_id": 200}
        query, params = build_keyset_query(
            safe_table='"public"."orders"',
            columns="*",
            sort_col="description",
            direction="ASC",
            cursor_data=cursor,
            fetch_limit=25,
        )
        assert 'WHERE ("description" IS NULL AND "id" > $1)' in query
        assert 'ORDER BY "description" ASC NULLS LAST, "id" ASC LIMIT $2' in query
        assert params == [200, 25]

    def test_keyset_desc_direction(self):
        cursor = {"sort_col": "amount", "last_val": 99.50, "last_id": 12}
        query, params = build_keyset_query(
            safe_table='"public"."orders"',
            columns="*",
            sort_col="amount",
            direction="DESC",
            cursor_data=cursor,
            fetch_limit=15,
        )
        assert '"amount" < $1' in query
        assert 'ORDER BY "amount" DESC NULLS LAST, "id" DESC LIMIT $3' in query
        assert params == [99.50, 12, 15]

    def test_sql_injection_protection(self):
        with pytest.raises(ValueError):
            validate_identifier("status; DROP TABLE orders;--")
        with pytest.raises(ValueError):
            sanitize_table_name("public.orders; SELECT 1;")


class TestOpaqueCursorToken:
    def test_cursor_encoding_and_decoding(self):
        token = encode_opaque_cursor(
            sort_col="created_at",
            last_val="2026-10-02T10:00:00Z",
            last_id=777,
            direction="ASC",
            query_hash="abc12345",
        )
        assert "." in token
        decoded = decode_opaque_cursor(token)
        if isinstance(decoded["last_val"], datetime):
            assert decoded["last_val"] == datetime(2026, 10, 2, 10, 0, tzinfo=timezone.utc)
        else:
            assert decoded["last_val"] == "2026-10-02T10:00:00Z"
        assert decoded["last_id"] == 777
        assert decoded["direction"] == "ASC"
        assert decoded["query_hash"] == "abc12345"

    def test_cursor_tamper_detection(self):
        token = encode_opaque_cursor(
            sort_col="amount",
            last_val=500.0,
            last_id=10,
        )
        b64_part, sig_part = token.split(".")
        # zmieniamy pierwszy znak podpisu
        tampered_sig = ("0" if sig_part[0] != "0" else "1") + sig_part[1:]
        tampered_token = f"{b64_part}.{tampered_sig}"

        with pytest.raises(ValueError, match="błąd weryfikacji HMAC"):
            decode_opaque_cursor(tampered_token)

    def test_legacy_cursor_compatibility(self):
        legacy_json = json.dumps({"last_id": 88})
        import base64
        legacy_token = base64.urlsafe_b64encode(legacy_json.encode("utf-8")).decode("ascii")

        decoded = decode_opaque_cursor(legacy_token)
        assert decoded["last_id"] == 88
        assert decode_cursor(legacy_token) == 88


class TestAdaptiveTokenBudgetEstimator:
    def test_welford_mean_and_variance(self):
        estimator = AdaptiveTokenBudgetEstimator(initial_tokens_per_row=100.0)
        samples = [10.0, 20.0, 30.0, 40.0, 50.0]

        for s in samples:
            estimator.update(s)

        assert estimator.count == 5
        assert estimator.mean == pytest.approx(30.0)
        assert estimator.variance == pytest.approx(250.0)
        assert estimator.std_dev == pytest.approx(math.sqrt(250.0))

    def test_batch_size_estimation(self):
        estimator = AdaptiveTokenBudgetEstimator(initial_tokens_per_row=50.0)
        # std = 0, więc (1000 * 0.95) // 50 = 19
        batch_size = estimator.estimate_batch_size(token_budget=1000, max_records=100)
        assert batch_size == 19

    def test_batch_update(self):
        estimator = AdaptiveTokenBudgetEstimator()
        estimator.update_from_batch(total_tokens=2000, num_rows=20)
        assert estimator.count == 1
        assert estimator.mean == pytest.approx(100.0)


class TestOutOfBandTransfer:
    def test_in_band_transfer(self):
        data = b"Dane testowe MCP In-Band"
        desc, delta_ms = write_in_band(data)

        assert desc["transport"] == TransferMode.IN_BAND.value
        assert desc["size_bytes"] == len(data)
        assert "payload" in desc
        assert desc["overhead_ms"] >= 0.0

        retrieved = data_dispatcher.retrieve(desc)
        assert retrieved == data

    def test_oob_posix_shm_transfer_and_cleanup(self):
        data = b"Duzy ladunek testowy dla pamieci POSIX SHM" * 100
        desc, delta_ms = write_posix_shm(data)

        assert desc["type"] == "shm"
        assert desc["transport"] == TransferMode.OOB_POSIX_SHM.value
        assert desc["name"].startswith("/dev/shm/")
        assert desc["size_bytes"] == len(data)
        assert "sha256" in desc
        assert desc["overhead_ms"] >= 0.0

        retrieved = read_posix_shm(desc["name"], unlink_after_read=True)
        assert retrieved == data

        # po unlinku segmentu już nie ma
        clean_name = desc["shm_id"]
        with pytest.raises(FileNotFoundError):
            shared_memory.SharedMemory(name=clean_name)

    def test_oob_http_stream_blob(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data = b"Zawartosc testowa strumienia blob HTTP"
            desc, delta_ms = write_http_stream_blob(data, storage_dir=Path(tmpdir))

            assert desc["type"] == "http_stream"
            assert "resource_uri" in desc
            assert "token=" in desc["resource_uri"]
            assert desc["size_bytes"] == len(data)
            assert desc["overhead_ms"] >= 0.0

            retrieved = data_dispatcher.retrieve(desc)
            assert retrieved == data

    def test_dispatcher_auto_mode(self):
        dispatcher = DataTransferDispatcher(threshold_bytes=1024)

        small_data = b"small" * 10
        desc_small = dispatcher.dispatch(small_data, mode=TransferMode.AUTO)
        assert desc_small["transport"] == TransferMode.IN_BAND.value

        large_data = b"large" * 1000  # 5000 bajtów > 1024
        desc_large = dispatcher.dispatch(large_data, mode=TransferMode.AUTO)
        assert desc_large["transport"] == TransferMode.OOB_POSIX_SHM.value

        # sprzątamy, inaczej segment zostaje w /dev/shm
        read_posix_shm(desc_large["name"], unlink_after_read=True)

    def test_sha256_integrity_failure(self):
        desc = {
            "type": "in_band",
            "transport": "IN_BAND",
            "payload": "SGVsbG8=",  # "Hello"
            "sha256": "incorrect_hash_value",
        }
        with pytest.raises(ValueError, match="Błąd integralności SHA-256"):
            data_dispatcher.retrieve(desc)


class TestPaginationIntegration:
    @pytest.mark.asyncio
    async def test_fetch_paginated_data_flow(self):
        mock_pool = MagicMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock(spec=asyncpg.Connection)

        mock_rows = []
        for i in range(1, 16):
            rec = {
                "id": i,
                "status": "COMPLETED",
                "amount": Decimal("100.50"),
                "description": f"Zamówienie nr {i}",
            }
            mock_rows.append(rec)

        mock_conn.fetch.return_value = mock_rows

        class PoolCtx:
            async def __aenter__(self):
                return mock_conn

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        mock_pool.acquire.return_value = PoolCtx()

        res = await fetch_paginated_data(
            pool=mock_pool,
            table_name="analytics_orders",
            cursor=None,
            token_budget=2000,
            max_records=10,
            sort_col="status",
            direction="ASC",
        )

        assert len(res["data"]) <= 10
        assert res["has_more"] is True
        assert res["tokens_used"] > 0
        assert res["next_cursor"] is not None
        assert "." in res["next_cursor"]  # Opaque cursor z sygnaturą HMAC
        assert "index_guard" in res


class TestMultiColumnKeyset:
    def test_normalize_order_by_auto_appends_id(self):
        specs = normalize_order_by("created_at DESC, amount ASC")
        assert len(specs) == 3
        assert specs[0].column == "created_at"
        assert specs[0].direction == "DESC"
        assert specs[1].column == "amount"
        assert specs[1].direction == "ASC"
        assert specs[2].column == "id"
        assert specs[2].direction == "ASC"

    def test_multi_column_keyset_query_generation(self):
        specs = normalize_order_by("created_at DESC NULLS LAST, amount ASC NULLS LAST, id ASC")
        cursor = {
            "last_values": ["2026-05-15T12:00:00Z", 250.75, 501],
            "last_id": 501,
        }
        query, params = build_keyset_query(
            safe_table='"public"."analytics_orders"',
            columns="id, created_at, amount",
            order_by=specs,
            cursor_data=cursor,
            fetch_limit=50,
        )

        assert '"created_at" < $1 OR "created_at" IS NULL' in query
        assert '"created_at" = $1 AND ("amount" > $2 OR "amount" IS NULL)' in query
        assert '"created_at" = $1 AND "amount" = $2 AND "id" > $3' in query
        assert 'ORDER BY "created_at" DESC NULLS LAST, "amount" ASC NULLS LAST, "id" ASC' in query
        assert params == ["2026-05-15T12:00:00Z", 250.75, 501, 50]

    def test_multi_column_union_all_branches(self):
        specs = normalize_order_by("created_at DESC NULLS LAST, amount ASC NULLS LAST, id ASC")
        cursor = {"last_values": ["2026-05-15T12:00:00Z", 250.75, 501], "last_id": 501}
        query, params = build_keyset_query(
            safe_table='"public"."analytics_orders"',
            columns="id, created_at, amount",
            order_by=specs,
            cursor_data=cursor,
            fetch_limit=50,
            use_union_all=True,
        )
        assert query.count("UNION ALL") == 4
        assert " OR " not in query
        assert 'WHERE "created_at" < $1 ORDER BY' in query
        assert 'WHERE "created_at" = $1 AND "amount" = $2 AND "id" > $3 ORDER BY' in query
        assert query.endswith('"id" ASC LIMIT $4;')
        assert params == ["2026-05-15T12:00:00Z", 250.75, 501, 50]

    def test_union_all_fallback_when_sort_keys_not_selected(self):
        """Bez kolumn sortowania na liście SELECT zapytanie wraca do trybu DNF."""
        specs = normalize_order_by("created_at DESC NULLS LAST, amount ASC NULLS LAST, id ASC")
        cursor = {"last_values": ["2026-05-15T12:00:00Z", 250.75, 501], "last_id": 501}
        query, _ = build_keyset_query(
            safe_table='"public"."analytics_orders"',
            columns="status",
            order_by=specs,
            cursor_data=cursor,
            fetch_limit=50,
            use_union_all=True,
        )
        assert "UNION ALL" not in query

    def test_union_all_single_column_unchanged(self):
        """Dla sortowania po samym id zapytanie jest identyczne niezależnie od use_union_all."""
        kwargs = dict(safe_table='"public"."orders"', columns="id", sort_col="id", cursor_data={"last_id": 42}, fetch_limit=20)
        assert build_keyset_query(**kwargs) == build_keyset_query(**kwargs, use_union_all=True)

    def test_multi_column_row_value_comparison(self):
        specs = normalize_order_by("created_at ASC, amount ASC, id ASC")
        cursor = {
            "last_values": ["2026-01-01", 10.0, 100],
            "last_id": 100,
        }
        query, params = build_keyset_query(
            safe_table='"public"."analytics_orders"',
            columns="*",
            order_by=specs,
            cursor_data=cursor,
            fetch_limit=25,
            prefer_tuple_comparison=True,
        )
        assert 'WHERE ("created_at", "amount", "id") > ($1, $2, $3)' in query
        assert params == ["2026-01-01", 10.0, 100, 25]

    def test_multi_column_opaque_token_roundtrip(self):
        specs = normalize_order_by("created_at DESC, priority ASC, id ASC")
        token = encode_opaque_cursor(
            order_specs=specs,
            last_values=["2026-10-02T16:00:00", 3, 999],
            last_id=999,
            snapshot_id="00000001-00001000-1",
        )
        decoded = decode_opaque_cursor(token)
        assert decoded["last_id"] == 999
        assert decoded["snapshot_id"] == "00000001-00001000-1"
        assert len(decoded["last_values"]) == 3
        assert decoded["last_values"][1] == 3


class TestIndexCatalogGuardAndSnapshots:
    @pytest.mark.asyncio
    async def test_index_guard_detection(self):
        guard = IndexCatalogGuard()
        mock_conn = AsyncMock()

        # w "katalogu" jest tylko indeks (created_at, amount, id)
        mock_conn.fetch.return_value = [
            {
                "index_name": "idx_analytics_orders_multi",
                "index_type": "btree",
                "is_valid": True,
                "index_columns": ["created_at", "amount", "id"],
            }
        ]

        specs = normalize_order_by("created_at DESC, amount ASC, id ASC")
        res = await guard.verify_index_compatibility(mock_conn, '"public"."analytics_orders"', specs)
        assert res.is_compatible is True
        assert res.matching_index_name == "idx_analytics_orders_multi"
        assert res.warning is None

        specs_unindexed = normalize_order_by("description ASC, status DESC, id ASC")
        res_unindexed = await guard.verify_index_compatibility(mock_conn, '"public"."analytics_orders"', specs_unindexed)
        assert res_unindexed.is_compatible is False
        assert "UnindexedKeysetWarning" in res_unindexed.warning

    @pytest.mark.asyncio
    async def test_snapshot_lifecycle(self):
        mgr = TransactionSnapshotManager()
        mock_pool = MagicMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock(spec=asyncpg.Connection)
        mock_tx = AsyncMock()

        mock_conn.transaction.return_value = mock_tx
        mock_conn.fetchval.return_value = "00000005-00002000-1"
        mock_conn.fetch.return_value = [{"id": 1, "status": "OK"}]

        class PoolCtx:
            def __await__(self):
                async def _acq():
                    return mock_conn
                return _acq().__await__()

            async def __aenter__(self):
                return mock_conn

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        mock_pool.acquire.return_value = PoolCtx()
        mock_pool.release = AsyncMock()

        snap_id = await mgr.create_snapshot(mock_pool, ttl_seconds=60.0, table_name="orders")
        assert snap_id == "00000005-00002000-1"
        assert mock_tx.start.called

        rows = await mgr.execute_in_snapshot(mock_pool, snap_id, "SELECT * FROM orders", [])
        assert len(rows) == 1
        assert mock_conn.execute.called

        await mgr.release_snapshot(snap_id)
        assert snap_id not in mgr._snapshots
        assert mock_tx.rollback.called

    @pytest.mark.asyncio
    async def test_snapshot_expired_error(self):
        mgr = TransactionSnapshotManager()
        mock_pool = MagicMock(spec=asyncpg.Pool)

        with pytest.raises(SnapshotExpiredError):
            await mgr.execute_in_snapshot(mock_pool, "nonexistent-snap", "SELECT 1", [])

