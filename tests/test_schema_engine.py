#!/usr/bin/env python3
"""
SchemaEngine na atrapie puli: BM25, dense, RRF i dokładanie mostków z grafu FK.
Model SentenceTransformer jest prawdziwy, więc pierwszy start trwa chwilę.
"""

from pathlib import Path
import sys
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import networkx as nx
import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.mcp_server.schema_engine import SchemaEngine, schema_engine


@pytest.fixture
def mock_pool():
    """Mock puli połączeń asyncpg z przykładowymi danymi schematu korporacyjnego."""
    pool = MagicMock(spec=asyncpg.Pool)
    conn = AsyncMock(spec=asyncpg.Connection)

    sample_tables = [
        {
            "table_name": "finance_payment_transactions_daily",
            "domain": "finance",
            "description": "Dzienne transakcje płatności dla rachunkowości finansowej daily payment transactions",
            "column_names": "id transaction_code account_id amount currency status created_at",
        },
        {
            "table_name": "finance_ledger_entries_daily",
            "domain": "finance",
            "description": "Dzienne pozycje księgi dla ewidencji finansowej daily ledger entries",
            "column_names": "id transaction_code account_id amount status created_at",
        },
        {
            "table_name": "crm_client_accounts_enterprise",
            "domain": "crm",
            "description": "Konta klientów korporacyjnych i relacje biznesowe enterprise client accounts",
            "column_names": "id customer_id stage deal_value assigned_agent_id",
        },
        {
            "table_name": "inventory_purchase_orders_raw_materials",
            "domain": "inventory",
            "description": "Zlecenia zakupu surowców i logistyka dostaw purchase orders raw materials",
            "column_names": "id sku warehouse_id quantity unit_cost",
        },
        {
            "table_name": "finance_invoices_payable_intercompany",
            "domain": "finance",
            "description": "Faktury należne międzyfirmowe łączące płatności i zlecenia intercompany invoices payable",
            "column_names": "id invoice_code client_id purchase_order_id amount",
        },
    ]

    # fetch rozpoznaje zapytanie po fragmencie SQL i zwraca pasujące dane
    async def mock_fetch(query, *args):
        if "information_schema.tables" in query:
            return sample_tables
        elif "public.schema_embeddings" in query and "ORDER BY" in query:
            limit = args[1] if len(args) > 1 else 5
            pgv_rows = [
                {
                    "table_name": "finance_payment_transactions_daily",
                    "domain": "finance",
                    "description": "Dzienne transakcje płatności",
                    "similarity": 0.89,
                },
                {
                    "table_name": "finance_ledger_entries_daily",
                    "domain": "finance",
                    "description": "Dzienne pozycje księgowe",
                    "similarity": 0.85,
                },
                {
                    "table_name": "crm_client_accounts_enterprise",
                    "domain": "crm",
                    "description": "Konta klientów korporacyjnych",
                    "similarity": 0.72,
                },
            ]
            return pgv_rows[:limit]
        elif "public.schema_foreign_keys" in query and "WHERE" in query:
            return [
                {
                    "source_table": "finance_payment_transactions_daily",
                    "target_table": "finance_invoices_payable_intercompany",
                    "constraint_name": "fk_pmt_inv",
                    "join_columns": {"source_column": "invoice_id", "target_column": "id"},
                }
            ]
        elif "public.schema_foreign_keys" in query:
            return [
                {
                    "source_table": "finance_payment_transactions_daily",
                    "target_table": "finance_invoices_payable_intercompany",
                    "constraint_name": "fk_pmt_inv",
                    "join_columns": '{"source_column": "invoice_id", "target_column": "id"}',
                },
                {
                    "source_table": "finance_invoices_payable_intercompany",
                    "target_table": "inventory_purchase_orders_raw_materials",
                    "constraint_name": "fk_inv_po",
                    "join_columns": '{"source_column": "purchase_order_id", "target_column": "id"}',
                },
            ]
        elif "information_schema.columns" in query:
            return [
                {
                    "table_name": "finance_payment_transactions_daily",
                    "column_name": "id",
                    "data_type": "bigint",
                    "is_nullable": "NO",
                    "column_default": "nextval('seq')",
                    "ordinal_position": 1,
                },
                {
                    "table_name": "finance_payment_transactions_daily",
                    "column_name": "amount",
                    "data_type": "numeric",
                    "is_nullable": "NO",
                    "column_default": None,
                    "ordinal_position": 2,
                },
            ]
        return []

    conn.fetch = mock_fetch

    class PoolAcquireContext:
        async def __aenter__(self):
            return conn

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    pool.acquire.return_value = PoolAcquireContext()
    return pool


class TestSchemaEngineBM25:
    @pytest.mark.asyncio
    async def test_bm25_retrieval(self, mock_pool):
        engine = SchemaEngine()
        results = await engine.retrieve_bm25(query="payment transactions daily", top_k=3, pool=mock_pool)

        assert len(results) > 0
        top_result = results[0]
        assert "payment" in top_result["table_name"] or "daily" in top_result["table_name"]
        assert top_result["rank"] == 1
        assert top_result["score"] > 0

    @pytest.mark.asyncio
    async def test_bm25_empty_query(self, mock_pool):
        engine = SchemaEngine()
        results = await engine.retrieve_bm25(query="", top_k=2, pool=mock_pool)
        assert len(results) <= 2


class TestSchemaEngineDense:
    @pytest.mark.asyncio
    async def test_dense_retrieval(self, mock_pool):
        engine = SchemaEngine()
        results = await engine.retrieve_dense(query="transakcje płatnicze księgi", top_k=2, pool=mock_pool)

        assert len(results) == 2
        assert "table_name" in results[0]
        assert "similarity" in results[0]
        assert results[0]["rank"] == 1
        assert results[1]["rank"] == 2
        assert results[0]["similarity"] >= results[1]["similarity"]


class TestSchemaEngineHybridRRF:
    @pytest.mark.asyncio
    async def test_hybrid_rrf_scoring(self, mock_pool):
        engine = SchemaEngine()
        results = await engine.retrieve_hybrid_rrf(query="payment ledger", top_k=3, pool=mock_pool)

        assert len(results) <= 3
        assert len(results) > 0

        for i in range(len(results) - 1):
            assert results[i]["rrf_score"] >= results[i + 1]["rrf_score"]

        for item in results:
            assert "table_name" in item
            assert "rrf_score" in item
            assert item["rrf_score"] > 0.0


class TestSchemaEngineGraphPruning:
    @pytest.mark.asyncio
    async def test_graph_pruning_and_bridge_detection(self, mock_pool):
        """
        Weryfikuje, czy discover_schema_with_graph_pruning prawidłowo wykrywa
        tabelę mostkującą (odległość <= 2 krawędzie) i generuje zwarty DDL.
        """
        engine = SchemaEngine()

        # RRF zwraca payment_transactions i purchase_orders, a w grafie łączą się
        # przez invoices_payable - ta powinna zostać dołożona jako most
        with patch.object(
            engine,
            "retrieve_hybrid_rrf",
            AsyncMock(
                return_value=[
                    {"table_name": "finance_payment_transactions_daily", "domain": "finance", "description": "Płatności"},
                    {"table_name": "inventory_purchase_orders_raw_materials", "domain": "inventory", "description": "Zlecenia"},
                ]
            ),
        ):
            ddl = await engine.discover_schema_with_graph_pruning(
                query="payments and purchase orders",
                top_k=2,
                max_expanded_tables=2,
                pool=mock_pool,
            )

            assert "public.finance_payment_transactions_daily" in ddl
            assert "public.inventory_purchase_orders_raw_materials" in ddl

            assert "finance_invoices_payable_intercompany" in ddl
            assert "TABELA MOSTKUJĄCA N:M (BRIDGE)" in ddl
            assert "PRIMARY KEY (id)" in ddl

    @pytest.mark.asyncio
    async def test_graph_pruning_handles_no_path_safely(self, mock_pool):
        engine = SchemaEngine()

        # dwie tabele bez żadnej ścieżki między nimi
        with patch.object(
            engine,
            "retrieve_hybrid_rrf",
            AsyncMock(
                return_value=[
                    {"table_name": "table_unknown_a", "domain": "test", "description": "Tabela izolowana A"},
                    {"table_name": "table_unknown_b", "domain": "test", "description": "Tabela izolowana B"},
                ]
            ),
        ):
            ddl = await engine.discover_schema_with_graph_pruning(
                query="isolated tables",
                top_k=2,
                max_expanded_tables=3,
                pool=mock_pool,
            )
            assert "public.table_unknown_a" in ddl
            assert "public.table_unknown_b" in ddl
            assert "PRIMARY KEY (id)" in ddl
