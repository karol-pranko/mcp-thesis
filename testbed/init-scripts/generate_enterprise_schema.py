#!/usr/bin/env python3
"""
Generator schematu testowego: 1000 tabel w 5 domenach, udających system typu ERP/CRM.

Graf FK ma być bezskalowy (rozkład potęgowy stopni): kilka tabel-hubów o stopniu 15-40,
tabele łącznikowe N:M i łańcuchy satelitów na 2-3 poziomach. Oprócz samych tabel wypełnia
public.schema_foreign_keys i public.schema_embeddings, z których korzysta SchemaEngine.
"""

import asyncio
from collections import Counter, defaultdict
from dataclasses import dataclass, field
import hashlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import random
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import asyncpg
import networkx as nx
from sentence_transformers import SentenceTransformer
import torch
from tqdm import tqdm


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("enterprise_schema_generator")


@dataclass
class DatabaseConfig:
    host: str = field(
        default_factory=lambda: os.getenv("POSTGRES_HOST", os.getenv("DB_HOST", "localhost"))
    )
    port: int = field(
        default_factory=lambda: int(os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432")))
    )
    user: str = field(
        default_factory=lambda: os.getenv("POSTGRES_USER", os.getenv("DB_USER", "mcp_user"))
    )
    password: str = field(
        default_factory=lambda: os.getenv(
            "POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", "mcp_password")
        )
    )
    database: str = field(
        default_factory=lambda: os.getenv("POSTGRES_DB", os.getenv("DB_NAME", "analytics_db"))
    )


@dataclass
class ForeignKeyRelation:
    source_table: str
    target_table: str
    constraint_name: str
    source_column: str
    target_column: str = "id"

    @property
    def join_columns_json(self) -> str:
        """Zwraca metadane kolumn złączenia w formacie JSON."""
        return json.dumps(
            {"source_column": self.source_column, "target_column": self.target_column}
        )


@dataclass
class EnterpriseTableDefinition:
    table_name: str
    domain: str
    description: str
    columns_sql: str
    create_table_sql: str
    comment_sql: str
    is_hub: bool = False


# Nawiązywanie asynchronicznego połączenia z PostgreSQL
async def create_database_connection(config: DatabaseConfig) -> asyncpg.Connection:
    logger.info(
        "Nawiązywanie połączenia z bazą danych %s@%s:%d/%s...",
        config.user,
        config.host,
        config.port,
        config.database,
    )

    try:
        connection = await asyncpg.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
        )
        logger.info("Połączenie z bazą danych PostgreSQL zostało pomyślnie ustanowione.")
        return connection
    except asyncpg.exceptions.InvalidPasswordError:
        fallback_password = (
            "mcp_secret_password" if config.password == "mcp_password" else "mcp_password"
        )
        logger.warning(
            "Błąd uwierzytelnienia hasła dla użytkownika '%s'. Próba połączenia hasłem alternatywnym...",
            config.user,
        )
        connection = await asyncpg.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=fallback_password,
            database=config.database,
        )
        logger.info("Połączenie nawiązane pomyślnie przy użyciu hasła rezerwowego.")
        return connection


async def initialize_enterprise_metadata_tables(connection: asyncpg.Connection) -> None:
    logger.info("Inicjalizacja rozszerzenia pgvector...")
    await connection.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    logger.info("Inicjalizacja tabeli 'public.schema_embeddings'...")
    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS public.schema_embeddings (
            id SERIAL PRIMARY KEY,
            table_name TEXT UNIQUE NOT NULL,
            domain TEXT NOT NULL,
            description TEXT NOT NULL,
            embedding vector(384)
        );
        CREATE INDEX IF NOT EXISTS schema_embeddings_embedding_idx
        ON public.schema_embeddings USING hnsw (embedding vector_cosine_ops);

        CREATE INDEX IF NOT EXISTS schema_embeddings_domain_idx
        ON public.schema_embeddings (domain);
        """
    )

    logger.info("Inicjalizacja tabeli metadanych relacji 'public.schema_foreign_keys'...")
    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS public.schema_foreign_keys (
            id SERIAL PRIMARY KEY,
            source_table TEXT NOT NULL,
            target_table TEXT NOT NULL,
            constraint_name TEXT NOT NULL UNIQUE,
            join_columns JSONB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sfk_source ON public.schema_foreign_keys (source_table);
        CREATE INDEX IF NOT EXISTS idx_sfk_target ON public.schema_foreign_keys (target_table);
        TRUNCATE public.schema_foreign_keys;
        """
    )
    logger.info("Tabele metadanych 'schema_embeddings' i 'schema_foreign_keys' są gotowe.")


# 5 domen x 20 encji x 10 kwalifikatorów = 1000 tabel
def get_domain_catalog() -> Dict[str, Tuple[List[str], List[str]]]:
    """Zwraca katalog 5 domen, 20 encji oraz 10 kwalifikatorów (łącznie 1000 tabel)."""
    return {
        "finance": (
            [
                "ledger_entries", "invoices_payable", "invoices_receivable", "payment_transactions",
                "expense_claims", "revenue_recognition", "tax_declarations", "budget_forecasts",
                "cash_flow_projections", "dividend_distributions", "audit_records", "accounts_payable",
                "accounts_receivable", "asset_depreciations", "credit_memos", "treasury_positions",
                "currency_hedges", "corporate_bonds", "escrow_deposits", "payroll_disbursements",
            ],
            [
                "daily", "monthly", "quarterly", "annual", "consolidated",
                "intercompany", "domestic", "international", "historical", "reconciled",
            ],
        ),
        "inventory": (
            [
                "stock_balances", "warehouse_bins", "purchase_orders", "goods_receipts",
                "dispatch_manifests", "picking_lists", "cycle_counts", "inventory_adjustments",
                "internal_transfers", "batch_inspections", "serial_trackers", "quarantine_items",
                "bills_of_materials", "work_in_progress", "supplier_shipments", "reorder_thresholds",
                "packaging_materials", "return_authorizations", "storage_zones", "shrinkage_records",
            ],
            [
                "raw_materials", "finished_goods", "components", "spare_parts", "perishable",
                "bonded", "in_transit", "allocated", "expedited", "archived",
            ],
        ),
        "hr": (
            [
                "employee_profiles", "job_positions", "department_units", "job_applicants",
                "recruitment_postings", "interview_scores", "onboarding_tasks", "labor_contracts",
                "salary_reviews", "benefit_plans", "time_attendance", "shift_rotations",
                "leave_requests", "performance_evaluations", "career_goals", "skill_certifications",
                "training_modules", "grievance_filings", "exit_interviews", "safety_incidents",
            ],
            [
                "full_time", "contractor", "executive", "internship", "remote",
                "hybrid", "quarterly", "annual", "probationary", "archived",
            ],
        ),
        "crm": (
            [
                "client_accounts", "contact_persons", "sales_leads", "deal_opportunities",
                "pipeline_stages", "price_quotes", "service_agreements", "client_interactions",
                "support_cases", "case_escalations", "marketing_campaigns", "campaign_attributions",
                "email_deliveries", "audience_segments", "churn_indicators", "satisfaction_surveys",
                "partner_referrals", "recurring_subscriptions", "renewal_contracts", "sales_targets",
            ],
            [
                "enterprise", "mid_market", "smb", "inbound", "outbound",
                "high_value", "escalated", "active", "retention_risk", "archived",
            ],
        ),
        "telemetry": (
            [
                "sensor_readings", "edge_appliances", "firmware_stats", "battery_discharges",
                "temperature_logs", "pressure_records", "vibration_samples", "host_servers",
                "cpu_telemetry", "memory_consumption", "disk_throughput", "network_packets",
                "container_restarts", "mesh_latencies", "api_response_times", "http_error_logs",
                "fleet_telematics", "smart_meter_readings", "security_signals", "stream_offsets",
            ],
            [
                "realtime", "minute_aggregates", "hourly_aggregates", "daily_aggregates", "raw_stream",
                "anomaly_detected", "datacenter_a", "datacenter_b", "edge_node", "cold_storage",
            ],
        ),
    }


def get_enterprise_hub_tables() -> List[str]:
    """
    Definiuje 20 centralnych tabel typu Hub w architekturze korporacyjnej ERP/CRM
    (po 4 kluczowe tabele w każdej z 5 domen).
    """
    return [
        # Finance Hubs
        "finance_ledger_entries_daily",
        "finance_invoices_payable_intercompany",
        "finance_payment_transactions_daily",
        "finance_accounts_payable_consolidated",
        # Inventory Hubs
        "inventory_purchase_orders_raw_materials",
        "inventory_stock_balances_finished_goods",
        "inventory_goods_receipts_finished_goods",
        "inventory_bills_of_materials_components",
        # HR Hubs
        "hr_employee_profiles_annual",
        "hr_employee_profiles_full_time",
        "hr_department_units_full_time",
        "hr_labor_contracts_annual",
        # CRM Hubs
        "crm_client_accounts_enterprise",
        "crm_client_accounts_mid_market",
        "crm_deal_opportunities_enterprise",
        "crm_support_cases_escalated",
        # Telemetry Hubs
        "telemetry_host_servers_datacenter_a",
        "telemetry_host_servers_datacenter_b",
        "telemetry_sensor_readings_realtime",
        "telemetry_edge_appliances_edge_node",
    ]


def generate_constraint_name(source_table: str, target_table: str, col_name: str) -> str:
    """
    Generuje unikalną nazwę ograniczenia klucza obcego o długości <= 63 znaków
    (limit PostgreSQL NAMEDATALEN), zachowując czytelność i unikalność.
    """
    src_short = "".join(part[:4] for part in source_table.split("_"))
    tgt_short = "".join(part[:4] for part in target_table.split("_"))
    hash_digest = hashlib.md5(f"{source_table}->{target_table}:{col_name}".encode("utf-8")).hexdigest()[:8]
    raw_name = f"fk_{src_short}_{tgt_short}_{hash_digest}"
    return raw_name[:63]


def build_enterprise_topology(
    all_tables: List[str],
    tables_by_domain: Dict[str, List[str]],
) -> Tuple[nx.DiGraph, List[ForeignKeyRelation]]:
    """
    Konstruuje topologię relacyjną sieci bezskalowej (Scale-Free Network):
    1. 20 centralnych tabel typu Hub o łącznym stopniu wierzchołka w przedziale 15 - 40.
    2. Mostki relacji N:M łączące kluczowe tabele z hubami i satelitami.
    3. Satelity poziomu 1 połączone z hubami.
    4. Łańcuchy satelickie 2- i 3-poziomowe.
    """
    hubs = get_enterprise_hub_tables()
    hub_set = set(hubs)

    G = nx.DiGraph()
    for t in all_tables:
        G.add_node(t)

    relations: List[ForeignKeyRelation] = []
    added_edges: Set[Tuple[str, str]] = set()

    def add_relation(src: str, dst: str, col_name: Optional[str] = None) -> None:
        if src == dst or (src, dst) in added_edges:
            return
        if col_name is None:
            dst_parts = dst.split("_")
            dst_core = dst_parts[1] if len(dst_parts) > 1 else dst
            col_name = f"fk_{dst_core}_id"

        c_name = generate_constraint_name(src, dst, col_name)
        rel = ForeignKeyRelation(
            source_table=src,
            target_table=dst,
            constraint_name=c_name,
            source_column=col_name,
            target_column="id",
        )
        relations.append(rel)
        added_edges.add((src, dst))
        G.add_edge(src, dst, constraint=c_name, column=col_name)

    # połączenia rdzenne między Hubami (cross-hub enterprise backbone)
    add_relation("crm_deal_opportunities_enterprise", "crm_client_accounts_enterprise", "account_id")
    add_relation("crm_deal_opportunities_enterprise", "hr_employee_profiles_full_time", "assigned_rep_id")
    add_relation("crm_support_cases_escalated", "crm_client_accounts_enterprise", "customer_account_id")
    add_relation("finance_invoices_payable_intercompany", "crm_client_accounts_enterprise", "client_id")
    add_relation("finance_invoices_payable_intercompany", "inventory_purchase_orders_raw_materials", "purchase_order_id")
    add_relation("finance_payment_transactions_daily", "finance_invoices_payable_intercompany", "invoice_id")
    add_relation("finance_payment_transactions_daily", "finance_ledger_entries_daily", "ledger_id")
    add_relation("finance_accounts_payable_consolidated", "finance_ledger_entries_daily", "annual_ledger_id")
    add_relation("inventory_goods_receipts_finished_goods", "inventory_purchase_orders_raw_materials", "order_id")
    add_relation("inventory_stock_balances_finished_goods", "inventory_bills_of_materials_components", "bom_id")
    add_relation("hr_employee_profiles_annual", "hr_department_units_full_time", "department_id")
    add_relation("hr_labor_contracts_annual", "hr_employee_profiles_annual", "employee_id")
    add_relation("telemetry_sensor_readings_realtime", "telemetry_host_servers_datacenter_a", "host_server_id")
    add_relation("telemetry_edge_appliances_edge_node", "telemetry_host_servers_datacenter_a", "gateway_id")

    rnd = random.Random(42)
    domain_of_table: Dict[str, str] = {}
    for dom, tbls in tables_by_domain.items():
        for t in tbls:
            domain_of_table[t] = dom

    hubs_by_domain: Dict[str, List[str]] = defaultdict(list)
    for h in hubs:
        hubs_by_domain[domain_of_table[h]].append(h)

    # generowanie powiązań w domenach: satelity L1, mostki N:M i łańcuchy L2-L3
    for domain, tbls in tables_by_domain.items():
        dom_hubs = hubs_by_domain[domain]
        non_hubs = [t for t in tbls if t not in hub_set]
        rnd.shuffle(non_hubs)

        # Podział tabel niebędących hubami (196 tabel):
        # 72 tabele L1 (po 18 na huba)
        # 36 tabel mostkujących N:M (łączące 2 tabele)
        # 88 tabel satelickich poziomu L2 i L3
        lvl1_tables = non_hubs[:72]
        bridge_tables = non_hubs[72:108]
        lvl2_tables = non_hubs[108:]

        # Poziom 1: równomierne połączenie z 4 hubami danej domeny
        for idx, t in enumerate(lvl1_tables):
            chosen_hub = dom_hubs[idx % len(dom_hubs)]
            if idx % 4 == 0:
                add_relation(chosen_hub, t)
            else:
                add_relation(t, chosen_hub)

        # Mostki N:M: łączą hub z innym hubem lub satelitą poziomu 1
        for idx, bt in enumerate(bridge_tables):
            hub_a = dom_hubs[idx % len(dom_hubs)]
            add_relation(bt, hub_a)
            if idx % 3 == 0:
                other_hub = dom_hubs[(idx + 1) % len(dom_hubs)]
                add_relation(bt, other_hub)
            else:
                partner = lvl1_tables[idx % len(lvl1_tables)]
                add_relation(bt, partner)

        # Poziom 2 i 3: łańcuchy satelickie (L2 -> L1 -> Hub oraz L3 -> L2 -> L1 -> Hub)
        for idx, l2 in enumerate(lvl2_tables):
            if idx < 60:
                parent = lvl1_tables[idx % len(lvl1_tables)]
                add_relation(l2, parent)
            else:
                parent_l2 = lvl2_tables[(idx - 60) % len(lvl2_tables)]
                add_relation(l2, parent_l2)

    return G, relations


def build_base_column_sql(domain: str) -> str:
    """Zwraca standardowe definicje bazowych kolumn dla danej domeny biznesowej."""
    if domain == "finance":
        return """
            id BIGSERIAL PRIMARY KEY,
            transaction_code VARCHAR(64) NOT NULL,
            account_id BIGINT,
            amount NUMERIC(15, 2) NOT NULL,
            currency VARCHAR(3) NOT NULL DEFAULT 'USD',
            status VARCHAR(32) NOT NULL,
            metadata JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        """
    elif domain == "inventory":
        return """
            id BIGSERIAL PRIMARY KEY,
            sku VARCHAR(64) NOT NULL,
            warehouse_id INTEGER,
            quantity INTEGER NOT NULL DEFAULT 0,
            unit_cost NUMERIC(12, 4) NOT NULL,
            batch_number VARCHAR(48),
            attributes JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        """
    elif domain == "hr":
        return """
            id BIGSERIAL PRIMARY KEY,
            employee_id BIGINT,
            department_code VARCHAR(32),
            position_title VARCHAR(96) NOT NULL,
            status VARCHAR(32) NOT NULL,
            effective_date DATE NOT NULL,
            details JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        """
    elif domain == "crm":
        return """
            id BIGSERIAL PRIMARY KEY,
            customer_id BIGINT,
            stage VARCHAR(32) NOT NULL,
            deal_value NUMERIC(12, 2),
            assigned_agent_id BIGINT,
            lead_source VARCHAR(48),
            properties JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        """
    elif domain == "telemetry":
        return """
            id BIGSERIAL PRIMARY KEY,
            device_id VARCHAR(64),
            sensor_type VARCHAR(48) NOT NULL,
            metric_value DOUBLE PRECISION NOT NULL,
            unit_of_measure VARCHAR(16),
            alert_level VARCHAR(16) NOT NULL DEFAULT 'INFO',
            payload JSONB,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        """
    else:
        return """
            id BIGSERIAL PRIMARY KEY,
            entity_key VARCHAR(64) NOT NULL,
            status VARCHAR(32) NOT NULL,
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        """


def build_enterprise_description(
    domain: str,
    entity_name: str,
    qualifier_name: str,
    is_hub: bool,
    connected_targets: List[str],
) -> str:
    """
    Tworzy bogaty semantycznie opis biznesowy dla tabeli uwzględniający jej rolę
    w korporacyjnym grafie powiązań (Hub / Satellite / Bridge).
    """
    domain_titles = {
        "finance": "rachunkowości finansowej, skarbowości korporacyjnej i przepływów pieniężnych",
        "inventory": "logistyki łańcucha dostaw, gospodarki magazynowej i stanów zapasów",
        "hr": "administracji zasobami ludzkimi, zarządzania personelem i wynagrodzeniami",
        "crm": "zarządzania relacjami z klientami, lejka sprzedaży i obsługi zgłoszeń",
        "telemetry": "przemysłowej infrastruktury IoT, telemetrii serwerów i diagnostyki urządzeń",
    }

    readable_entity = entity_name.replace("_", " ")
    readable_qualifier = qualifier_name.replace("_", " ")
    domain_context = domain_titles.get(domain, domain)

    role_str = (
        "Centralna korporacyjna tabela typu Hub"
        if is_hub
        else "Operacyjna encja relacyjna"
    )

    if connected_targets:
        targets_str = ", ".join(t.replace("_", " ") for t in connected_targets[:3])
        conn_info = f" Utrzymuje integralność relacyjną kluczy obcych z tabelami: {targets_str}."
    else:
        conn_info = ""

    return (
        f"{role_str} przechowująca dane dla {readable_entity} ({readable_qualifier}) w obszarze {domain_context}, "
        f"wspierająca ścieżki audytu operacyjnego, analityczne złączenia JOIN oraz raportowanie biznesowe.{conn_info}"
    )


def generate_enterprise_schema_definitions() -> Tuple[
    List[EnterpriseTableDefinition],
    List[ForeignKeyRelation],
    nx.DiGraph,
]:
    """
    Tworzy pełen zestaw definicji 1000 tabel enterprise oraz krawędzi relacji grafu bezskalowego.
    """
    catalog = get_domain_catalog()
    hubs = set(get_enterprise_hub_tables())

    all_tables: List[str] = []
    tables_by_domain: Dict[str, List[str]] = defaultdict(list)

    for domain, (entities, qualifiers) in catalog.items():
        for entity in entities:
            for qualifier in qualifiers:
                t_name = f"{domain}_{entity}_{qualifier}"
                all_tables.append(t_name)
                tables_by_domain[domain].append(t_name)

    # Konstrukcja topologii grafu
    G, relations = build_enterprise_topology(all_tables, tables_by_domain)

    # Zestawienie wychodzących kluczy obcych dla każdej tabeli źródłowej
    out_relations_by_source: Dict[str, List[ForeignKeyRelation]] = defaultdict(list)
    for rel in relations:
        out_relations_by_source[rel.source_table].append(rel)

    table_definitions: List[EnterpriseTableDefinition] = []

    for domain, (entities, qualifiers) in catalog.items():
        base_cols = build_base_column_sql(domain)

        for entity in entities:
            for qualifier in qualifiers:
                t_name = f"{domain}_{entity}_{qualifier}"
                is_hub = t_name in hubs
                fk_rels = out_relations_by_source.get(t_name, [])

                # Jeśli istnieją relacje wychodzące, upewnijmy się, że kolumny kluczy obcych są w tabeli
                extra_fk_cols: List[str] = []
                for rel in fk_rels:
                    # Jeśli kolumna nie istnieje jeszcze w bazowych kolumnach, dołączamy ją
                    col = rel.source_column
                    if col not in base_cols:
                        extra_fk_cols.append(f"{col} BIGINT")

                all_cols_sql = base_cols.strip()
                if extra_fk_cols:
                    unique_cols = list(dict.fromkeys(extra_fk_cols))
                    all_cols_sql = f"{all_cols_sql},\n            " + ",\n            ".join(unique_cols)

                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS public.{t_name} (
                    {all_cols_sql}
                );
                """

                connected_targets = [rel.target_table for rel in fk_rels]
                description = build_enterprise_description(
                    domain, entity, qualifier, is_hub, connected_targets
                )
                escaped_desc = description.replace("'", "''")
                comment_sql = f"COMMENT ON TABLE public.{t_name} IS '{escaped_desc}';"

                table_definitions.append(
                    EnterpriseTableDefinition(
                        table_name=t_name,
                        domain=domain,
                        description=description,
                        columns_sql=all_cols_sql,
                        create_table_sql=create_table_sql,
                        comment_sql=comment_sql,
                        is_hub=is_hub,
                    )
                )

    return table_definitions, relations, G


# Ładowanie modelu wektorowego SentenceTransformer
def load_embedding_model(model_name: str = "all-MiniLM-L6-v2") -> SentenceTransformer:
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info("Ładowanie modelu wektorowego '%s' na urządzeniu: %s...", model_name, device)
    model = SentenceTransformer(model_name, device=device)
    logger.info("Model '%s' załadowany pomyślnie.", model_name)
    return model


# Zasilenie bazy danych strukturą DDL, metadanymi relacji i wektorami semantycznymi
async def execute_enterprise_schema_deployment(
    connection: asyncpg.Connection,
    tables: List[EnterpriseTableDefinition],
    relations: List[ForeignKeyRelation],
    model: SentenceTransformer,
    batch_size: int = 100,
) -> None:
    """
    Wykonuje pełne wdrożenie schematu enterprise w PostgreSQL:
    1. Tworzy tabele DDL oraz komentarze biznesowe.
    2. Dodaje fizyczne ograniczenia kluczy obcych (FOREIGN KEY ... REFERENCES).
    3. Rejestruje graf relacji w tabeli public.schema_foreign_keys.
    4. Wektoryzuje opisy biznesowe i zapisuje do public.schema_embeddings.
    """
    total_tables = len(tables)
    total_batches = (total_tables + batch_size - 1) // batch_size

    logger.info(
        "Krok 1/4: Tworzenie 1000 tabel SQL oraz komentarzy w %d partiach...",
        total_batches,
    )

    upsert_embedding_sql = """
    INSERT INTO public.schema_embeddings (table_name, domain, description, embedding)
    VALUES ($1, $2, $3, $4::vector)
    ON CONFLICT (table_name) DO UPDATE
    SET domain = EXCLUDED.domain,
        description = EXCLUDED.description,
        embedding = EXCLUDED.embedding;
    """

    start_time = time.perf_counter()

    with tqdm(
        total=total_tables,
        desc="Tworzenie tabel i embeddingów",
        unit="tabel",
        ncols=100,
    ) as progress_bar:
        for batch_index in range(total_batches):
            b_start = batch_index * batch_size
            b_end = min(b_start + batch_size, total_tables)
            batch = tables[b_start:b_end]

            descriptions = [t.description for t in batch]
            embeddings = model.encode(
                descriptions,
                batch_size=len(descriptions),
                show_progress_bar=False,
                normalize_embeddings=True,
            )

            embedding_records: List[Tuple[str, str, str, str]] = [
                (t.table_name, t.domain, t.description, str(vec.tolist()))
                for t, vec in zip(batch, embeddings)
            ]

            async with connection.transaction():
                ddl_batch = "\n".join(f"{t.create_table_sql}\n{t.comment_sql}" for t in batch)
                await connection.execute(ddl_batch)
                await connection.executemany(upsert_embedding_sql, embedding_records)

            progress_bar.update(len(batch))

    logger.info(
        "Krok 1/4 zakończony. Utworzono %d tabel w czasie %.2f s.",
        total_tables,
        time.perf_counter() - start_time,
    )

    # tworzenie fizycznych kluczy obcych i rejestracja w schema_foreign_keys
    logger.info(
        "Krok 2/4: Zapis topologii grafu relacji (%d kluczy obcych) w 'public.schema_foreign_keys'...",
        len(relations),
    )

    upsert_fk_sql = """
    INSERT INTO public.schema_foreign_keys (source_table, target_table, constraint_name, join_columns)
    VALUES ($1, $2, $3, $4::jsonb)
    ON CONFLICT (constraint_name) DO UPDATE
    SET source_table = EXCLUDED.source_table,
        target_table = EXCLUDED.target_table,
        join_columns = EXCLUDED.join_columns;
    """

    fk_records = [
        (rel.source_table, rel.target_table, rel.constraint_name, rel.join_columns_json)
        for rel in relations
    ]

    async with connection.transaction():
        await connection.executemany(upsert_fk_sql, fk_records)

    logger.info("Krok 3/4: Aplikowanie fizycznych ograniczeń FOREIGN KEY w schemacie PostgreSQL...")

    # Zastosowanie ograniczeń kluczy obcych
    applied_fks = 0
    with tqdm(
        total=len(relations),
        desc="Aplikowanie kluczy obcych (FK)",
        unit="FK",
        ncols=100,
    ) as fk_pbar:
        # Aplikujemy w transakcyjnych partiach po 50
        fk_batch_size = 50
        for i in range(0, len(relations), fk_batch_size):
            chunk = relations[i : i + fk_batch_size]
            alter_statements: List[str] = []
            for rel in chunk:
                # Dodajemy kolumnę FK jeśli jeszcze nie istnieje, a następnie nakładamy ograniczenie FK
                sql = f"""
                DO $$ BEGIN
                    ALTER TABLE public.{rel.source_table}
                    ADD COLUMN IF NOT EXISTS {rel.source_column} BIGINT;

                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = '{rel.constraint_name}'
                    ) THEN
                        ALTER TABLE public.{rel.source_table}
                        ADD CONSTRAINT {rel.constraint_name}
                        FOREIGN KEY ({rel.source_column})
                        REFERENCES public.{rel.target_table}({rel.target_column})
                        ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
                    END IF;
                END $$;
                """
                alter_statements.append(sql)

            full_chunk_sql = "\n".join(alter_statements)
            async with connection.transaction():
                await connection.execute(full_chunk_sql)

            applied_fks += len(chunk)
            fk_pbar.update(len(chunk))

    logger.info("Krok 3/4 zakończony. Zastosowano %d fizycznych ograniczeń FK.", applied_fks)


async def verify_enterprise_schema(
    connection: asyncpg.Connection,
    G: nx.DiGraph,
) -> None:
    """
    Weryfikuje poprawność utworzonych obiektów w PostgreSQL, bada stopnie Hubów
    oraz weryfikuje rozkład stopni w grafie bezskalowym.
    """
    logger.info("Krok 4/4: Weryfikacja spójności bazy danych i topologii grafu...")

    table_count = await connection.fetchval(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name NOT IN ('schema_embeddings', 'schema_foreign_keys', 'analytics_orders');
        """
    )

    embeddings_count = await connection.fetchval("SELECT count(*) FROM public.schema_embeddings;")
    fk_count = await connection.fetchval("SELECT count(*) FROM public.schema_foreign_keys;")

    logger.info("--- PODSUMOWANIE GENERATORA ENTERPRISE ---")
    logger.info("Liczba utworzonych tabel domenowych:   %d (oczekiwano 1000)", table_count)
    logger.info("Liczba wektorów w schema_embeddings: %d (oczekiwano 1000)", embeddings_count)
    logger.info("Liczba relacji w schema_foreign_keys: %d", fk_count)

    hubs = get_enterprise_hub_tables()
    logger.info("Weryfikacja stopni 20 centralnych Hubów (wymóg: 15 <= stopień <= 40):")
    invalid_hubs = []
    for h in hubs:
        deg = G.degree(h)
        in_deg = G.in_degree(h)
        out_deg = G.out_degree(h)
        logger.info(
            "  Hub '%s': łączny stopień=%d (in=%d, out=%d)",
            h, deg, in_deg, out_deg
        )
        if not (15 <= deg <= 40):
            invalid_hubs.append((h, deg))

    if invalid_hubs:
        logger.error("Błąd: następuje naruszenie zakresu stopni hubów: %s", invalid_hubs)
        raise RuntimeError(f"Huby poza zakresem stopni [15, 40]: {invalid_hubs}")

    degree_counts = Counter(dict(G.degree()).values())
    logger.info("Rozkład potęgowy stopni wierzchołków w sieci bezskalowej:")
    for deg_val, cnt in sorted(degree_counts.items(), key=lambda x: x[0]):
        logger.info("  Stopień %2d: %4d tabel", deg_val, cnt)

    if table_count != 1000 or embeddings_count != 1000:
        raise RuntimeError("Niezgodność liczby tabel lub embeddingów!")

    logger.info("Weryfikacja zakończona pełnym sukcesem!")


async def main() -> None:
    logger.info("Start generatora enterprise schema (Scale-Free Network Topology)...")

    config = DatabaseConfig()
    connection = await create_database_connection(config)

    try:
        await initialize_enterprise_metadata_tables(connection)

        logger.info("Generowanie topologii grafu bezskalowego i definicji 1000 tabel...")
        tables, relations, G = generate_enterprise_schema_definitions()
        logger.info("Wygenerowano %d definicji tabel oraz %d relacji FK.", len(tables), len(relations))

        # Model embeddingowy
        model = load_embedding_model("all-MiniLM-L6-v2")

        # Wdrożenie do bazy
        await execute_enterprise_schema_deployment(connection, tables, relations, model, batch_size=100)

        await verify_enterprise_schema(connection, G)

        # Deterministyczne zasilenie 1000 tabel relacyjnych danymi biznesowymi (10-50 rekordów)
        logger.info("Deterministyczne zasilanie 1000 tabel relacyjnych (seed_enterprise_tables)...")
        try:
            from seed_enterprise_tables import seed_enterprise_tables
        except ImportError:
            current_script_dir = Path(__file__).resolve().parent
            spec_seed = importlib.util.spec_from_file_location("seed_enterprise_tables", current_script_dir / "seed_enterprise_tables.py")
            seed_mod = importlib.util.module_from_spec(spec_seed)
            spec_seed.loader.exec_module(seed_mod)
            seed_enterprise_tables = seed_mod.seed_enterprise_tables

        await seed_enterprise_tables(
            connection=connection,
            G=G,
            tables=tables,
            relations=relations,
            min_records=10,
            max_records=50,
            random_seed=42,
        )

        logger.info("Wdrożenie korporacyjnego schematu bazy danych i zasilenie danymi zakończone pomyślnie!")
    finally:
        await connection.close()
        logger.info("Połączenie z bazą danych zamknięte.")


if __name__ == "__main__":
    asyncio.run(main())
