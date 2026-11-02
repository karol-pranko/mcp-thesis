#!/usr/bin/env python3
"""
Wypełnia 1000 tabel testowych danymi (10-50 wierszy na tabelę, random.seed(42)).

Żeby FK się zgadzały, tabele idą w odwróconym porządku topologicznym grafu (najpierw te,
do których inne się odwołują), a w transakcji i tak jest SET CONSTRAINTS ALL DEFERRED.
Wstawianie paczkami tabel; na końcu setval dla sekwencji BIGSERIAL.
"""

import argparse
import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
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
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("seed_enterprise_tables")


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


def load_schema_generator_module() -> Any:
    """Dynamicznie ładuje moduł generate_enterprise_schema.py z katalogu nadrzędnego/bieżącego."""
    current_dir = Path(__file__).resolve().parent
    target_path = current_dir / "generate_enterprise_schema.py"
    if not target_path.exists():
        # Fallback jeśli uruchamiany z katalogu głównego
        target_path = Path("testbed/init-scripts/generate_enterprise_schema.py").resolve()

    if not target_path.exists():
        raise FileNotFoundError(f"Nie odnaleziono modułu generatora schematu: {target_path}")

    spec = importlib.util.spec_from_file_location("generate_enterprise_schema", target_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Nie można załadować modułu {target_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


async def create_database_connection(config: DatabaseConfig) -> asyncpg.Connection:
    logger.info(
        "Nawiązywanie połączenia z bazą PostgreSQL: %s@%s:%d/%s...",
        config.user,
        config.host,
        config.port,
        config.database,
    )
    try:
        conn = await asyncpg.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
        )
        logger.info("Połączenie z bazą PostgreSQL zostało ustanowione.")
        return conn
    except asyncpg.exceptions.InvalidPasswordError:
        fallback_password = (
            "mcp_secret_password" if config.password == "mcp_password" else "mcp_password"
        )
        logger.warning(
            "Błąd autoryzacji hasła dla '%s'. Próba połączenia hasłem alternatywnym...",
            config.user,
        )
        conn = await asyncpg.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=fallback_password,
            database=config.database,
        )
        logger.info("Połączenie ustanowione pomyślnie z hasłem alternatywnym.")
        return conn


def generate_rows_for_table(
    table_name: str,
    domain: str,
    num_records: int,
    table_idx: int,
    outgoing_fks: List[Any],
    table_record_counts: Dict[str, int],
    rnd: random.Random,
) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    """
    Generuje deterministyczne rekordy dla pojedynczej tabeli z zachowaniem integralności FK.
    Zwraca krotkę: (lista_kolumn, lista_krotek_wartości).
    """
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # kolumny bazowe w zależności od domeny biznesowej
    if domain == "finance":
        col_names = [
            "id", "transaction_code", "account_id", "amount", "currency",
            "status", "metadata", "created_at"
        ]
        statuses = ["PENDING", "COMPLETED", "SETTLED", "RECONCILED", "ACTIVE"]
        currencies = ["USD", "EUR", "GBP", "PLN"]

        base_rows = []
        for i in range(1, num_records + 1):
            t_code = f"TXN_{table_idx:04d}_{i:05d}"
            acc_id = 1000 + (i % 200)
            amt = Decimal(str(round(rnd.uniform(10.0, 50000.0), 2)))
            curr = currencies[i % len(currencies)]
            stat = statuses[i % len(statuses)]
            meta = json.dumps({"audit": True, "seed": 42, "row": i})
            c_at = base_date + timedelta(minutes=i * 15)
            base_rows.append([i, t_code, acc_id, amt, curr, stat, meta, c_at])

    elif domain == "inventory":
        col_names = [
            "id", "sku", "warehouse_id", "quantity", "unit_cost",
            "batch_number", "attributes", "created_at"
        ]
        base_rows = []
        for i in range(1, num_records + 1):
            sku_val = f"SKU-{table_idx:03d}-{i:05d}"
            w_id = 1 + (i % 15)
            qty = 10 + (i % 300)
            cost = Decimal(str(round(rnd.uniform(1.0, 1500.0), 4)))
            batch = f"BATCH-{100 + (i % 900)}"
            attr = json.dumps({"grade": "A", "perishable": False, "row": i})
            c_at = base_date + timedelta(minutes=i * 15)
            base_rows.append([i, sku_val, w_id, qty, cost, batch, attr, c_at])

    elif domain == "hr":
        col_names = [
            "id", "employee_id", "department_code", "position_title",
            "status", "effective_date", "details", "created_at"
        ]
        depts = ["FIN", "ENG", "HR", "OPS", "SALES", "EXEC"]
        positions = ["Senior Specialist", "Team Lead", "Director", "Analyst", "Engineer"]
        statuses = ["ACTIVE", "ON_LEAVE", "PROBATION", "ACTIVE", "COMPLETED"]

        base_rows = []
        for i in range(1, num_records + 1):
            emp_id = 10000 + (i % 500)
            dept = depts[i % len(depts)]
            pos = positions[i % len(positions)]
            stat = statuses[i % len(statuses)]
            eff_date = (datetime(2023, 1, 1) + timedelta(days=i * 5)).date()
            dtl = json.dumps({"level": 1 + (i % 5), "row": i})
            c_at = base_date + timedelta(minutes=i * 15)
            base_rows.append([i, emp_id, dept, pos, stat, eff_date, dtl, c_at])

    elif domain == "crm":
        col_names = [
            "id", "customer_id", "stage", "deal_value", "assigned_agent_id",
            "lead_source", "properties", "created_at"
        ]
        stages = ["PROSPECT", "QUALIFIED", "PROPOSAL", "NEGOTIATION", "CLOSED_WON"]
        sources = ["INBOUND", "OUTBOUND", "REFERRAL", "PARTNER", "WEB"]

        base_rows = []
        for i in range(1, num_records + 1):
            cust_id = 5000 + (i % 300)
            stg = stages[i % len(stages)]
            val = Decimal(str(round(rnd.uniform(500.0, 250000.0), 2)))
            agent_id = 1 + (i % 40)
            src = sources[i % len(sources)]
            prop = json.dumps({"priority": "HIGH", "score": 60 + (i % 40)})
            c_at = base_date + timedelta(minutes=i * 15)
            base_rows.append([i, cust_id, stg, val, agent_id, src, prop, c_at])

    elif domain == "telemetry":
        col_names = [
            "id", "device_id", "sensor_type", "metric_value", "unit_of_measure",
            "alert_level", "payload", "recorded_at"
        ]
        sensors = ["temperature", "pressure", "vibration", "voltage", "current"]
        uoms = ["C", "bar", "Hz", "V", "A"]
        alerts = ["INFO", "INFO", "WARN", "INFO", "CRITICAL"]

        base_rows = []
        for i in range(1, num_records + 1):
            dev_id = f"DEV-{table_idx:03d}-{i:04d}"
            s_type = sensors[i % len(sensors)]
            m_val = round(float(rnd.uniform(10.0, 99.9)), 3)
            uom = uoms[i % len(uoms)]
            alrt = alerts[i % len(alerts)]
            pld = json.dumps({"firmware": "v2.1", "uptime_sec": i * 3600})
            c_at = base_date + timedelta(minutes=i * 15)
            base_rows.append([i, dev_id, s_type, m_val, uom, alrt, pld, c_at])

    else:
        col_names = ["id", "entity_key", "status", "payload", "created_at"]
        base_rows = []
        for i in range(1, num_records + 1):
            e_key = f"ENT_{table_idx:04d}_{i:05d}"
            stat = "ACTIVE"
            pld = json.dumps({"row": i})
            c_at = base_date + timedelta(minutes=i * 15)
            base_rows.append([i, e_key, stat, pld, c_at])

    # dołączanie dodatkowych kolumn kluczy obcych (FK) z zapewnieniem spójności
    # Każdy klucz obcy wskazuje na istniejący rekord w tabeli docelowej (1..N_target)
    # Grupowanie po nazwie kolumny, aby w przypadku relacji współdzielonych wybrać bezpieczne minimum
    fks_by_col: Dict[str, List[Any]] = defaultdict(list)
    for rel in outgoing_fks:
        fks_by_col[rel.source_column].append(rel)

    for col, rels in fks_by_col.items():
        min_tgt_count = min(table_record_counts.get(r.target_table, 10) for r in rels)

        if col in col_names:
            # Kolumna już istnieje w bazie (np. account_id) - aktualizujemy wartości na poprawne FK
            col_pos = col_names.index(col)
            for row_idx, r in enumerate(base_rows):
                r[col_pos] = (row_idx % min_tgt_count) + 1
        else:
            # Nowa kolumna FK dodana do schematu tabeli
            col_names.append(col)
            for row_idx, r in enumerate(base_rows):
                fk_val = (row_idx % min_tgt_count) + 1
                r.append(fk_val)

    tuple_rows = [tuple(r) for r in base_rows]
    return col_names, tuple_rows


async def seed_enterprise_tables(
    connection: asyncpg.Connection,
    G: Optional[nx.DiGraph] = None,
    tables: Optional[List[Any]] = None,
    relations: Optional[List[Any]] = None,
    min_records: int = 10,
    max_records: int = 50,
    random_seed: int = 42,
    batch_size_tables: int = 50,
    truncate_first: bool = False,
    is_dry_run: bool = False,
) -> int:
    t_start = time.perf_counter()
    logger.info("=== START DETERMINISTYCZNEGO SEEDERA DANYCH DLA 1000 TABEL ===")
    logger.info("Parametry: min_records=%d, max_records=%d, seed=%d, dry_run=%s",
                min_records, max_records, random_seed, is_dry_run)

    # przygotowanie definicji schematu i grafu zależności DAG
    if tables is None or relations is None or G is None:
        logger.info("Ładowanie definicji 1000 tabel i grafu relacji z generatora schematu...")
        ges = load_schema_generator_module()
        tables, relations, G = ges.generate_enterprise_schema_definitions()

    # przy cyklu w FK nie da się ustalić kolejności wstawiania
    if not nx.is_directed_acyclic_graph(G):
        logger.warning("Uwaga: Graf relacji zawiera cykle! Użycie standardowego porządku z odroczeniem więzów.")
        insertion_order = [t.table_name for t in tables]
    else:
        # W G krawędź to (source_table -> target_table), co oznacza, że source ma FK do target.
        # Aby zachować integralność, target musi posiadać rekordy przed source.
        # Dlatego kolejność wstawiania to odwrócony sort topologiczny:
        insertion_order = list(reversed(list(nx.topological_sort(G))))
        logger.info("Pomyślnie wyznaczono porządek topologiczny DAG dla %d tabel.", len(insertion_order))

    # Opcjonalne ograniczenie próby w trybie dry-run
    if is_dry_run:
        logger.info("Tryb --dry-run: ograniczenie do 10 tabel testowych z próby topologicznej.")
        insertion_order = insertion_order[:10]

    # Mapowanie definicji tabel i kluczy obcych
    table_def_map = {t.table_name: t for t in tables}
    outgoing_fks_by_source: Dict[str, List[Any]] = defaultdict(list)
    for rel in relations:
        outgoing_fks_by_source[rel.source_table].append(rel)

    # wyznaczenie liczby rekordów dla każdej tabeli (determinizm: seed 42)
    rnd = random.Random(random_seed)
    table_record_counts: Dict[str, int] = {}
    for t_name in insertion_order:
        table_record_counts[t_name] = rnd.randint(min_records, max_records)

    # opcjonalne wyczyszczenie danych przed zasileniem (TRUNCATE CASCADE)
    if truncate_first and not is_dry_run:
        logger.info("Czyszczenie istniejących danych (TRUNCATE) w 1000 tabelach...")
        async with connection.transaction():
            table_list_str = ", ".join(f"public.{t}" for t in insertion_order)
            await connection.execute(f"TRUNCATE TABLE {table_list_str} CASCADE;")
        logger.info("Tabele zostały pomyślnie wyczyszczone.")

    # wstawianie wsadowe w transakcjach po batch_size_tables
    total_inserted = 0
    total_tables_count = len(insertion_order)
    num_batches = (total_tables_count + batch_size_tables - 1) // batch_size_tables

    logger.info("Rozpoczynanie zasilania wsadowego %d tabel w %d partiach transakcyjnych...",
                total_tables_count, num_batches)

    with tqdm(total=total_tables_count, desc="Zasilanie tabel", unit="tabel", ncols=100) as pbar:
        for batch_idx in range(num_batches):
            b_start = batch_idx * batch_size_tables
            b_end = min(b_start + batch_size_tables, total_tables_count)
            batch_tables = insertion_order[b_start:b_end]

            async with connection.transaction():
                # Odroczenie sprawdzania ograniczeń kluczy obcych do momentu zatwierdzenia transakcji
                try:
                    await connection.execute("SET CONSTRAINTS ALL DEFERRED;")
                except Exception as defer_exc:
                    logger.debug("SET CONSTRAINTS ALL DEFERRED pominięte: %s", defer_exc)

                for table_idx_global, t_name in enumerate(batch_tables, start=b_start + 1):
                    t_def = table_def_map.get(t_name)
                    domain = t_def.domain if t_def else t_name.split("_")[0]
                    num_rows = table_record_counts[t_name]
                    out_fks = outgoing_fks_by_source.get(t_name, [])

                    col_names, tuple_rows = generate_rows_for_table(
                        table_name=t_name,
                        domain=domain,
                        num_records=num_rows,
                        table_idx=table_idx_global,
                        outgoing_fks=out_fks,
                        table_record_counts=table_record_counts,
                        rnd=rnd,
                    )

                    # Konstrukcja wsadowego zapytania INSERT z parametrami
                    # INSERT INTO public.table (col1, col2) VALUES ($1, $2), ($3, $4) ...
                    cols_str = ", ".join(col_names)
                    param_placeholders: List[str] = []
                    flattened_params: List[Any] = []
                    param_counter = 1

                    for row in tuple_rows:
                        row_holders = []
                        for val in row:
                            row_holders.append(f"${param_counter}")
                            flattened_params.append(val)
                            param_counter += 1
                        param_placeholders.append(f"({', '.join(row_holders)})")

                    values_sql = ",\n                        ".join(param_placeholders)
                    insert_sql = f"""
                    INSERT INTO public.{t_name} ({cols_str})
                    VALUES
                        {values_sql}
                    ON CONFLICT (id) DO NOTHING;
                    """

                    await connection.execute(insert_sql, *flattened_params)

                    # Aktualizacja sekwencji klucza głównego BIGSERIAL
                    seq_update_sql = f"""
                    SELECT setval(
                        pg_get_serial_sequence('public.{t_name}', 'id'),
                        COALESCE((SELECT max(id) FROM public.{t_name}), 1),
                        true
                    );
                    """
                    try:
                        await connection.execute(seq_update_sql)
                    except Exception:
                        pass

                    total_inserted += len(tuple_rows)
                    pbar.update(1)

    elapsed = time.perf_counter() - t_start
    logger.info(
        "=== ZASILANIE ZAKOŃCZONE SUKCESEM W CZASIE %.2f s ===",
        elapsed,
    )
    logger.info("Łączna liczba zasilonych tabel:    %d", total_tables_count)
    logger.info("Łączna liczba wstawionych rekordów: %d", total_inserted)
    logger.info("Średnia liczba rekordów na tabelę:  %.1f", total_inserted / max(1, total_tables_count))

    # szybka weryfikacja losowych 5 tabel hubowych
    hub_samples = [
        "finance_ledger_entries_daily",
        "crm_client_accounts_enterprise",
        "hr_employee_profiles_annual",
        "inventory_purchase_orders_raw_materials",
        "telemetry_host_servers_datacenter_a",
    ]
    logger.info("Weryfikacja integralności przykładowych tabel Hub:")
    for h in hub_samples:
        if h in table_record_counts:
            cnt = await connection.fetchval(f"SELECT count(*) FROM public.{h};")
            logger.info("  Tabela '%s': %d rekordów (oczekiwano >= 10)", h, cnt)
            if cnt == 0 and not is_dry_run:
                raise RuntimeError(f"Błąd integralności: tabela {h} jest pusta!")

    return total_inserted


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deterministyczny seeder danych relacyjnych dla 1000 tabel (analytics_db)."
    )
    parser.add_argument(
        "--min-records",
        type=int,
        default=10,
        help="Minimalna liczba rekordów na tabelę (domyślnie: 10).",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=50,
        help="Maksymalna liczba rekordów na tabelę (domyślnie: 50).",
    )
    parser.add_argument(
        "--rows-per-table",
        type=int,
        default=None,
        help="Stała liczba rekordów na każdą tabelę (nadpisuje --min-records/--max-records; konfiguracja rundy: data.rows_per_table).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Stałe ziarno losowości dla pełnej deterministyczności (domyślnie: 42).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Liczba tabel w pojedynczym bloku transakcyjnym (domyślnie: 50).",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Czyści istniejące rekordy przed zasileniem (TRUNCATE CASCADE).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Tryb szybkiej weryfikacji: zasila tylko 10 tabel testowych.",
    )
    parser.add_argument(
        "--db-host",
        type=str,
        default=os.getenv("POSTGRES_HOST", "localhost"),
        help="Host bazy PostgreSQL.",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
        help="Port bazy PostgreSQL.",
    )
    parser.add_argument(
        "--db-user",
        type=str,
        default=os.getenv("POSTGRES_USER", "mcp_user"),
        help="Użytkownik bazy PostgreSQL.",
    )
    parser.add_argument(
        "--db-pass",
        type=str,
        default=os.getenv("POSTGRES_PASSWORD", "mcp_secret_password"),
        help="Hasło bazy PostgreSQL.",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default=os.getenv("POSTGRES_DB", "analytics_db"),
        help="Nazwa bazy PostgreSQL.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_arguments()
    config = DatabaseConfig(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name,
    )

    connection = await create_database_connection(config)
    try:
        await seed_enterprise_tables(
            connection=connection,
            min_records=args.rows_per_table or args.min_records,
            max_records=args.rows_per_table or args.max_records,
            random_seed=args.seed,
            batch_size_tables=args.batch_size,
            truncate_first=args.truncate,
            is_dry_run=args.dry_run,
        )
    finally:
        await connection.close()
        logger.info("Połączenie z bazą danych PostgreSQL zostało zamknięte.")


if __name__ == "__main__":
    asyncio.run(main())
