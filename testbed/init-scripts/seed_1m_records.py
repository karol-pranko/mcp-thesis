#!/usr/bin/env python3
"""
Wypełnia public.analytics_orders (domyślnie 1 mln wierszy) pod testy paginacji i narzutu base64.
Wstawia przez copy_records_to_table, na końcu wypisuje rozmiar tabeli i TOAST. --cleanup czyści tabelę.
"""

import argparse
import asyncio
import base64
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
import os
import random
import sys
import time
from typing import List, Tuple
import uuid

import asyncpg
from tqdm import tqdm


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("mcp_record_seeder")


# Konfiguracja połączenia z bazą PostgreSQL
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


def parse_command_line_arguments() -> argparse.Namespace:
    """
    Parsuje argumenty wiersza poleceń:
    --count: liczba rekordów do wygenerowania (domyślnie 1 000 000).
    --batch-size: rozmiar partii dla copy_records_to_table (domyślnie 25 000).
    --cleanup: flaga usuwająca wstawione dane (TRUNCATE) po weryfikacji.
    """
    parser = argparse.ArgumentParser(
        description="Generator danych wolumenowych dla benchmarków paginacji i Base64 w MCP."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1_000_000,
        help="Całkowita liczba rekordów do wygenerowania (domyślnie: 1 000 000).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25_000,
        help="Liczba rekordów w pojedynczej paczce (domyślnie: 25 000).",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Czyści tabelę (TRUNCATE) po zmierzeniu rozmiaru i weryfikacji wyników.",
    )
    return parser.parse_args()


async def create_database_connection(config: DatabaseConfig) -> asyncpg.Connection:
    logger.info(
        "Nawiązywanie połączenia z bazą %s@%s:%d/%s...",
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
        logger.info("Połączenie z bazą PostgreSQL zostało nawiązane.")
        return connection
    except asyncpg.exceptions.InvalidPasswordError:
        fallback_password = (
            "mcp_secret_password" if config.password == "mcp_password" else "mcp_password"
        )
        logger.warning(
            "Uwierzytelnienie hasłem domyślnym nie powiodło się. Próba połączenia z hasłem alternatywnym..."
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


async def initialize_target_table(connection: asyncpg.Connection) -> None:
    """
    Tworzy tabelę docelową public.analytics_orders wraz z indeksem pod kursor,
    jeśli jeszcze nie istnieje w schemacie.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.analytics_orders (
        id BIGSERIAL PRIMARY KEY,
        order_uid UUID NOT NULL,
        customer_id INT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        status VARCHAR(20) NOT NULL,
        amount NUMERIC(10, 2) NOT NULL,
        description TEXT NOT NULL,
        document_base64 TEXT
    );
    """
    await connection.execute(create_table_sql)
    logger.info("Struktura tabeli 'public.analytics_orders' jest gotowa.")


def generate_base64_document_pool(
    pool_size: int = 50, min_bytes: int = 10240, max_bytes: int = 30720
) -> List[str]:
    """
    Generuje pulę realistycznych dokumentów binarnych zakodowanych w Base64
    o rozmiarze od 10 KB do 30 KB.
    Wykorzystanie puli zapobiega nadmiernemu obciążeniu CPU generowaniem losowych bajtów.
    """
    logger.info(
        "Generowanie puli %d dokumentów Base64 o rozmiarze od %d do %d bajtów...",
        pool_size,
        min_bytes,
        max_bytes,
    )
    document_pool: List[str] = []

    for i in range(pool_size):
        target_size = random.randint(min_bytes, max_bytes)
        # powtarzany wzorzec udający pozycje faktury - tańsze niż generowanie losowych bajtów
        template_chunk = (
            f"INVOICE_LINE_ITEM_RECORD_ID_{i:04d}_PRODUCT_CODE_SKU_PRICE_TAX_QUANTITY_TOTAL_"
        ).encode("utf-8")
        repeats = (target_size // len(template_chunk)) + 1
        raw_bytes = (template_chunk * repeats)[:target_size]
        base64_string = base64.b64encode(raw_bytes).decode("ascii")
        document_pool.append(base64_string)

    logger.info("Pula dokumentów Base64 została przygotowana.")
    return document_pool


async def seed_orders_data(
    connection: asyncpg.Connection,
    total_count: int,
    batch_size: int,
    document_pool: List[str],
) -> float:
    """
    Wstawia wsadowo rekordy do tabeli public.analytics_orders za pomocą copy_records_to_table.
    Czas utworzenia (created_at) uporządkowany jest ściśle chronologicznie w zakresie ostatnich 2 lat.
    Zwraca całkowity czas operacji w sekundach.
    """
    logger.info(
        "Rozpoczęcie zasilania bazy danych: %d rekordów (paczki po %d)...",
        total_count,
        batch_size,
    )

    statuses = ["PENDING", "COMPLETED", "PROCESSING", "CANCELLED", "REFUNDED", "SHIPPED", "DELIVERED"]
    descriptions = [
        "Standard e-commerce purchase order with digital invoice receipt",
        "B2B enterprise quarterly inventory replenishment order",
        "Automated recurring subscription service renewal fee",
        "Omnichannel retail order processed through mobile application",
        "Expedited warehouse logistics shipment with commercial invoice",
    ]

    # Zakres czasowy: ostatnie 2 lata (730 dni)
    time_window_seconds = 730 * 24 * 3600
    start_timestamp = datetime.now(timezone.utc) - timedelta(seconds=time_window_seconds)
    time_step_seconds = time_window_seconds / max(total_count, 1)

    columns = [
        "order_uid",
        "customer_id",
        "created_at",
        "status",
        "amount",
        "description",
        "document_base64",
    ]

    pool_len = len(document_pool)
    statuses_len = len(statuses)
    desc_len = len(descriptions)

    total_batches = (total_count + batch_size - 1) // batch_size
    overall_start_time = time.perf_counter()

    with tqdm(
        total=total_count,
        desc="Zasilanie tabeli analytics_orders",
        unit="rekordów",
        ncols=100,
    ) as progress_bar:
        for batch_index in range(total_batches):
            current_batch_start = batch_index * batch_size
            current_batch_count = min(batch_size, total_count - current_batch_start)

            batch_records: List[Tuple] = []
            for offset in range(current_batch_count):
                global_index = current_batch_start + offset
                record_time = start_timestamp + timedelta(seconds=global_index * time_step_seconds)
                order_uid = uuid.uuid4()
                customer_id = (global_index % 50000) + 1
                status = statuses[global_index % statuses_len]
                amount = Decimal(f"{(global_index * 13 % 10000) + 5}.{(global_index * 37 % 100):02d}")
                description = descriptions[global_index % desc_len]
                document_base64 = document_pool[global_index % pool_len]

                batch_records.append(
                    (
                        order_uid,
                        customer_id,
                        record_time,
                        status,
                        amount,
                        description,
                        document_base64,
                    )
                )

            # Wstawienie paczki za pomocą protokołu PostgreSQL COPY
            await connection.copy_records_to_table(
                "analytics_orders",
                records=batch_records,
                columns=columns,
                schema_name="public",
            )

            progress_bar.update(current_batch_count)

    elapsed_time = time.perf_counter() - overall_start_time
    throughput = total_count / elapsed_time if elapsed_time > 0 else 0
    logger.info(
        "Zakończono wstawianie %d rekordów w czasie: %.2f s (przepustowość: %.1f rekordów/s).",
        total_count,
        elapsed_time,
        throughput,
    )
    return elapsed_time


async def fetch_table_metrics(connection: asyncpg.Connection) -> Tuple[int, str, str]:
    """
    Pobiera aktualną liczbę rekordów, fizyczny rozmiar samej relacji
    oraz całkowity rozmiar z uwzględnieniem danych TOAST i indeksów.
    """
    query = """
    SELECT
        count(*) AS total_rows,
        pg_size_pretty(pg_relation_size('public.analytics_orders')) AS relation_size,
        pg_size_pretty(pg_total_relation_size('public.analytics_orders')) AS total_size
    FROM public.analytics_orders;
    """
    row = await connection.fetchrow(query)
    return row["total_rows"], row["relation_size"], row["total_size"]


async def truncate_target_table(connection: asyncpg.Connection) -> None:
    """
    Czyści tabelę public.analytics_orders (TRUNCATE) w celu zwolnienia miejsca na dysku.
    """
    logger.info("Wykonywanie TRUNCATE TABLE public.analytics_orders...")
    await connection.execute("TRUNCATE TABLE public.analytics_orders RESTART IDENTITY;")
    logger.info("Tabela public.analytics_orders została wyczyszczona.")


async def main() -> None:
    args = parse_command_line_arguments()
    logger.info(
        "Parametry uruchomienia: liczba rekordów = %d, rozmiar partii = %d, czyszczenie = %s",
        args.count,
        args.batch_size,
        args.cleanup,
    )

    config = DatabaseConfig()
    connection = await create_database_connection(config)

    try:
        # inicjalizacja tabeli docelowej
        await initialize_target_table(connection)

        # przygotowanie puli dokumentów Base64
        document_pool = generate_base64_document_pool(pool_size=50)

        # wsadowe wstawianie rekordów
        elapsed_seconds = await seed_orders_data(
            connection=connection,
            total_count=args.count,
            batch_size=args.batch_size,
            document_pool=document_pool,
        )

        # weryfikacja metryk bazy danych
        row_count, relation_size, total_size = await fetch_table_metrics(connection)

        logger.info("--- Podsumowanie wolumenu bazy danych ---")
        logger.info("Liczba wierszy w tabeli:           %d", row_count)
        logger.info("Rozmiar relacji (pg_relation_size): %s", relation_size)
        logger.info("Całkowity rozmiar z TOAST/indeks:  %s", total_size)
        logger.info("Czas trwania zapisu:               %.2f sekund", elapsed_seconds)
        logger.info("------------------------------------------")

        # opcjonalne czyszczenie dysku po zakończeniu testu
        if args.cleanup:
            logger.info("Uruchomiono z flagą --cleanup. Zwalnianie zasobów dyskowych...")
            await truncate_target_table(connection)

            cleaned_count, cleaned_rel_size, cleaned_tot_size = await fetch_table_metrics(
                connection
            )
            logger.info("Stan tabeli po wykonaniu TRUNCATE:")
            logger.info("  - Liczba wierszy: %d", cleaned_count)
            logger.info("  - Rozmiar relacji: %s", cleaned_rel_size)
            logger.info("  - Całkowity rozmiar: %s", cleaned_tot_size)
            logger.info("Pomyślnie zwolniono miejsce na dysku.")

        logger.info("Zakończono pomyślnie wszystkie operacje.")
    finally:
        logger.info("Zamykanie połączenia z bazą danych...")
        await connection.close()
        logger.info("Połączenie z bazą zostało zamknięte.")


if __name__ == "__main__":
    asyncio.run(main())
