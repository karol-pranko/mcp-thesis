#!/usr/bin/env python3
"""
Serwer MCP (FastMCP) - "Smart-BigData-Gateway".

Pula asyncpg i konsument Kafki żyją w lifespan serwera, a narzędzia poniżej
to wszystko, co widzi agent: ping, SELECT-y, wyszukiwanie schematu, paginacja i OOB.
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, AsyncIterator, Dict, List, Optional

# przy odpaleniu `python server.py` importy pakietowe nie działają, stąd dopisanie ścieżek
_project_root = str(Path(__file__).resolve().parent.parent.parent)
_current_dir = str(Path(__file__).resolve().parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

import asyncpg
from fastmcp import Context, FastMCP


# logi na stderr - stdout jest zajęty przez transport stdio MCP
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("mcp_server")


@dataclass
class DatabaseConfig:
    """Konfiguracja z env. POSTGRES_* ma pierwszeństwo, DB_* to zapasowe nazwy."""

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
    min_size: int = 2
    max_size: int = 10


# potrzebne poza kontekstem żądania (np. w testach, gdzie nie ma ctx)
database_pool: Optional[asyncpg.Pool] = None


async def create_database_pool(config: DatabaseConfig) -> asyncpg.Pool:
    """
    Tworzy pulę asyncpg. Jak hasło nie pasuje, próbujemy drugiego - docker-compose
    z testbed/ ustawia mcp_secret_password, a domyślne w kodzie jest mcp_password.
    """
    logger.info(
        "Tworzenie puli połączeń z bazą %s@%s:%d/%s (min_size=%d, max_size=%d)...",
        config.user,
        config.host,
        config.port,
        config.database,
        config.min_size,
        config.max_size,
    )

    try:
        pool = await asyncpg.create_pool(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            min_size=config.min_size,
            max_size=config.max_size,
        )
        logger.info("Pula połączeń asyncpg została pomyślnie zainicjalizowana.")
        return pool
    except asyncpg.exceptions.InvalidPasswordError:
        fallback_password = (
            "mcp_secret_password" if config.password == "mcp_password" else "mcp_password"
        )
        logger.warning(
            "Uwierzytelnienie hasłem domyślnym nie powiodło się. Próba połączenia hasłem rezerwowym..."
        )
        pool = await asyncpg.create_pool(
            host=config.host,
            port=config.port,
            user=config.user,
            password=fallback_password,
            database=config.database,
            min_size=config.min_size,
            max_size=config.max_size,
        )
        logger.info("Pula połączeń została pomyślnie utworzona przy użyciu hasła rezerwowego.")
        return pool


def get_database_pool(ctx: Optional[Context] = None) -> asyncpg.Pool:
    """Pula z kontekstu lifespan, a jak go nie ma - globalna."""
    if ctx and ctx.lifespan_context and "database_pool" in ctx.lifespan_context:
        return ctx.lifespan_context["database_pool"]

    if database_pool is not None:
        return database_pool

    raise RuntimeError(
        "Pula połączeń z bazą danych nie została zainicjalizowana. Upewnij się, że cykl życia serwera wystartował."
    )


# trzy warianty importu: jako pakiet, z katalogu głównego repo i jako luźny skrypt
try:
    from .streaming import streaming_consumer
    from .schema_engine import schema_engine
    from .pagination import fetch_paginated_data, serialize_database_record
    from .out_of_band import data_dispatcher, TransferMode, read_posix_shm
    from .security import (
        SqlSanitizer,
        ValidationResult,
        async_validate_query,
        encapsulate_untrusted_database_records,
        enforce_limit,
        get_process_pool,
        prevent_multi_statements,
        shutdown_process_pool,
        sql_sanitizer,
        validate_read_only,
        wrap_in_context_delimiters,
    )
except (ImportError, ValueError):
    try:
        from src.mcp_server.streaming import streaming_consumer
        from src.mcp_server.schema_engine import schema_engine
        from src.mcp_server.pagination import fetch_paginated_data, serialize_database_record
        from src.mcp_server.out_of_band import data_dispatcher, TransferMode, read_posix_shm
        from src.mcp_server.security import (
            SqlSanitizer,
            ValidationResult,
            async_validate_query,
            encapsulate_untrusted_database_records,
            enforce_limit,
            get_process_pool,
            prevent_multi_statements,
            shutdown_process_pool,
            sql_sanitizer,
            validate_read_only,
            wrap_in_context_delimiters,
        )
    except (ImportError, ValueError):
        from streaming import streaming_consumer
        from schema_engine import schema_engine
        from pagination import fetch_paginated_data, serialize_database_record
        from out_of_band import data_dispatcher, TransferMode, read_posix_shm
        from security import (
            SqlSanitizer,
            ValidationResult,
            async_validate_query,
            encapsulate_untrusted_database_records,
            enforce_limit,
            get_process_pool,
            prevent_multi_statements,
            shutdown_process_pool,
            sql_sanitizer,
            validate_read_only,
            wrap_in_context_delimiters,
        )


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[Dict[str, Any]]:
    """Start: pula DB, konsument Kafki, pula procesów AST. Na końcu zamykamy wszystko w odwrotnej kolejności."""
    global database_pool
    logger.info("Rozpoczynanie cyklu życia serwera MCP: '%s'...", server.name)

    config = DatabaseConfig()
    database_pool = await create_database_pool(config)

    await streaming_consumer.start()

    # tworzymy od razu, żeby pierwsze zapytanie nie płaciło za start procesów
    get_process_pool()

    try:
        yield {
            "database_pool": database_pool,
            "streaming_consumer": streaming_consumer,
        }
    finally:
        logger.info("Zamykanie zasobów serwera MCP...")
        await streaming_consumer.stop()

        logger.info("Zamykanie puli procesów roboczych AST...")
        shutdown_process_pool()

        logger.info("Zamykanie puli połączeń z bazą PostgreSQL...")
        if database_pool is not None:
            await database_pool.close()
            database_pool = None
        logger.info("Pula połączeń z bazą danych została pomyślnie zamknięta.")


mcp = FastMCP(
    "Smart-BigData-Gateway",
    lifespan=lifespan,
)


@mcp.tool()
async def ping_database(ctx: Optional[Context] = None) -> Dict[str, Any]:
    """
    Sprawdza stan połączenia z bazą danych PostgreSQL (diagnostyka zdrowia serwera).
    Pobiera aktywne połączenie z puli połączeń, wykonuje zapytanie kontrolne SELECT 1,
    sprawdza aktualny czas systemowy serwera oraz nazwę podłączonej bazy danych.
    """
    start_time = time.perf_counter()

    try:
        pool = get_database_pool(ctx)

        async with pool.acquire() as connection:
            ping_result = await connection.fetchval("SELECT 1;")
            current_db = await connection.fetchval("SELECT current_database();")
            server_time = await connection.fetchval("SELECT NOW();")

        latency_ms = (time.perf_counter() - start_time) * 1000

        return {
            "status": "sukces",
            "connected": ping_result == 1,
            "database": current_db,
            "server_time": str(server_time),
            "latency_ms": round(latency_ms, 2),
            "pool_stats": {
                "size": pool.get_size(),
                "idle_connections": pool.get_idle_size(),
                "min_size": pool.get_min_size(),
                "max_size": pool.get_max_size(),
            },
            "message": "Połączenie z bazą danych PostgreSQL działa prawidłowo.",
        }

    except Exception as exc:
        latency_ms = (time.perf_counter() - start_time) * 1000
        logger.error("Błąd podczas sprawdzania połączenia z bazą danych: %s", exc, exc_info=True)
        return {
            "status": "błąd",
            "connected": False,
            "error": str(exc),
            "latency_ms": round(latency_ms, 2),
            "message": f"Wystąpił błąd podczas komunikacji z bazą danych: {exc}",
        }


@mcp.tool()
async def execute_read_only_query(
    query: str,
    max_limit: int = 1000,
    transport_mode: str = "IN_BAND",
    ctx: Optional[Context] = None,
) -> Dict[str, Any]:
    """
    Bezpiecznie wykonuje analityczne zapytanie SQL tylko do odczytu (SELECT, CTE, funkcje okna, operacje teoriomnogościowe).
    Realizuje asynchroniczną walidację AST w procesach potomnych ProcessPoolExecutor,
    eliminując blokowanie pętli zdarzeń asyncio (Event Loop Starvation).
    Wymusza maksymalny limit zwracanych wierszy (LIMIT) oraz hermetyzuje dane wyjściowe
    w znacznikach izolacyjnych XML (<untrusted_db_record role="data_only" execution_forbidden="true">)
    w celu ochrony agenta przed atakami Second-Order Indirect Prompt Injection.
    Dla dużych wolumenów danych wspiera opcjonalny transfer Out-of-Band (OOB_POSIX_SHM lub OOB_HTTP_STREAM).
    """
    start_time = time.perf_counter()
    logger.info(
        "Odebrano zapytanie SQL do wykonania: query='%s', max_limit=%d, transport=%s",
        query,
        max_limit,
        transport_mode,
    )

    validation = await async_validate_query(query, dialect="postgres", max_limit=max_limit)
    if not validation.is_valid:
        logger.warning(
            "Odrzucono zapytanie SQL ze względów bezpieczeństwa: %s", validation.error_message
        )
        return {
            "status": "błąd",
            "error": validation.error_message,
            "query": query,
            "validation_time_ms": round(validation.execution_time_ms, 2),
            "message": f"Kwerenda SQL została odrzucona przez filtr bezpieczeństwa AST: {validation.error_message}",
        }

    sanitized_sql = validation.sanitized_query or query

    try:
        pool = get_database_pool(ctx)
        async with pool.acquire() as connection:
            rows = await connection.fetch(sanitized_sql)

        serialized_records = [serialize_database_record(r) for r in rows]
        elapsed_total_ms = (time.perf_counter() - start_time) * 1000

        encapsulated_xml = encapsulate_untrusted_database_records(serialized_records)

        # przy OOB dane idą osobnym kanałem, w odpowiedzi zostaje tylko deskryptor
        norm_transport = transport_mode.upper()
        if norm_transport != "IN_BAND" and serialized_records:
            oob_desc = data_dispatcher.dispatch(serialized_records, mode=norm_transport)
            return {
                "status": "sukces",
                "oob_descriptor": oob_desc,
                "records_count": len(serialized_records),
                "transport": oob_desc.get("transport"),
                "sanitized_query": sanitized_sql,
                "validation_time_ms": round(validation.execution_time_ms, 2),
                "execution_time_ms": round(elapsed_total_ms, 2),
                "encapsulated_records": encapsulated_xml,
                "message": "Pomyślnie wykonano zapytanie SQL. Dane przekazano protokołem Out-of-Band.",
            }

        return {
            "status": "sukces",
            "data": serialized_records,
            "records_count": len(serialized_records),
            "transport": "IN_BAND",
            "sanitized_query": sanitized_sql,
            "validation_time_ms": round(validation.execution_time_ms, 2),
            "execution_time_ms": round(elapsed_total_ms, 2),
            "encapsulated_records": encapsulated_xml,
            "message": "Pomyślnie wykonano analityczne zapytanie SQL tylko do odczytu.",
        }

    except Exception as exc:
        elapsed_total_ms = (time.perf_counter() - start_time) * 1000
        logger.error("Błąd podczas wykonywania zapytania SQL: %s", exc, exc_info=True)
        return {
            "status": "błąd",
            "error": str(exc),
            "query": sanitized_sql,
            "validation_time_ms": round(validation.execution_time_ms, 2),
            "execution_time_ms": round(elapsed_total_ms, 2),
            "message": f"Wystąpił błąd podczas wykonywania zapytania w bazie PostgreSQL: {exc}",
        }


@mcp.tool()
async def search_schema(
    query: str,
    top_k: int = 5,
    max_expanded_tables: int = 3,
    ctx: Optional[Context] = None,
) -> str:
    """
    Zaawansowane odkrywanie schematów tabel na podstawie zapytania w języku naturalnym.
    Wykorzystuje fuzję wyszukiwania leksykalnego i semantycznego (Hybrid RRF) oraz analizę topologii
    grafu relacji NetworkX, dołączając brakujące tabele mostkujące N:M dla poprawnych złączeń JOIN.
    Zwraca kompaktowy format DDL (nazwy tabel, klucze główne, kolumny, klucze obce).
    """
    logger.info(
        "Odebrano wywołanie narzędzia search_schema: query='%s', top_k=%d, max_expanded_tables=%d",
        query,
        top_k,
        max_expanded_tables,
    )
    pool = get_database_pool(ctx)
    return await schema_engine.discover_schema_with_graph_pruning(
        query=query, top_k=top_k, max_expanded_tables=max_expanded_tables, pool=pool
    )


@mcp.tool()
async def execute_query_paginated(
    table_name: str,
    cursor: Optional[str] = None,
    token_budget: int = 2000,
    max_records: int = 500,
    sort_col: str = "id",
    direction: str = "ASC",
    order_by: Optional[str] = None,
    use_snapshot: bool = False,
    strict_index_check: bool = False,
    transport_mode: str = "IN_BAND",
    ctx: Optional[Context] = None,
) -> Dict[str, Any]:
    """
    Pobiera dane z tabeli PostgreSQL przy użyciu bezpiecznej paginacji keyset z deterministycznym
    porządkiem wielokolumnowym (order_by lub sort_col), walidacją indeksów B-drzewa (pg_catalog),
    opcjonalną izolacją snapshotową (REPEATABLE READ), bezstanowym tokenem HMAC oraz estymatorem Welforda.
    Dla dużych ładunków wspiera protokół Out-of-Band (OOB_POSIX_SHM lub OOB_HTTP_STREAM).
    """
    logger.info(
        "Wywołanie execute_query_paginated: tabela='%s', order_by='%s', sort_col='%s', dir='%s', kursor=%s, budżet=%d, snapshot=%s, transport=%s",
        table_name,
        order_by,
        sort_col,
        direction,
        cursor,
        token_budget,
        use_snapshot,
        transport_mode,
    )
    pool = get_database_pool(ctx)
    result = await fetch_paginated_data(
        pool=pool,
        table_name=table_name,
        cursor=cursor,
        token_budget=token_budget,
        max_records=max_records,
        sort_col=sort_col,
        direction=direction,
        order_by=order_by,
        use_snapshot=use_snapshot,
        strict_index_check=strict_index_check,
    )

    # AUTO też tu wpada - o tym, czy faktycznie iść przez OOB, decyduje dispatcher po rozmiarze
    norm_transport = transport_mode.upper()
    if norm_transport != "IN_BAND" and result["data"]:
        oob_descriptor = data_dispatcher.dispatch(result["data"], mode=norm_transport)
        return {
            "oob_descriptor": oob_descriptor,
            "next_cursor": result["next_cursor"],
            "has_more": result["has_more"],
            "tokens_used": result["tokens_used"],
            "records_count": len(result["data"]),
            "transport": oob_descriptor.get("transport"),
        }

    return result


@mcp.tool()
async def retrieve_out_of_band_payload(
    descriptor: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Odbiera i weryfikuje dane przekazane przez protokół Out-of-Band (POSIX SHM lub HTTP Stream).
    Weryfikuje sumę kontrolną SHA-256 i zwraca zdekodowany ładunek JSON.
    """
    logger.info("Odebrano żądanie odczytu danych OOB: transport=%s", descriptor.get("transport"))
    try:
        raw_bytes = data_dispatcher.retrieve(descriptor)
        parsed_data = json.loads(raw_bytes.decode("utf-8"))
        return {
            "status": "sukces",
            "size_bytes": len(raw_bytes),
            "sha256": descriptor.get("sha256"),
            "data": parsed_data,
            "message": "Pomyślnie odebrano i zweryfikowano ładunek z protokołu Out-of-Band.",
        }
    except Exception as exc:
        logger.error("Błąd podczas odczytu danych OOB: %s", exc, exc_info=True)
        return {
            "status": "błąd",
            "error": str(exc),
            "descriptor": descriptor,
            "message": f"Wystąpił błąd podczas odczytu danych OOB: {exc}",
        }


@mcp.resource("telemetry://latest_anomaly")
async def get_latest_anomaly() -> str:
    """Zwraca ostatnią zarejestrowaną krytyczną anomalię telemetryczną w formacie JSON."""
    return streaming_consumer.get_latest_anomaly_json()


@mcp.resource("telemetry://live_stream")
async def get_live_stream() -> str:
    """Zwraca podgląd najnowszych anomalii telemetrycznych ze strumienia Kafka w formacie JSON."""
    anomalies = streaming_consumer.get_latest_anomalies(limit=10)
    if not anomalies:
        return json.dumps(
            {
                "status": "NO_ANOMALIES_RECORDED",
                "message": "Brak zarejestrowanych anomalii w bieżącej sesji strumienia Kafka.",
                "total_events_received": streaming_consumer.total_events_received,
            },
            ensure_ascii=False,
            indent=2,
        )
    return json.dumps(anomalies, ensure_ascii=False, indent=2)


@mcp.tool()
async def get_latest_telemetry_anomalies(limit: int = 5) -> str:
    """
    Pobiera listę ostatnich krytycznych anomalii telemetrycznych (CRITICAL_ANOMALY)
    odebranych ze strumienia Apache Kafka w bieżącej sesji serwera MCP w formacie JSON.
    Umożliwia agentowi AI natychmiastowy wgląd w awarie sensorów i urządzeń IoT.
    """
    logger.info("Odebrano wywołanie narzędzia get_latest_telemetry_anomalies: limit=%d", limit)
    anomalies = streaming_consumer.get_latest_anomalies(limit=limit)
    if not anomalies:
        return json.dumps(
            {
                "status": "NO_ANOMALIES_RECORDED",
                "message": "Brak zarejestrowanych anomalii w bieżącej sesji strumienia Kafka.",
                "total_events_received": streaming_consumer.total_events_received,
            },
            ensure_ascii=False,
            indent=2,
        )
    return json.dumps(anomalies, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    logger.info("Uruchamianie serwera FastMCP 'Smart-BigData-Gateway' w trybie transportu stdio...")
    mcp.run(transport="stdio")
