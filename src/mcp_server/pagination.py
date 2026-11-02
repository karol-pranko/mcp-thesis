#!/usr/bin/env python3
"""
Paginacja keyset dla narzędzia execute_query_paginated.

Co tu jest:
- porządek po dowolnej liczbie kolumn, zawsze domknięty kolumną `id` jako tie-breakerem,
- kursor jako base64(JSON) + HMAC - serwer nic nie pamięta między stronami,
- sprawdzanie w pg_catalog, czy jest indeks B-tree pod ten porządek (jak nie ma, to ostrzeżenie
  i mniejsza paczka, albo błąd przy strict_index_check),
- opcjonalny snapshot REPEATABLE READ (pg_export_snapshot), żeby kolejne strony widziały ten sam stan bazy,
- rozmiar paczki z budżetu tokenów, liczony online algorytmem Welforda.
"""

import base64
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal
import hashlib
import hmac
import json
import logging
import math
import os
import re
import sys
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Tuple, Union
import uuid

import asyncpg

try:
    from .security import sql_sanitizer, validate_read_only, wrap_in_context_delimiters
except (ImportError, ValueError):
    try:
        from src.mcp_server.security import (
            sql_sanitizer,
            validate_read_only,
            wrap_in_context_delimiters,
        )
    except (ImportError, ValueError):
        from security import (
            sql_sanitizer,
            validate_read_only,
            wrap_in_context_delimiters,
        )


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("mcp_pagination")


# domyślny sekret jest tylko do testbedu - na serio trzeba ustawić MCP_CURSOR_SECRET
CURSOR_HMAC_SECRET: bytes = os.getenv(
    "MCP_CURSOR_SECRET", "mcp-enterprise-cursor-secret-key-2026-secure-32b"
).encode("utf-8")


# tiktoken przy pierwszym użyciu ściąga plik z enkoderem, więc bez sieci może się wysypać
try:
    import tiktoken

    _TOKEN_ENCODER = tiktoken.get_encoding("cl100k_base")
    logger.info("Załadowano enkoder tiktoken 'cl100k_base' do precyzyjnego liczenia tokenów.")
except Exception as init_exc:
    _TOKEN_ENCODER = None
    logger.warning(
        "Nie udało się załadować tiktoken: %s. Używana będzie estymacja heurystyczna.", init_exc
    )


def estimate_tokens(text_data: str) -> int:
    """cl100k_base, a bez tiktokena zgrubnie: 4 znaki na token."""
    if not text_data:
        return 0

    if _TOKEN_ENCODER is not None:
        try:
            return len(_TOKEN_ENCODER.encode(text_data))
        except Exception:
            pass

    return max(1, len(text_data) // 4)


class UnindexedKeysetWarning(UserWarning):
    pass


class IndexMissingError(RuntimeError):
    """Tylko przy strict_index_check=True. Kod -32000 jak w JSON-RPC (server error)."""

    def __init__(self, message: str, table_name: str, sort_columns: List[str]) -> None:
        super().__init__(message)
        self.code = -32000
        self.table_name = table_name
        self.sort_columns = sort_columns


class SnapshotExpiredError(RuntimeError):
    """Snapshot przekroczył TTL - agent musi zacząć paginację od nowa."""

    def __init__(self, message: str, snapshot_id: str, ttl_seconds: float) -> None:
        super().__init__(message)
        self.code = -32001
        self.snapshot_id = snapshot_id
        self.ttl_seconds = ttl_seconds


# --- budżet tokenów (Welford) ---

class AdaptiveTokenBudgetEstimator:
    """
    Średnia i wariancja liczby tokenów na wiersz, aktualizowane po każdej stronie.
    Dzięki temu znamy rozmiar paczki przed zapytaniem i nie tokenizujemy każdego wiersza osobno.
    """

    def __init__(self, initial_tokens_per_row: float = 85.0) -> None:
        # 85 to tylko punkt startowy, po pierwszej stronie estymator i tak się dostroi
        self.count: int = 0
        self.mean: float = float(initial_tokens_per_row)
        self.M2: float = 0.0

    def update(self, sample_tokens_per_row: float) -> None:
        # M2_n = M2_{n-1} + (x - mean_{n-1}) * (x - mean_n)
        if sample_tokens_per_row <= 0:
            return
        self.count += 1
        delta = sample_tokens_per_row - self.mean
        self.mean += delta / self.count
        delta2 = sample_tokens_per_row - self.mean
        self.M2 += delta * delta2

    def update_from_batch(self, total_tokens: int, num_rows: int) -> None:
        if num_rows <= 0 or total_tokens <= 0:
            return
        avg_tokens = total_tokens / float(num_rows)
        self.update(avg_tokens)

    @property
    def variance(self) -> float:
        return self.M2 / (self.count - 1) if self.count > 1 else 0.0

    @property
    def std_dev(self) -> float:
        return math.sqrt(max(0.0, self.variance))

    def estimate_batch_size(
        self,
        token_budget: int,
        max_records: int = 500,
        safety_margin: float = 0.95,
    ) -> int:
        """Ile wierszy zmieści się w budżecie. Liczymy ostrożnie: mean + 0.5*std, z 5% zapasu."""
        if token_budget <= 0:
            return 1

        conservative_tokens_per_row = max(1.0, self.mean + 0.5 * self.std_dev)
        usable_budget = token_budget * safety_margin
        estimated_rows = int(usable_budget // conservative_tokens_per_row)

        return max(1, min(max_records, estimated_rows))


token_budget_estimator = AdaptiveTokenBudgetEstimator()


# --- specyfikacja porządku ---

def validate_identifier(name: str) -> str:
    # identyfikatory wklejamy do SQL f-stringiem, więc whitelist zamiast escapowania
    identifier_pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
    if not re.match(identifier_pattern, name):
        raise ValueError(f"Nieprawidłowy identyfikator SQL: '{name}'.")
    return name


def sanitize_table_name(table_name: str) -> str:
    """'tabela' albo 'schemat.tabela' -> '"schemat"."tabela"' (domyślnie public)."""
    if not table_name or not table_name.strip():
        raise ValueError("Nazwa tabeli SQL nie może być pusta.")

    raw_parts = [part.strip() for part in table_name.strip().split(".")]

    if len(raw_parts) == 1:
        schema_name = "public"
        table_only = validate_identifier(raw_parts[0])
    elif len(raw_parts) == 2:
        schema_name = validate_identifier(raw_parts[0])
        table_only = validate_identifier(raw_parts[1])
    else:
        raise ValueError(
            f"Nieprawidłowy format nazwy tabeli: '{table_name}'. "
            f"Oczekiwano formatu 'nazwa_tabeli' lub 'schemat.nazwa_tabeli'."
        )

    return f'"{schema_name}"."{table_only}"'


@dataclass
class SortColumnSpec:
    column: str
    direction: str = "ASC"
    nulls_rule: str = "NULLS LAST"

    def __post_init__(self) -> None:
        self.column = validate_identifier(self.column.strip())
        self.direction = self.direction.strip().upper()
        if self.direction not in ("ASC", "DESC"):
            self.direction = "ASC"

        self.nulls_rule = self.nulls_rule.strip().upper()
        if "FIRST" in self.nulls_rule:
            self.nulls_rule = "NULLS FIRST"
        else:
            self.nulls_rule = "NULLS LAST"

    def to_sql_order_clause(self) -> str:
        # id jest NOT NULL, więc NULLS LAST tylko by zaśmiecało zapytanie
        if self.column == "id" and self.nulls_rule == "NULLS LAST":
            return f'"{self.column}" {self.direction}'
        return f'"{self.column}" {self.direction} {self.nulls_rule}'


def normalize_order_by(
    order_by: Optional[Union[str, List[Any]]] = None,
    sort_col: str = "id",
    direction: str = "ASC",
    tie_breaker_col: str = "id",
) -> List[SortColumnSpec]:
    """
    order_by może być stringiem ("created_at DESC NULLS LAST, amount"), listą krotek,
    listą stringów albo listą SortColumnSpec. Na końcu zawsze ląduje tie-breaker (id),
    bez niego keyset gubi/duplikuje wiersze przy równych wartościach.
    """
    specs: List[SortColumnSpec] = []

    if order_by is None:
        valid_col = validate_identifier(sort_col)
        norm_dir = direction.upper() if direction.upper() in ("ASC", "DESC") else "ASC"
        specs.append(SortColumnSpec(column=valid_col, direction=norm_dir, nulls_rule="NULLS LAST"))

    elif isinstance(order_by, str):
        parts = [p.strip() for p in order_by.split(",") if p.strip()]
        for p in parts:
            tokens = p.split()
            col = tokens[0]
            dir_val = "ASC"
            nulls_val = "NULLS LAST"
            if len(tokens) >= 2:
                if tokens[1].upper() in ("ASC", "DESC"):
                    dir_val = tokens[1].upper()
            if "NULLS FIRST" in p.upper():
                nulls_val = "NULLS FIRST"
            elif "NULLS LAST" in p.upper():
                nulls_val = "NULLS LAST"
            specs.append(SortColumnSpec(column=col, direction=dir_val, nulls_rule=nulls_val))

    elif isinstance(order_by, list):
        for item in order_by:
            if isinstance(item, SortColumnSpec):
                specs.append(item)
            elif isinstance(item, (tuple, list)):
                col = item[0]
                dir_val = item[1] if len(item) > 1 else "ASC"
                nulls_val = item[2] if len(item) > 2 else "NULLS LAST"
                specs.append(SortColumnSpec(column=col, direction=dir_val, nulls_rule=nulls_val))
            elif isinstance(item, str):
                tokens = item.split()
                col = tokens[0]
                dir_val = tokens[1].upper() if len(tokens) > 1 and tokens[1].upper() in ("ASC", "DESC") else "ASC"
                nulls_val = "NULLS FIRST" if "NULLS FIRST" in item.upper() else "NULLS LAST"
                specs.append(SortColumnSpec(column=col, direction=dir_val, nulls_rule=nulls_val))

    if not specs:
        specs.append(SortColumnSpec(column="id", direction="ASC", nulls_rule="NULLS LAST"))

    has_tie_breaker = any(s.column == tie_breaker_col for s in specs)
    if not has_tie_breaker:
        # kierunek jak ostatnia kolumna - wtedy przy jednolitym kierunku działa porównanie krotek
        last_dir = specs[-1].direction if specs else "ASC"
        specs.append(SortColumnSpec(column=tie_breaker_col, direction=last_dir, nulls_rule="NULLS LAST"))
    elif specs[-1].column != tie_breaker_col:
        # id w środku listy - przenosimy na koniec
        specs = [s for s in specs if s.column != tie_breaker_col]
        specs.append(SortColumnSpec(column=tie_breaker_col, direction="ASC", nulls_rule="NULLS LAST"))

    return specs


# --- kursor (base64 JSON + HMAC) ---

def _serialize_cursor_value(val: Any) -> Any:
    # Decimal jako string, żeby nie zgubić precyzji przez float
    if val is None:
        return None
    if isinstance(val, (datetime, uuid.UUID)):
        return str(val)
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, bytes):
        return base64.b64encode(val).decode("ascii")
    return val


def encode_opaque_cursor(
    sort_col: Optional[str] = None,
    last_val: Optional[Any] = None,
    last_id: int = 0,
    direction: str = "ASC",
    query_hash: str = "",
    order_specs: Optional[List[SortColumnSpec]] = None,
    last_values: Optional[List[Any]] = None,
    snapshot_id: Optional[str] = None,
) -> str:
    """
    Zwraca "<base64url_payload>.<hmac_hex>". Nowe wywołania podają order_specs + last_values,
    stare (sort_col/last_val/last_id) dalej działają.
    """
    specs_list: List[Dict[str, str]] = []
    vals_list: List[Any] = []

    if order_specs is not None:
        for s in order_specs:
            specs_list.append({"col": s.column, "dir": s.direction, "nulls": s.nulls_rule})
        if last_values is not None:
            vals_list = [_serialize_cursor_value(v) for v in last_values]
        primary_sort = order_specs[0].column
        primary_dir = order_specs[0].direction
        primary_val = vals_list[0] if vals_list else None
    else:
        # stary sposób wywołania
        effective_col = sort_col or "id"
        primary_sort = effective_col
        primary_dir = direction.upper()
        primary_val = _serialize_cursor_value(last_val)
        specs_list = [
            {"col": effective_col, "dir": primary_dir, "nulls": "NULLS LAST"},
            {"col": "id", "dir": primary_dir, "nulls": "NULLS LAST"},
        ]
        vals_list = [primary_val, int(last_id)]

    payload = {
        "order_specs": specs_list,
        "last_values": vals_list,
        "sort_col": primary_sort,
        "last_val": primary_val,
        "last_id": int(last_id),
        "direction": primary_dir,
        "query_hash": query_hash,
        "snapshot_id": snapshot_id,
        "ts": int(time.time()),
    }

    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    b64_payload = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("ascii")
    signature = hmac.new(CURSOR_HMAC_SECRET, b64_payload.encode("ascii"), hashlib.sha256).hexdigest()

    return f"{b64_payload}.{signature}"


def _deserialize_cursor_value(val: Any) -> Any:
    """
    Odwrotność _serialize_cursor_value. Typ zgadujemy po kształcie stringa - asyncpg
    nie przyjmie stringa jako parametru dla kolumny timestamptz/uuid/numeric.
    """
    if val is None:
        return None
    if isinstance(val, str):
        # np. '2026-10-01T11:46:27.318180+00:00'
        if len(val) >= 19 and ("T" in val or " " in val) and ("-" in val or ":" in val):
            try:
                return datetime.fromisoformat(val)
            except (ValueError, TypeError):
                pass
        if len(val) == 36 and val.count("-") == 4:
            try:
                return uuid.UUID(val)
            except (ValueError, TypeError):
                pass
        if re.match(r"^-?\d+\.\d+$", val):
            try:
                return Decimal(val)
            except (ValueError, TypeError):
                pass
    if isinstance(val, float):
        return Decimal(str(val))
    return val


def decode_opaque_cursor(cursor_str: str) -> Dict[str, Any]:
    """
    Sprawdza HMAC i rozpakowuje kursor. Przyjmuje też najstarszy format
    (samo base64 z {"last_id": int}, bez podpisu).
    """
    if not cursor_str or not cursor_str.strip():
        raise ValueError("Token kursora nie może być pusty.")

    cleaned_cursor = cursor_str.strip()
    parts = cleaned_cursor.split(".")

    if len(parts) == 2:
        b64_payload, signature = parts
        expected_sig = hmac.new(
            CURSOR_HMAC_SECRET, b64_payload.encode("ascii"), hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(signature, expected_sig):
            logger.error("Wykryto próbę modyfikacji kursora (błąd sygnatury HMAC)!")
            raise ValueError("Nieprawidłowy lub zmodyfikowany token kursora (błąd weryfikacji HMAC).")

        try:
            payload_bytes = base64.urlsafe_b64decode(b64_payload.encode("ascii"))
            data = json.loads(payload_bytes.decode("utf-8"), parse_float=Decimal)
            if not isinstance(data, dict):
                raise ValueError("Ładunek kursora nie jest obiektem JSON.")

            # starsze kursory nie miały wszystkich pól
            if "last_id" not in data:
                data["last_id"] = 0
            if "sort_col" not in data:
                data["sort_col"] = "id"
            if "direction" not in data:
                data["direction"] = "ASC"

            if "last_values" in data and isinstance(data["last_values"], list):
                data["last_values"] = [_deserialize_cursor_value(v) for v in data["last_values"]]
            if "last_val" in data:
                data["last_val"] = _deserialize_cursor_value(data["last_val"])

            return data
        except Exception as exc:
            raise ValueError(f"Uszkodzony ładunek tokenu kursora: {exc}") from exc

    elif len(parts) == 1:
        try:
            decoded_bytes = base64.urlsafe_b64decode(parts[0].encode("ascii"))
            parsed_data = json.loads(decoded_bytes.decode("utf-8"))
            if not isinstance(parsed_data, dict) or "last_id" not in parsed_data:
                raise ValueError("Kursor nie zawiera wymaganego pola 'last_id'.")
            lid = int(parsed_data["last_id"])
            return {
                "order_specs": [{"col": "id", "dir": "ASC", "nulls": "NULLS LAST"}],
                "last_values": [lid],
                "sort_col": "id",
                "last_val": lid,
                "last_id": lid,
                "direction": "ASC",
                "query_hash": "",
                "snapshot_id": None,
                "ts": int(time.time()),
            }
        except Exception as exc:
            raise ValueError(f"Niepoprawny format starszego formatu kursora: {exc}") from exc

    else:
        raise ValueError("Nieprawidłowa struktura tokenu kursora (oczekiwano formatu payload.hmac).")


def encode_cursor(last_id: int) -> str:
    return encode_opaque_cursor(sort_col="id", last_val=last_id, last_id=last_id, direction="ASC")


def decode_cursor(cursor_str: str) -> int:
    data = decode_opaque_cursor(cursor_str)
    return int(data["last_id"])


# --- sprawdzanie indeksów w pg_catalog ---

@dataclass
class IndexCheckResult:
    is_compatible: bool
    matching_index_name: Optional[str] = None
    matching_index_columns: List[str] = field(default_factory=list)
    warning: Optional[str] = None
    checked_columns: List[str] = field(default_factory=list)


class IndexCatalogGuard:
    """Czy jest indeks B-tree, którego początek pokrywa kolumny sortowania (w tej kolejności)."""

    def __init__(self) -> None:
        self._cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self._cache_ttl = 60.0  # s; indeksy rzadko się zmieniają, a to zapytanie leci przy każdej stronie

    async def get_table_btree_indexes(
        self,
        conn: Union[asyncpg.Connection, asyncpg.Pool],
        schema_name: str,
        table_name: str,
    ) -> List[Dict[str, Any]]:
        cache_key = f"{schema_name}.{table_name}"
        now = time.time()
        if cache_key in self._cache:
            ts, cached_rows = self._cache[cache_key]
            if now - ts < self._cache_ttl:
                return cached_rows

        query = """
        SELECT
            i.relname AS index_name,
            am.amname AS index_type,
            ix.indisvalid AS is_valid,
            ARRAY(
                SELECT a.attname
                FROM generate_series(0, ix.indnatts - 1) AS s(idx)
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ix.indkey[s.idx]
                ORDER BY s.idx
            ) AS index_columns
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        WHERE n.nspname = $1 AND t.relname = $2
          AND am.amname = 'btree'
          AND ix.indisvalid = true;
        """
        rows = await conn.fetch(query, schema_name, table_name)
        indexes_list = [dict(r) for r in rows]
        self._cache[cache_key] = (now, indexes_list)
        return indexes_list

    async def verify_index_compatibility(
        self,
        conn: Union[asyncpg.Connection, asyncpg.Pool],
        table_identifier: str,
        order_specs: List[SortColumnSpec],
    ) -> IndexCheckResult:
        parts = table_identifier.replace('"', '').split(".")
        if len(parts) == 1:
            s_name, t_name = "public", parts[0]
        else:
            s_name, t_name = parts[0], parts[1]

        needed_cols = [s.column for s in order_specs]
        indexes = await self.get_table_btree_indexes(conn, s_name, t_name)

        for idx in indexes:
            idx_cols = idx.get("index_columns") or []
            if len(idx_cols) >= len(needed_cols):
                if idx_cols[:len(needed_cols)] == needed_cols:
                    return IndexCheckResult(
                        is_compatible=True,
                        matching_index_name=idx["index_name"],
                        matching_index_columns=idx_cols,
                        warning=None,
                        checked_columns=needed_cols,
                    )

        # nie ma pełnego - szukamy chociaż indeksu na pierwszej kolumnie (tylko do raportu)
        partial_match = None
        for idx in indexes:
            idx_cols = idx.get("index_columns") or []
            if idx_cols and idx_cols[0] == needed_cols[0]:
                partial_match = idx
                break

        warn_msg = (
            f"UnindexedKeysetWarning: Tabela '{s_name}.{t_name}' nie posiada pełnego kompozytowego "
            f"indeksu B-drzewa dla kolumn {needed_cols}. Może wystąpić pełny skan (Seq Scan) lub sortowanie dyskowe."
        )
        logger.warning(warn_msg)

        return IndexCheckResult(
            is_compatible=False,
            matching_index_name=partial_match["index_name"] if partial_match else None,
            matching_index_columns=partial_match.get("index_columns", []) if partial_match else [],
            warning=warn_msg,
            checked_columns=needed_cols,
        )


index_catalog_guard = IndexCatalogGuard()


# --- snapshoty REPEATABLE READ ---

@dataclass
class ActiveSnapshot:
    snapshot_id: str
    connection: asyncpg.Connection
    transaction: Any
    created_at: float
    ttl_seconds: float
    table_name: str
    pool: Optional[asyncpg.Pool] = None


class TransactionSnapshotManager:
    """
    Jedno połączenie trzyma otwartą transakcję z wyeksportowanym snapshotem, a kolejne strony
    importują go przez SET TRANSACTION SNAPSHOT. Każdy snapshot blokuje jedno połączenie z puli
    aż do TTL lub końca paginacji - przy małej puli to się szybko kończy (bench_snapshot_pool_starvation).
    """

    def __init__(self) -> None:
        self._snapshots: Dict[str, ActiveSnapshot] = {}

    async def create_snapshot(
        self,
        pool: asyncpg.Pool,
        ttl_seconds: float = 300.0,
        table_name: str = "",
        acquire_timeout: Optional[float] = None,
    ) -> str:
        await self.cleanup_expired()

        # połączenie zostaje u nas, więc bez `async with`; obsługujemy i pulę asyncpg, i atrapy z testów
        acq = pool.acquire(timeout=acquire_timeout)
        if hasattr(acq, "__await__"):
            conn = await acq
        else:
            conn = await acq.__aenter__()

        tx = conn.transaction(isolation="repeatable_read")
        await tx.start()

        snapshot_id = await conn.fetchval("SELECT pg_export_snapshot();")
        now = time.time()

        active = ActiveSnapshot(
            snapshot_id=snapshot_id,
            connection=conn,
            transaction=tx,
            created_at=now,
            ttl_seconds=ttl_seconds,
            table_name=table_name,
            pool=pool,
        )
        self._snapshots[snapshot_id] = active

        logger.info(
            "Utworzono nowy snapshot transakcyjny PostgreSQL: %s dla tabeli '%s' (TTL=%.1f s).",
            snapshot_id,
            table_name,
            ttl_seconds,
        )
        return snapshot_id

    async def execute_in_snapshot(
        self,
        pool: asyncpg.Pool,
        snapshot_id: str,
        query_sql: str,
        params: List[Any],
    ) -> List[asyncpg.Record]:
        now = time.time()
        active = self._snapshots.get(snapshot_id)

        if not active or (now - active.created_at > active.ttl_seconds):
            if active:
                await self.release_snapshot(snapshot_id)
            raise SnapshotExpiredError(
                f"Identyfikator snapshotu '{snapshot_id}' wygasł lub nie istnieje. Wymagany restart paginacji od strony 0.",
                snapshot_id=snapshot_id,
                ttl_seconds=active.ttl_seconds if active else 0.0,
            )

        async with pool.acquire() as worker_conn:
            worker_tx = worker_conn.transaction(isolation="repeatable_read")
            await worker_tx.start()
            try:
                await worker_conn.execute(f"SET TRANSACTION SNAPSHOT '{snapshot_id}';")
                rows = await worker_conn.fetch(query_sql, *params)
                await worker_tx.commit()
                return rows
            except Exception:
                await worker_tx.rollback()
                raise

    async def release_snapshot(self, snapshot_id: str) -> None:
        active = self._snapshots.pop(snapshot_id, None)
        if active:
            try:
                await active.transaction.rollback()
            except Exception as exc:
                logger.debug("Błąd wycofania transakcji snapshotu %s: %s", snapshot_id, exc)
            try:
                if active.pool is not None:
                    await active.pool.release(active.connection)
                else:
                    await active.connection.close()
            except Exception as exc:
                logger.debug("Błąd zwalniania połączenia snapshotu %s: %s", snapshot_id, exc)
            logger.info("Zwolniono zasoby snapshotu transakcyjnego: %s", snapshot_id)

    async def cleanup_expired(self) -> None:
        # długo otwarta transakcja wstrzymuje VACUUM, więc nie trzymamy ich ponad TTL
        now = time.time()
        expired_ids = [
            sid for sid, snap in self._snapshots.items()
            if now - snap.created_at > snap.ttl_seconds
        ]
        for sid in expired_ids:
            await self.release_snapshot(sid)

    async def close_all(self) -> None:
        for sid in list(self._snapshots.keys()):
            await self.release_snapshot(sid)


snapshot_manager = TransactionSnapshotManager()


# --- budowa zapytania keyset ---

def _columns_expose_sort_keys(columns: str, order_specs: List[Any]) -> bool:
    # zewnętrzny ORDER BY w wariancie UNION ALL musi widzieć kolumny sortowania pod tymi samymi nazwami
    if columns.strip() == "*":
        return True
    exposed = {c.strip().strip('"') for c in columns.split(",")}
    return all(spec.column in exposed for spec in order_specs)


def build_keyset_query(
    safe_table: str,
    columns: str = "*",
    sort_col: str = "id",
    direction: str = "ASC",
    cursor_data: Optional[Dict[str, Any]] = None,
    fetch_limit: int = 50,
    order_by: Optional[Union[str, List[Any]]] = None,
    prefer_tuple_comparison: bool = False,
    use_union_all: bool = False,
) -> Tuple[str, List[Any]]:
    """
    Zwraca (sql, params). Predykat "za kursorem" budujemy na jeden z trzech sposobów:

    1. (a, b, id) > ($1, $2, $3) - tylko gdy prefer_tuple_comparison, jeden kierunek i bez NULL-i.
    2. OR-y w postaci DNF - działa zawsze (mieszane kierunki, NULLS FIRST/LAST), ale Postgres
       robi z tego Filter i skanuje indeks od początku, czyli O(M) dla M-tej strony.
    3. UNION ALL rozłącznych gałęzi, każda z własnym ORDER BY ... LIMIT - każda gałąź robi
       seek po B-tree, więc O(log N + K). Tylko dla >1 kolumny i gdy kolumny sortowania są w `columns`,
       inaczej spadamy do 2.
    """
    order_specs = normalize_order_by(order_by=order_by, sort_col=sort_col, direction=direction)

    order_parts = [s.to_sql_order_clause() for s in order_specs]
    order_clause = f"ORDER BY {', '.join(order_parts)}"

    # pierwsza strona
    if cursor_data is None:
        query = f"SELECT {columns} FROM {safe_table} {order_clause} LIMIT $1;"
        return query, [fetch_limit]

    last_values: List[Any] = []
    if "last_values" in cursor_data and cursor_data["last_values"]:
        last_values = list(cursor_data["last_values"])
    elif "last_val" in cursor_data:
        # kursor w starym formacie (sort_col, last_val, last_id)
        last_id = int(cursor_data.get("last_id", 0))
        if len(order_specs) == 1:
            last_values = [last_id]
        elif len(order_specs) == 2 and order_specs[0].column != "id":
            last_values = [cursor_data.get("last_val"), last_id]
        else:
            last_values = [cursor_data.get("last_val")] + [None] * (len(order_specs) - 2) + [last_id]

    # wyrównanie do liczby kolumn (kursor mógł powstać dla innego order_by)
    while len(last_values) < len(order_specs):
        last_values.append(None)
    last_values = last_values[:len(order_specs)]

    # porównanie krotek ma sens tylko dla jednego kierunku i bez NULL-i w kursorze
    all_same_dir = len(set(s.direction for s in order_specs)) == 1
    has_null_val = any(v is None for v in last_values)
    can_use_tuple = prefer_tuple_comparison and all_same_dir and not has_null_val

    if can_use_tuple:
        col_names = [f'"{s.column}"' for s in order_specs]
        row_cols = f"({', '.join(col_names)})"
        comp_op = ">" if order_specs[0].direction == "ASC" else "<"
        param_placeholders = [f"${i+1}" for i in range(len(last_values))]
        row_params = f"({', '.join(param_placeholders)})"
        limit_placeholder = f"${len(last_values) + 1}"

        where_clause = f"WHERE {row_cols} {comp_op} {row_params}"
        query = f"SELECT {columns} FROM {safe_table} {where_clause} {order_clause} LIMIT {limit_placeholder};"
        return query, last_values + [fetch_limit]

    # wariant ogólny: dla kolumny i -> (wszystkie wcześniejsze równe) AND (kolumna i "dalej")
    params: List[Any] = []
    col_param_map: Dict[str, str] = {}
    p_idx = 1
    for spec, val in zip(order_specs, last_values):
        col = spec.column
        if val is not None:
            col_param_map[col] = f"${p_idx}"
            params.append(val)
            p_idx += 1

    branches: List[str] = []
    split_branches: List[str] = []
    for i, spec in enumerate(order_specs):
        col = spec.column
        dir_val = spec.direction
        nulls = spec.nulls_rule
        val = last_values[i]

        eqs: List[str] = []
        for j in range(i):
            prev_col = order_specs[j].column
            prev_val = last_values[j]
            if prev_val is None:
                eqs.append(f'"{prev_col}" IS NULL')
            else:
                eqs.append(f'"{prev_col}" = {col_param_map[prev_col]}')

        # "dalej" zależy od kierunku i od tego, gdzie stoją NULL-e; alternatywy są rozłączne,
        # co jest potrzebne dla wariantu UNION ALL (bez duplikatów)
        afters: List[str] = []
        is_id = (col == "id")

        if "LAST" in nulls:
            if dir_val == "ASC":
                if val is not None:
                    afters.append(f'"{col}" > {col_param_map[col]}')
                    if not is_id:
                        afters.append(f'"{col}" IS NULL')
            else:  # DESC
                if val is not None:
                    afters.append(f'"{col}" < {col_param_map[col]}')
                    if not is_id:
                        afters.append(f'"{col}" IS NULL')
        else:  # NULLS FIRST
            if dir_val == "ASC":
                if val is None:
                    afters.append(f'"{col}" IS NOT NULL')
                else:
                    afters.append(f'"{col}" > {col_param_map[col]}')
            else:  # DESC
                if val is None:
                    afters.append(f'"{col}" IS NOT NULL')
                else:
                    afters.append(f'"{col}" < {col_param_map[col]}')

        if afters:
            after = afters[0] if len(afters) == 1 else f"({' OR '.join(afters)})"
            branches.append(" AND ".join(eqs + [after]))
            split_branches.extend(" AND ".join(eqs + [alt]) for alt in afters)

    if not branches:
        # wszystkie wartości w kursorze NULL - zostaje samo id
        last_id_val = int(cursor_data.get("last_id", 0))
        query = (
            f"SELECT {columns} FROM {safe_table} "
            f'WHERE "id" > ${p_idx} {order_clause} LIMIT ${p_idx + 1};'
        )
        return query, params + [last_id_val, fetch_limit]

    if use_union_all and len(order_specs) > 1 and len(split_branches) > 1 and _columns_expose_sort_keys(columns, order_specs):
        params.append(fetch_limit)
        limit_placeholder = f"${p_idx}"
        selects = [
            f"(SELECT {columns} FROM {safe_table} WHERE {cond} {order_clause} LIMIT {limit_placeholder})"
            for cond in split_branches
        ]
        query = (
            f"SELECT * FROM ({' UNION ALL '.join(selects)}) AS keyset_page "
            f"{order_clause} LIMIT {limit_placeholder};"
        )
        return query, params

    if len(branches) == 1:
        where_expr = f"({branches[0]})" if not (branches[0].startswith("(") and branches[0].endswith(")")) else branches[0]
    else:
        inner_branches = " OR ".join(f"({b})" if not (b.startswith("(") and b.endswith(")")) else b for b in branches)
        where_expr = f"({inner_branches})"

    limit_placeholder = f"${p_idx}"
    params.append(fetch_limit)

    query = f"SELECT {columns} FROM {safe_table} WHERE {where_expr} {order_clause} LIMIT {limit_placeholder};"
    return query, params


# --- główna funkcja ---

def serialize_database_record(record: asyncpg.Record) -> Dict[str, Any]:
    # typy, których json.dumps sam nie ogarnie
    serialized: Dict[str, Any] = {}
    for column_name, value in record.items():
        if isinstance(value, uuid.UUID):
            serialized[column_name] = str(value)
        elif isinstance(value, datetime):
            serialized[column_name] = value.isoformat()
        elif isinstance(value, Decimal):
            serialized[column_name] = float(value)
        elif isinstance(value, bytes):
            serialized[column_name] = base64.b64encode(value).decode("ascii")
        else:
            serialized[column_name] = value
    return serialized


async def fetch_paginated_data(
    pool: asyncpg.Pool,
    table_name: str,
    cursor: Optional[str] = None,
    token_budget: int = 2000,
    max_records: int = 500,
    columns: str = "*",
    sort_col: str = "id",
    direction: str = "ASC",
    order_by: Optional[Union[str, List[Any]]] = None,
    use_snapshot: bool = False,
    snapshot_ttl: float = 300.0,
    strict_index_check: bool = False,
    estimator: Optional[AdaptiveTokenBudgetEstimator] = None,
    prefer_tuple_comparison: bool = False,
    use_union_all: bool = True,
) -> Dict[str, Any]:
    """
    Jedna strona danych. Kolejność: kursor -> sprawdzenie indeksu -> rozmiar paczki z budżetu
    -> zapytanie (w snapshocie albo zwykłe) -> przycięcie do budżetu -> nowy kursor.
    Pobieramy batch_size + 1 wierszy, żeby wiedzieć, czy jest następna strona.
    """
    start_time = time.perf_counter()
    safe_table = sanitize_table_name(table_name)
    current_estimator = estimator or token_budget_estimator

    cursor_data: Optional[Dict[str, Any]] = None
    active_snapshot_id: Optional[str] = None

    if cursor and cursor.strip():
        cursor_data = decode_opaque_cursor(cursor.strip())
        logger.info(
            "Zdekodowano bezpieczny kursor HMAC dla '%s' (sort_col=%s, last_id=%s)",
            safe_table,
            cursor_data.get("sort_col"),
            cursor_data.get("last_id"),
        )
        # agent zwykle podaje tylko kursor - porządek bierzemy wtedy z niego
        if order_by is None and "order_specs" in cursor_data and cursor_data["order_specs"]:
            order_by = [
                (s["col"], s["dir"], s["nulls"]) for s in cursor_data["order_specs"]
            ]
        elif order_by is None and "sort_col" in cursor_data and cursor_data["sort_col"]:
            sort_col = cursor_data["sort_col"]
            direction = cursor_data.get("direction", "ASC")

        active_snapshot_id = cursor_data.get("snapshot_id")
    else:
        logger.info("Brak kursora wejściowego. Pobieranie pierwszej strony dla tabeli %s.", safe_table)

    normalized_specs = normalize_order_by(order_by=order_by, sort_col=sort_col, direction=direction)

    async with pool.acquire() as check_conn:
        index_check = await index_catalog_guard.verify_index_compatibility(
            conn=check_conn,
            table_identifier=safe_table,
            order_specs=normalized_specs,
        )

    if not index_check.is_compatible:
        if strict_index_check:
            raise IndexMissingError(
                f"Rygorystyczne odrzucenie zapytania: brak kompozytowego indeksu B-drzewa dla tabeli {safe_table} "
                f"i kolumn {[s.column for s in normalized_specs]}.",
                table_name=safe_table,
                sort_columns=[s.column for s in normalized_specs],
            )

    estimated_batch_size = current_estimator.estimate_batch_size(
        token_budget=token_budget, max_records=max_records
    )

    # bez indeksu każda strona to sort całej tabeli, więc przynajmniej nie bierzemy dużych paczek
    if not index_check.is_compatible:
        safe_unindexed_limit = 50
        if estimated_batch_size > safe_unindexed_limit:
            logger.info(
                "Redukcja batch_size z %d do %d z powodu braku kompatybilnego indeksu B-drzewa.",
                estimated_batch_size,
                safe_unindexed_limit,
            )
            estimated_batch_size = safe_unindexed_limit

    fetch_limit = estimated_batch_size + 1

    query_sql, query_params = build_keyset_query(
        safe_table=safe_table,
        columns=columns,
        sort_col=sort_col,
        direction=direction,
        cursor_data=cursor_data,
        fetch_limit=fetch_limit,
        order_by=normalized_specs,
        prefer_tuple_comparison=prefer_tuple_comparison,
        use_union_all=use_union_all,
    )

    # zapytanie składamy sami, ale i tak przepuszczamy przez ten sam filtr co resztę
    sql_sanitizer.assert_valid_read_only_query(query_sql)

    if cursor_data is None and use_snapshot:
        # snapshot zakładamy tylko na pierwszej stronie, dalej jedzie w kursorze
        active_snapshot_id = await snapshot_manager.create_snapshot(
            pool=pool, ttl_seconds=snapshot_ttl, table_name=safe_table
        )

    if active_snapshot_id:
        rows = await snapshot_manager.execute_in_snapshot(
            pool=pool,
            snapshot_id=active_snapshot_id,
            query_sql=query_sql,
            params=query_params,
        )
    else:
        async with pool.acquire() as connection:
            rows = await connection.fetch(query_sql, *query_params)

    logger.info("Pobrano %d wierszy z bazy dla zapytania Keyset.", len(rows))

    has_more = len(rows) > estimated_batch_size
    retrieved_rows = rows[:estimated_batch_size]

    records_data: List[Dict[str, Any]] = [
        serialize_database_record(r) for r in retrieved_rows
    ]

    # ostatnia strona - oddajemy połączenie od razu, nie czekamy na TTL
    if not has_more and active_snapshot_id:
        await snapshot_manager.release_snapshot(active_snapshot_id)
        active_snapshot_id = None

    # tokenizujemy całą paczkę raz, nie wiersz po wierszu
    if records_data:
        aggregated_json = json.dumps(records_data, ensure_ascii=False)
        tokens_accumulated = estimate_tokens(aggregated_json)

        # estymator się pomylił (np. kilka bardzo długich wierszy) - tniemy proporcjonalnie
        if tokens_accumulated > token_budget and len(records_data) > 1:
            ratio = token_budget / float(tokens_accumulated)
            adjusted_count = max(1, int(len(records_data) * ratio * 0.95))
            records_data = records_data[:adjusted_count]
            aggregated_json = json.dumps(records_data, ensure_ascii=False)
            tokens_accumulated = estimate_tokens(aggregated_json)
            has_more = True

        current_estimator.update_from_batch(tokens_accumulated, len(records_data))
    else:
        tokens_accumulated = 0

    next_cursor: Optional[str] = None
    if has_more and records_data:
        last_rec = records_data[-1]
        last_id_val = int(last_rec.get("id", 0))
        last_values = [last_rec.get(s.column) for s in normalized_specs]

        query_hash = hashlib.sha256(
            f"{safe_table}:{[s.column for s in normalized_specs]}:{[s.direction for s in normalized_specs]}".encode("utf-8")
        ).hexdigest()[:8]

        next_cursor = encode_opaque_cursor(
            order_specs=normalized_specs,
            last_values=last_values,
            last_id=last_id_val,
            query_hash=query_hash,
            snapshot_id=active_snapshot_id,
        )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    logger.info(
        "Paginacja [%s]: zwrócono %d rekordów, zużyto %d tokenów (budżet: %d), has_more=%s w czasie %.2f ms",
        safe_table,
        len(records_data),
        tokens_accumulated,
        token_budget,
        has_more,
        elapsed_ms,
    )

    context_delimited = wrap_in_context_delimiters(records_data)

    return {
        "data": records_data,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "tokens_used": tokens_accumulated,
        "context_delimited_data": context_delimited,
        "index_guard": {
            "is_indexed": index_check.is_compatible,
            "index_name": index_check.matching_index_name,
            "warning": index_check.warning,
            "checked_columns": index_check.checked_columns,
        },
        "snapshot_id": active_snapshot_id,
    }
