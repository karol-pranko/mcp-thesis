"""Połączenia z PostgreSQL oraz DISCARD ALL przed każdą serią pomiarową."""
from __future__ import annotations

import logging
from typing import Any, Dict

import asyncpg

logger = logging.getLogger("bench.db")


async def create_pool(db: Dict[str, Any], min_size: int = 2, max_size: int = 10) -> asyncpg.Pool:
    return await asyncpg.create_pool(min_size=min_size, max_size=max_size, **db)


async def discard_all(pool: asyncpg.Pool, series: str) -> None:
    """
    Wykonuje DISCARD ALL na każdym bezczynnym połączeniu puli przed serią pomiarową
    (czyści przygotowane plany, ustawienia sesji i tabele tymczasowe) i loguje to zdarzenie.
    """
    n = 0
    conns = []
    try:
        for _ in range(pool.get_size() or 1):
            conns.append(await pool.acquire())
        for c in conns:
            await c.execute("DISCARD ALL")
            # DISCARD ALL usuwa po stronie serwera przygotowane instrukcje, które asyncpg trzyma w swojej
            # pamięci podręcznej; unieważnienie cache wymusza ich ponowne przygotowanie przy kolejnym użyciu.
            await c.reload_schema_state()
            n += 1
    finally:
        for c in conns:
            await pool.release(c)
    logger.info("DISCARD ALL przed serią '%s' wykonane na %d połączeniach.", series, n)
