"""
Jednolita synteza DDL dla wszystkich strategii selekcji schematu (Domain-Scoped, BM25, Dense, RRF, Graph).

Każda strategia dostaje DDL w identycznym formacie z tej samej funkcji `synthesize_ddl`:
opis i domena tabeli, wszystkie kolumny z prawdziwymi typami i ograniczeniem NOT NULL, klucz główny
oraz klucze obce prowadzące do innych tabel z tego samego zbioru. Różni się wyłącznie zbiór tabel.
Tabele są wypisywane w kolejności alfabetycznej, aby kolejność rankingu nie ujawniała strategii.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Tuple

import asyncpg


@dataclass
class SchemaCatalog:
    columns: Dict[str, List[Tuple[str, str, bool]]] = field(default_factory=dict)  # tabela -> [(kolumna, typ, nullable)]
    primary_keys: Dict[str, List[str]] = field(default_factory=dict)
    foreign_keys: List[Tuple[str, str, str, str, str]] = field(default_factory=list)  # (src, src_col, tgt, tgt_col, nazwa)
    descriptions: Dict[str, str] = field(default_factory=dict)
    domains: Dict[str, str] = field(default_factory=dict)

    @classmethod
    async def load(cls, pool: asyncpg.Pool) -> "SchemaCatalog":
        cat = cls()
        async with pool.acquire() as conn:
            for r in await conn.fetch(
                "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns "
                "WHERE table_schema = 'public' ORDER BY table_name, ordinal_position"
            ):
                cat.columns.setdefault(r["table_name"], []).append((r["column_name"], r["data_type"].upper(), r["is_nullable"] == "YES"))
            for r in await conn.fetch(
                "SELECT tc.table_name, kcu.column_name FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name "
                "AND tc.table_schema = kcu.table_schema WHERE tc.table_schema = 'public' AND tc.constraint_type = 'PRIMARY KEY' "
                "ORDER BY tc.table_name, kcu.ordinal_position"
            ):
                cat.primary_keys.setdefault(r["table_name"], []).append(r["column_name"])
            for r in await conn.fetch(
                "SELECT source_table, target_table, constraint_name, join_columns FROM public.schema_foreign_keys "
                "ORDER BY source_table, target_table, constraint_name"
            ):
                meta = r["join_columns"]
                if isinstance(meta, str):
                    meta = json.loads(meta)
                cat.foreign_keys.append((r["source_table"], meta.get("source_column", "id"), r["target_table"],
                                         meta.get("target_column", "id"), r["constraint_name"]))
            for r in await conn.fetch("SELECT table_name, domain, description FROM public.schema_embeddings"):
                cat.descriptions[r["table_name"]] = r["description"]
                cat.domains[r["table_name"]] = r["domain"]
        return cat

    def table_columns_flat(self) -> Dict[str, List[str]]:
        return {t: [c for c, _, _ in cols] for t, cols in self.columns.items()}


def synthesize_ddl(catalog: SchemaCatalog, tables: Iterable[str]) -> str:
    """Jedyna funkcja syntezy DDL używana dla wszystkich strategii."""
    selected = sorted(set(tables))
    sel = set(selected)
    fks_by_src: Dict[str, List[Tuple[str, str, str, str, str]]] = {}
    for fk in catalog.foreign_keys:
        if fk[0] in sel and fk[2] in sel:
            fks_by_src.setdefault(fk[0], []).append(fk)
    blocks: List[str] = []
    for t in selected:
        lines = [f"-- Tabela public.{t} (domena: {catalog.domains.get(t, 'n/d')})",
                 f"-- Opis: {catalog.descriptions.get(t, 'brak opisu')}",
                 f"CREATE TABLE public.{t} ("]
        defs = [f"    {c} {typ}{'' if nullable else ' NOT NULL'}" for c, typ, nullable in catalog.columns.get(t, [])]
        pk = catalog.primary_keys.get(t)
        if pk:
            defs.append(f"    PRIMARY KEY ({', '.join(pk)})")
        for src, sc, tgt, tc, name in fks_by_src.get(t, []):
            defs.append(f"    CONSTRAINT {name} FOREIGN KEY ({sc}) REFERENCES public.{tgt}({tc})")
        lines.append(",\n".join(defs))
        lines.append(");")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


PROMPT_TEMPLATE = (
    "You are a PostgreSQL expert. Return ONLY valid SQL wrapped in ```sql ... ```.\n"
    "CRITICAL REQUIREMENT: Do NOT use SELECT *. Specify explicit column lists matching the requested attributes.\n"
    "Database Schema:\n{ddl}\n\n"
    "Task: Write a SQL query: {question}"
)


def build_prompt(ddl: str, question: str) -> str:
    return PROMPT_TEMPLATE.format(ddl=ddl, question=question)
