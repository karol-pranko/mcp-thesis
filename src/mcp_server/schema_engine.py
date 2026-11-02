#!/usr/bin/env python3
"""
Wyszukiwanie schematu dla search_schema (baza testowa ma ~1000 tabel).

Strategie, od najprostszej:
- retrieve_bm25        - leksykalnie po nazwie tabeli, kolumnach i opisie
- retrieve_dense       - pgvector, odległość kosinusowa (<=>)
- retrieve_hybrid_rrf  - połączenie obu przez Reciprocal Rank Fusion
- discover_schema_with_graph_pruning - RRF + dorzucenie tabel łącznikowych z grafu FK,
  bo bez nich model nie potrafi poprawnie złożyć JOIN-a przez relację N:M
"""

import asyncio
from collections import Counter, defaultdict
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import asyncpg
import networkx as nx
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer
import torch


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("mcp_schema_engine")


class SchemaEngine:
    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        pool: Optional[asyncpg.Pool] = None,
    ) -> None:
        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(
            "Ładowanie modelu wektoryzacji semantycznej '%s' na urządzeniu: %s...",
            model_name,
            device,
        )
        self.device = device
        self.model = SentenceTransformer(model_name, device=device)
        self.pool = pool

        # BM25 i graf budujemy leniwie przy pierwszym zapytaniu, potem trzymamy w pamięci
        self._bm25_index: Optional[BM25Okapi] = None
        self._corpus_metadata: List[Dict[str, Any]] = []
        self._table_name_to_corpus_idx: Dict[str, int] = {}
        self._columns_by_table_cache: Dict[str, List[Dict[str, Any]]] = {}

        self._relation_graph: Optional[nx.DiGraph] = None
        self._last_index_sync_time: float = 0.0
        self._drift_lock: Optional[asyncio.Lock] = None

        # czasy etapów ostatniego discover_schema_with_graph_pruning [ns], czyta je benchmark
        self.last_graph_timings: Dict[str, int] = {}

        logger.info("Silnik SchemaEngine został pomyślnie zainicjalizowany.")

    def _get_drift_lock(self) -> asyncio.Lock:
        # Lock tworzony leniwie - instancja powstaje przy imporcie, jeszcze bez pętli zdarzeń
        if self._drift_lock is None:
            self._drift_lock = asyncio.Lock()
        return self._drift_lock

    def set_pool(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    def _get_active_pool(self, pool_override: Optional[asyncpg.Pool] = None) -> asyncpg.Pool:
        active_pool = pool_override or self.pool
        if active_pool is not None:
            return active_pool

        # jak nikt nie podał puli, próbujemy wziąć ją z działającego serwera
        try:
            from .server import get_database_pool
            return get_database_pool()
        except Exception:
            pass

        try:
            from src.mcp_server.server import get_database_pool
            return get_database_pool()
        except Exception:
            pass

        raise RuntimeError(
            "Brak zainicjalizowanej puli połączeń z bazą danych. "
            "Przekaż obiekt pool do metody lub skonfiguruj serwer MCP."
        )

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        # "customer_orders" -> ["customer", "orders"]; jednoliterowe tokeny odrzucamy
        if not text:
            return []
        normalized = text.replace("_", " ").lower()
        return [word for word in re.findall(r"\w+", normalized) if len(word) > 1]

    async def _ensure_bm25_index(self, pool: asyncpg.Pool, force_rebuild: bool = False) -> None:
        """Jeden "dokument" na tabelę: nazwa + domena + opis + nazwy kolumn."""
        if self._bm25_index is not None and not force_rebuild:
            return

        logger.info("Inicjalizacja indeksu leksykalnego BM25 na podstawie bazy PostgreSQL...")
        start_time = time.perf_counter()

        query_sql = """
        SELECT
            t.table_name,
            COALESCE(se.domain, '') as domain,
            COALESCE(se.description, '') as description,
            COALESCE(
                (SELECT string_agg(c.column_name, ' ')
                 FROM information_schema.columns c
                 WHERE c.table_schema = 'public' AND c.table_name = t.table_name),
                ''
            ) as column_names
        FROM information_schema.tables t
        LEFT JOIN public.schema_embeddings se ON se.table_name = t.table_name
        WHERE t.table_schema = 'public'
          AND t.table_name NOT IN ('schema_embeddings', 'schema_foreign_keys', 'analytics_orders')
        ORDER BY t.table_name;
        """

        async with pool.acquire() as connection:
            rows = await connection.fetch(query_sql)

        corpus_tokens: List[List[str]] = []
        corpus_metadata: List[Dict[str, Any]] = []
        name_to_idx: Dict[str, int] = {}

        for idx, row in enumerate(rows):
            t_name = row["table_name"]
            domain = row["domain"]
            description = row["description"]
            col_names = row["column_names"]

            doc_text = f"{t_name} {domain} {description} {col_names}"
            tokens = self._tokenize(doc_text)
            corpus_tokens.append(tokens)

            meta = {
                "table_name": t_name,
                "domain": domain,
                "description": description,
                "column_names": col_names,
            }
            corpus_metadata.append(meta)
            name_to_idx[t_name] = idx

        new_bm25 = BM25Okapi(corpus_tokens)
        self._bm25_index = new_bm25
        self._corpus_metadata = corpus_metadata
        self._table_name_to_corpus_idx = name_to_idx
        self._last_index_sync_time = time.perf_counter()

        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.info(
            "Indeks BM25 zainicjalizowany pomyślnie dla %d tabel w %.2f ms.",
            len(corpus_metadata),
            elapsed_ms,
        )

    async def _ensure_relation_graph(self, pool: asyncpg.Pool, force_rebuild: bool = False) -> nx.DiGraph:
        """Graf FK z public.schema_foreign_keys (krawędź: tabela źródłowa -> docelowa)."""
        if self._relation_graph is not None and not force_rebuild:
            return self._relation_graph

        logger.info("Ładowanie grafu relacji z tabeli 'public.schema_foreign_keys'...")
        start_time = time.perf_counter()

        query_sql = """
        SELECT source_table, target_table, constraint_name, join_columns
        FROM public.schema_foreign_keys;
        """

        async with pool.acquire() as connection:
            fk_rows = await connection.fetch(query_sql)

        graph = nx.DiGraph()
        for row in fk_rows:
            graph.add_edge(
                row["source_table"],
                row["target_table"],
                constraint_name=row["constraint_name"],
                join_columns=row["join_columns"],
            )

        self._relation_graph = graph
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.info(
            "Graf relacji NetworkX zainicjalizowany: %d węzłów, %d krawędzi (czas: %.2f ms).",
            graph.number_of_nodes(),
            graph.number_of_edges(),
            elapsed_ms,
        )
        return graph

    def invalidate_cache(
        self,
        clear_bm25: bool = True,
        clear_graph: bool = True,
        clear_columns: bool = True,
    ) -> None:
        if clear_bm25:
            self._bm25_index = None
            self._corpus_metadata.clear()
            self._table_name_to_corpus_idx.clear()
        if clear_graph:
            self._relation_graph = None
        if clear_columns:
            self._columns_by_table_cache.clear()
        logger.info(
            "Pamięć podręczna SchemaEngine unieważniona (bm25=%s, graph=%s, columns=%s).",
            clear_bm25,
            clear_graph,
            clear_columns,
        )

    async def reload_metadata(
        self, pool: Optional[asyncpg.Pool] = None
    ) -> Dict[str, Any]:
        """Przebudowa BM25 i grafu od zera. Zwraca czasy, bo mierzy to bench_schema_drift."""
        active_pool = self._get_active_pool(pool)
        t_start = time.perf_counter()

        self._columns_by_table_cache.clear()

        t_bm25_start = time.perf_counter()
        await self._ensure_bm25_index(active_pool, force_rebuild=True)
        bm25_ms = (time.perf_counter() - t_bm25_start) * 1000.0

        t_graph_start = time.perf_counter()
        graph = await self._ensure_relation_graph(active_pool, force_rebuild=True)
        graph_ms = (time.perf_counter() - t_graph_start) * 1000.0

        total_ms = (time.perf_counter() - t_start) * 1000.0
        self._last_index_sync_time = time.perf_counter()

        logger.info(
            "Przeładowano metadane schematu: BM25=%.2f ms, Graph=%.2f ms, Razem=%.2f ms (węzłów: %d, krawędzi: %d).",
            bm25_ms,
            graph_ms,
            total_ms,
            graph.number_of_nodes(),
            graph.number_of_edges(),
        )
        return {
            "bm25_rebuild_ms": round(bm25_ms, 3),
            "graph_rebuild_ms": round(graph_ms, 3),
            "total_reload_ms": round(total_ms, 3),
            "nodes_count": graph.number_of_nodes(),
            "edges_count": graph.number_of_edges(),
        }

    async def sync_embeddings_for_new_tables(
        self,
        pool: Optional[asyncpg.Pool] = None,
        table_names: Optional[List[str]] = None,
        domain: str = "dynamic_drift",
    ) -> Dict[str, Any]:
        """
        Liczy embeddingi dla tabel, których nie ma jeszcze w schema_embeddings
        (albo dla podanych table_names) i zapisuje je do bazy.
        """
        active_pool = self._get_active_pool(pool)
        t_start = time.perf_counter()

        async with active_pool.acquire() as conn:
            if table_names is None:
                missing_rows = await conn.fetch("""
                    SELECT t.table_name,
                           COALESCE(
                               (SELECT string_agg(c.column_name, ' ')
                                FROM information_schema.columns c
                                WHERE c.table_schema = 'public' AND c.table_name = t.table_name),
                               ''
                           ) as column_names
                    FROM information_schema.tables t
                    LEFT JOIN public.schema_embeddings se ON se.table_name = t.table_name
                    WHERE t.table_schema = 'public'
                      AND se.table_name IS NULL
                      AND t.table_name NOT IN ('schema_embeddings', 'schema_foreign_keys', 'analytics_orders');
                """)
                target_tables = [dict(r) for r in missing_rows]
            else:
                cols_rows = await conn.fetch("""
                    SELECT t.table_name,
                           COALESCE(
                               (SELECT string_agg(c.column_name, ' ')
                                FROM information_schema.columns c
                                WHERE c.table_schema = 'public' AND c.table_name = t.table_name),
                               ''
                           ) as column_names
                    FROM information_schema.tables t
                    WHERE t.table_schema = 'public' AND t.table_name = ANY($1::text[]);
                """, table_names)
                target_tables = [dict(r) for r in cols_rows]

        if not target_tables:
            return {
                "new_tables_count": 0,
                "embedding_calc_ms": 0.0,
                "db_insert_ms": 0.0,
                "total_sync_ms": 0.0,
            }

        # obok czasu ściennego liczymy też czas CPU procesu (raportowany w drifcie)
        t_enc_start = time.perf_counter()
        cpu_enc_start = time.process_time()
        docs_to_embed = [
            f"{r['table_name']} {domain} Dynamic domain table for schema drift evaluation. Columns: {r['column_names']}"
            for r in target_tables
        ]
        embeddings = self.model.encode(docs_to_embed, normalize_embeddings=True, show_progress_bar=False)
        enc_ms = (time.perf_counter() - t_enc_start) * 1000.0
        enc_cpu_ms = (time.process_time() - cpu_enc_start) * 1000.0

        t_db_start = time.perf_counter()
        async with active_pool.acquire() as conn:
            for r, emb in zip(target_tables, embeddings):
                vec_str = str(emb.tolist())
                await conn.execute("DELETE FROM public.schema_embeddings WHERE table_name = $1;", r["table_name"])
                await conn.execute(
                    "INSERT INTO public.schema_embeddings (table_name, domain, description, embedding) VALUES ($1, $2, $3, $4::vector);",
                    r["table_name"],
                    domain,
                    f"Tabela domenowa {r['table_name']} wygenerowana podczas migracji schema drift.",
                    vec_str,
                )
        db_ms = (time.perf_counter() - t_db_start) * 1000.0
        total_ms = (time.perf_counter() - t_start) * 1000.0

        logger.info(
            "Zsynchronizowano osadzenia dla %d nowych tabel: Encode=%.2f ms (CPU: %.2f ms), DB Insert=%.2f ms, Total=%.2f ms.",
            len(target_tables),
            enc_ms,
            enc_cpu_ms,
            db_ms,
            total_ms,
        )
        return {
            "new_tables_count": len(target_tables),
            "embedding_calc_ms": round(enc_ms, 3),
            "embedding_cpu_ms": round(enc_cpu_ms, 3),
            "db_insert_ms": round(db_ms, 3),
            "total_sync_ms": round(total_ms, 3),
        }

    async def handle_schema_drift(
        self,
        pool: Optional[asyncpg.Pool] = None,
        new_tables: Optional[List[str]] = None,
        domain: str = "dynamic_drift",
    ) -> Dict[str, Any]:
        """
        Reakcja na nowe tabele w bazie: embeddingi dla nich + przebudowa BM25 i grafu.
        Pod lockiem, żeby dwa równoległe drifty nie przebudowywały indeksów naraz.
        """
        active_pool = self._get_active_pool(pool)
        t0 = time.perf_counter()
        cpu0 = time.process_time()

        async with self._get_drift_lock():
            emb_res = await self.sync_embeddings_for_new_tables(active_pool, table_names=new_tables, domain=domain)

            reload_res = await self.reload_metadata(active_pool)

            total_drift_ms = (time.perf_counter() - t0) * 1000.0
            total_cpu_ms = (time.process_time() - cpu0) * 1000.0

            return {
                "status": "DRIFT_HANDLED_SUCCESS",
                "new_tables_count": emb_res["new_tables_count"],
                "embedding_calc_ms": emb_res["embedding_calc_ms"],
                "embedding_cpu_ms": emb_res.get("embedding_cpu_ms", 0.0),
                "db_insert_ms": emb_res["db_insert_ms"],
                "bm25_rebuild_ms": reload_res["bm25_rebuild_ms"],
                "graph_rebuild_ms": reload_res["graph_rebuild_ms"],
                "total_drift_reload_ms": round(total_drift_ms, 3),
                "total_cpu_ms": round(total_cpu_ms, 3),
                "nodes_count": reload_res["nodes_count"],
                "edges_count": reload_res["edges_count"],
            }

    async def _fetch_column_definitions(
        self, pool: asyncpg.Pool, table_names: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        # z cache, dociągamy tylko brakujące tabele
        if not table_names:
            return {}

        missing_tables = [t for t in table_names if t not in self._columns_by_table_cache]
        if missing_tables:
            query_sql = """
            SELECT
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ANY($1::text[])
            ORDER BY table_name, ordinal_position;
            """
            async with pool.acquire() as connection:
                rows = await connection.fetch(query_sql, missing_tables)

            for row in rows:
                t_name = row["table_name"]
                if t_name not in self._columns_by_table_cache:
                    self._columns_by_table_cache[t_name] = []
                self._columns_by_table_cache[t_name].append(
                    {
                        "column_name": row["column_name"],
                        "data_type": row["data_type"],
                        "is_nullable": row["is_nullable"] == "YES",
                        "column_default": row["column_default"],
                    }
                )

        return {t: self._columns_by_table_cache.get(t, []) for t in table_names}

    # --- strategie wyszukiwania ---

    async def retrieve_bm25(
        self,
        query: str,
        top_k: int = 5,
        pool: Optional[asyncpg.Pool] = None,
    ) -> List[Dict[str, Any]]:
        active_pool = self._get_active_pool(pool)
        await self._ensure_bm25_index(active_pool)

        if not self._bm25_index or not self._corpus_metadata:
            return []

        tokenized_query = self._tokenize(query)
        if not tokenized_query:
            # puste zapytanie (albo same jednoliterowe słowa) - po prostu pierwsze top_k
            results = []
            for rank_idx, meta in enumerate(self._corpus_metadata[:top_k]):
                results.append(
                    {
                        "table_name": meta["table_name"],
                        "domain": meta["domain"],
                        "description": meta["description"],
                        "score": 0.0,
                        "rank": rank_idx + 1,
                    }
                )
            return results

        scores = self._bm25_index.get_scores(tokenized_query)
        ranked_indices = sorted(
            range(len(scores)),
            key=lambda idx: scores[idx],
            reverse=True,
        )[:top_k]

        results: List[Dict[str, Any]] = []
        for rank_idx, doc_idx in enumerate(ranked_indices):
            meta = self._corpus_metadata[doc_idx]
            results.append(
                {
                    "table_name": meta["table_name"],
                    "domain": meta["domain"],
                    "description": meta["description"],
                    "score": float(scores[doc_idx]),
                    "rank": rank_idx + 1,
                }
            )

        return results

    async def retrieve_dense(
        self,
        query: str,
        top_k: int = 5,
        pool: Optional[asyncpg.Pool] = None,
    ) -> List[Dict[str, Any]]:
        active_pool = self._get_active_pool(pool)

        query_embedding = self.model.encode(query, normalize_embeddings=True)
        vector_str = str(query_embedding.tolist())

        search_sql = """
        SELECT
            table_name,
            domain,
            description,
            1 - (embedding <=> $1::vector) AS similarity
        FROM public.schema_embeddings
        ORDER BY embedding <=> $1::vector ASC
        LIMIT $2;
        """

        async with active_pool.acquire() as connection:
            rows = await connection.fetch(search_sql, vector_str, top_k)

        results: List[Dict[str, Any]] = []
        for rank_idx, row in enumerate(rows):
            sim = float(row["similarity"])
            results.append(
                {
                    "table_name": row["table_name"],
                    "domain": row["domain"],
                    "description": row["description"],
                    "similarity": sim,
                    "score": sim,
                    "rank": rank_idx + 1,
                }
            )

        return results

    async def retrieve_hybrid_rrf(
        self,
        query: str,
        top_k: int = 5,
        pool: Optional[asyncpg.Pool] = None,
    ) -> List[Dict[str, Any]]:
        """
        RRF(d) = 1/(60 + rank_bm25) + 1/(60 + rank_dense), k=60 jak w oryginalnej pracy Cormacka.
        Z każdej strategii bierzemy więcej kandydatów niż top_k, inaczej fuzja nie ma z czego wybierać.
        """
        active_pool = self._get_active_pool(pool)

        candidate_size = max(top_k * 4, 60)
        bm25_results = await self.retrieve_bm25(query=query, top_k=candidate_size, pool=active_pool)
        dense_results = await self.retrieve_dense(query=query, top_k=candidate_size, pool=active_pool)

        k_rrf = 60.0
        rrf_scores: Dict[str, float] = defaultdict(float)
        table_metadata: Dict[str, Dict[str, Any]] = {}
        bm25_ranks: Dict[str, int] = {}
        dense_ranks: Dict[str, int] = {}

        for item in bm25_results:
            t_name = item["table_name"]
            rank = item["rank"]
            bm25_ranks[t_name] = rank
            rrf_scores[t_name] += 1.0 / (k_rrf + rank)
            table_metadata[t_name] = item

        for item in dense_results:
            t_name = item["table_name"]
            rank = item["rank"]
            dense_ranks[t_name] = rank
            rrf_scores[t_name] += 1.0 / (k_rrf + rank)
            if t_name not in table_metadata:
                table_metadata[t_name] = item
            else:
                table_metadata[t_name]["similarity"] = item.get("similarity", 0.0)

        sorted_tables = sorted(rrf_scores.keys(), key=lambda t: rrf_scores[t], reverse=True)[:top_k]

        results: List[Dict[str, Any]] = []
        for rank_idx, t_name in enumerate(sorted_tables):
            meta = table_metadata[t_name]
            score = rrf_scores[t_name]
            results.append(
                {
                    "table_name": t_name,
                    "domain": meta.get("domain", ""),
                    "description": meta.get("description", ""),
                    "rrf_score": score,
                    "score": score,
                    "rank": rank_idx + 1,
                    "rank_bm25": bm25_ranks.get(t_name),
                    "rank_dense": dense_ranks.get(t_name),
                    "similarity": meta.get("similarity", 0.0),
                }
            )

        return results

    async def discover_schema_with_graph_pruning(
        self,
        query: str,
        top_k: int = 5,
        max_expanded_tables: int = 3,
        pool: Optional[asyncpg.Pool] = None,
    ) -> str:
        """
        top_k tabel z RRF, a potem dla każdej pary szukamy najkrótszej ścieżki w grafie FK.
        Jeśli para łączy się przez dokładnie jedną tabelę pośrednią, ta tabela jest kandydatem
        na most. Dokładamy max_expanded_tables najczęściej trafianych mostów. Wynik to DDL dla LLM.
        """
        active_pool = self._get_active_pool(pool)
        _t0 = time.perf_counter_ns()

        base_schemas = await self.retrieve_hybrid_rrf(query=query, top_k=top_k, pool=active_pool)
        base_table_names = [item["table_name"] for item in base_schemas]
        _t_rrf = time.perf_counter_ns()

        if not base_table_names:
            return f"-- Brak pasujących tabel w bazie danych dla zapytania: '{query}'"

        # kierunek FK nie ma znaczenia dla JOIN-a, więc szukamy w grafie nieskierowanym
        G = await self._ensure_relation_graph(active_pool)
        G_undirected = G.to_undirected(as_view=True)
        _t_graph = time.perf_counter_ns()

        bridge_counter: Counter[str] = Counter()

        for i in range(len(base_table_names)):
            for j in range(i + 1, len(base_table_names)):
                u = base_table_names[i]
                v = base_table_names[j]

                if u not in G or v not in G:
                    continue

                try:
                    path = nx.shortest_path(G_undirected, source=u, target=v)
                    # [u, most, v]
                    if len(path) == 3:
                        bridge_node = path[1]
                        if bridge_node not in base_table_names:
                            bridge_counter[bridge_node] += 1
                except (nx.NetworkXNoPath, nx.NodeNotFound, nx.NetworkXError):
                    continue

        selected_bridges: List[str] = [
            node for node, _ in bridge_counter.most_common(max_expanded_tables)
        ]

        final_tables: List[str] = list(base_table_names)
        for bridge in selected_bridges:
            if bridge not in final_tables:
                final_tables.append(bridge)
        _t_bridges = time.perf_counter_ns()

        columns_by_table = await self._fetch_column_definitions(active_pool, final_tables)

        fk_query_sql = """
        SELECT source_table, target_table, constraint_name, join_columns
        FROM public.schema_foreign_keys
        WHERE source_table = ANY($1::text[]) AND target_table = ANY($1::text[]);
        """
        async with active_pool.acquire() as connection:
            active_fks = await connection.fetch(fk_query_sql, final_tables)

        fks_by_source: Dict[str, List[asyncpg.Record]] = defaultdict(list)
        for fk in active_fks:
            fks_by_source[fk["source_table"]].append(fk)

        desc_query_sql = """
        SELECT table_name, domain, description
        FROM public.schema_embeddings
        WHERE table_name = ANY($1::text[]);
        """
        async with active_pool.acquire() as connection:
            desc_rows = await connection.fetch(desc_query_sql, final_tables)

        table_descriptions = {r["table_name"]: r["description"] for r in desc_rows}
        table_domains = {r["table_name"]: r["domain"] for r in desc_rows}

        ddl_output = self._format_compact_enterprise_ddl(
            query=query,
            base_tables=base_table_names,
            bridge_tables=selected_bridges,
            all_tables=final_tables,
            columns_by_table=columns_by_table,
            fks_by_source=fks_by_source,
            table_descriptions=table_descriptions,
            table_domains=table_domains,
        )
        _t_ddl = time.perf_counter_ns()
        self.last_graph_timings = {
            "rrf_ns": _t_rrf - _t0,
            "graph_load_ns": _t_graph - _t_rrf,
            "bridge_search_ns": _t_bridges - _t_graph,
            "ddl_synthesis_ns": _t_ddl - _t_bridges,
            "total_ns": _t_ddl - _t0,
            "n_base_tables": len(base_table_names),
            "n_bridges": len(selected_bridges),
        }

        return ddl_output

    def _format_compact_enterprise_ddl(
        self,
        query: str,
        base_tables: List[str],
        bridge_tables: List[str],
        all_tables: List[str],
        columns_by_table: Dict[str, List[Dict[str, Any]]],
        fks_by_source: Dict[str, List[asyncpg.Record]],
        table_descriptions: Dict[str, str],
        table_domains: Dict[str, str],
    ) -> str:
        """DDL tylko z FK między tabelami, które faktycznie są w kontekście."""
        lines: List[str] = [
            f"-- ============================================================================",
            f"-- SCHEMAT BAZY DANYCH MCP: Progressive Schema Discovery z pruningiem grafu",
            f"-- Zapytanie: \"{query}\"",
            f"-- Tabele bazowe (Hybrid RRF): {', '.join(base_tables)}",
            f"-- Dołączone tabele mostkujące N:M (Graph Bridges): {', '.join(bridge_tables) if bridge_tables else 'Brak'}",
            f"-- Łączna liczba tabel w kontekście: {len(all_tables)}",
            f"-- ============================================================================",
            "",
        ]

        for t_name in all_tables:
            is_bridge = t_name in bridge_tables
            role_label = "TABELA MOSTKUJĄCA N:M (BRIDGE)" if is_bridge else "TABELA BAZOWA (BASE)"
            desc = table_descriptions.get(t_name, "Brak opisu biznesowego.")
            domain = table_domains.get(t_name, "general")

            lines.append(f"-- [{role_label}] public.{t_name} (Domena: {domain})")
            lines.append(f"-- Opis: {desc}")
            lines.append(f"CREATE TABLE public.{t_name} (")

            col_defs: List[str] = []
            cols = columns_by_table.get(t_name, [])

            for c in cols:
                c_name = c["column_name"]
                c_type = c["data_type"].upper()
                nullable = "" if c["is_nullable"] else " NOT NULL"
                default_val = f" DEFAULT {c['column_default']}" if c.get("column_default") else ""
                col_defs.append(f"    {c_name} {c_type}{nullable}{default_val}")

            # PK na sztywno na id - generator testbedu zawsze tak zakłada tabele
            col_defs.append("    PRIMARY KEY (id)")

            table_fks = fks_by_source.get(t_name, [])
            for fk in table_fks:
                target_t = fk["target_table"]
                c_name = fk["constraint_name"]
                join_meta = fk["join_columns"]
                if isinstance(join_meta, str):
                    import json
                    join_meta = json.loads(join_meta)

                src_col = join_meta.get("source_column", "id")
                tgt_col = join_meta.get("target_column", "id")
                col_defs.append(
                    f"    CONSTRAINT {c_name} FOREIGN KEY ({src_col}) REFERENCES public.{target_t}({tgt_col})"
                )

            lines.append(",\n".join(col_defs))
            lines.append(");")
            lines.append("")

        return "\n".join(lines).strip()

    # --- starsze API, używane jeszcze przez benchmark_schema.py ---

    async def find_relevant_schemas(
        self,
        pool: asyncpg.Pool,
        query: str,
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        results = await self.retrieve_hybrid_rrf(query=query, top_k=top_k, pool=pool)
        table_names = [r["table_name"] for r in results]
        columns_by_table = await self._fetch_column_definitions(pool, table_names)

        schemas: List[Dict[str, Any]] = []
        for r in results:
            t_name = r["table_name"]
            schemas.append(
                {
                    "table_name": t_name,
                    "domain": r.get("domain", ""),
                    "description": r.get("description", ""),
                    "similarity": float(r.get("similarity", r.get("rrf_score", 0.0))),
                    "columns": columns_by_table.get(t_name, []),
                }
            )

        return schemas

    def format_schemas_as_ddl(self, query: str, schemas: List[Dict[str, Any]]) -> str:
        if not schemas:
            return f"-- Nie znaleziono tabel odpowiadających zapytaniu: '{query}'"

        lines: List[str] = [
            f"-- Wyniki wyszukiwania schematów dla zapytania: \"{query}\"",
            f"-- Liczba odnalezionych tabel: {len(schemas)}",
            "",
        ]

        for s in schemas:
            t_name = s["table_name"]
            domain = s.get("domain", "")
            desc = s.get("description", "")
            sim = s.get("similarity", 0.0)
            cols = s.get("columns", [])

            lines.append(f"-- Tabela: public.{t_name}")
            lines.append(f"-- Domena: {domain} | Wynik trafności: {sim:.4f}")
            lines.append(f"-- Opis biznesowy: {desc}")
            lines.append(f"CREATE TABLE public.{t_name} (")

            col_defs: List[str] = []
            for col in cols:
                c_name = col["column_name"]
                c_type = col["data_type"].upper()
                nullable = "" if col["is_nullable"] else " NOT NULL"
                default_val = f" DEFAULT {col['column_default']}" if col.get("column_default") else ""
                col_defs.append(f"    {c_name} {c_type}{nullable}{default_val}")

            lines.append(",\n".join(col_defs))
            lines.append(");")
            lines.append("")

        return "\n".join(lines).strip()


# uwaga: model SentenceTransformer ładuje się już przy imporcie modułu
schema_engine = SchemaEngine()
