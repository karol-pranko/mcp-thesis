"""
Deterministyczny generator zbioru ewaluacyjnego Text-to-SQL (Scenariusz A).

Pytania i zapytania wzorcowe powstają z szablonów na ścieżkach prostych w grafie kluczy obcych
(30% prostych: 2-3 tabele, 50% średnich: 3-4 tabele, 20% złożonych: 4-6 tabel na domenę).
Kolejność krawędzi i sąsiadów jest sortowana, a ziarno pochodzi z konfiguracji (seeds.query_generator),
więc zbiór jest odtwarzalny niezależnie od PYTHONHASHSEED. Pełny zbiór (pytanie, SQL, tabele GT)
jest zapisywany w raw_results/schema_quality_dataset.jsonl.
"""
from __future__ import annotations

import json
import logging
import random
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import asyncpg

logger = logging.getLogger("text2sql_dataset")


@dataclass
class TestQueryCase:
    """Reprezentacja pojedynczego przypadku testowego Text-to-SQL z referencją Ground Truth."""

    query_id: int
    natural_language_query: str
    primary_domain: str
    ground_truth_tables: List[str]
    join_path: List[Tuple[str, str, str, str]]  # (source_table, target_table, src_col, tgt_col)
    ground_truth_sql: str
    difficulty: str = "medium"  # simple (2-3 tabele), medium (3-4 tabele), complex (4-6 tabel)
    required_tables: List[str] = field(default_factory=list)
    required_columns: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.required_tables:
            self.required_tables = list(self.ground_truth_tables)



class SyntheticQueryBenchmarkGenerator:
    """
    Generator zróżnicowanych zapytań testowych Text-to-SQL opartych na rzeczywistej topologii
    bazy enterprise (1000 tabel) zintegrowanej z kluczami obcymi public.schema_foreign_keys.
    Generuje N zapytań (N/5 na każdą z 5 domen biznesowych)
    ze zróżnicowaniem trudności (30% proste, 50% średnie, 20% złożone).
    """

    def __init__(self, pool: asyncpg.Pool, seed: int = 2026) -> None:
        self.pool = pool
        self.seed = seed
        self.foreign_keys: List[Dict[str, Any]] = []
        self.foreign_keys_pairs: Set[Tuple[str, str]] = set()
        self.fks_by_pair: Dict[Tuple[str, str], Tuple[str, str]] = {}
        self.table_descriptions: Dict[str, str] = {}
        self.table_columns: Dict[str, List[str]] = {}
        self.domain_tables: Dict[str, List[str]] = {}

    async def initialize(self) -> None:
        """Pobiera z bazy relacje kluczy obcych, opisy biznesowe i definicje kolumn."""
        logger.info("Ładowanie metadanych relacyjnych dla generatora zapytań N=1000...")
        async with self.pool.acquire() as connection:
            fk_rows = await connection.fetch(
                "SELECT source_table, target_table, constraint_name, join_columns FROM public.schema_foreign_keys "
                "ORDER BY source_table, target_table, constraint_name;"
            )
            self.foreign_keys = [dict(r) for r in fk_rows]
            for fk in self.foreign_keys:
                src = fk["source_table"]
                tgt = fk["target_table"]
                meta = fk["join_columns"]
                if isinstance(meta, str):
                    meta = json.loads(meta)
                src_col = meta.get("source_column", "id")
                tgt_col = meta.get("target_column", "id")
                self.fks_by_pair[(src, tgt)] = (src_col, tgt_col)
                self.fks_by_pair[(tgt, src)] = (tgt_col, src_col)
                self.foreign_keys_pairs.add((src, tgt))
                self.foreign_keys_pairs.add((tgt, src))

            desc_rows = await connection.fetch(
                "SELECT table_name, domain, description FROM public.schema_embeddings ORDER BY table_name;"
            )
            self.table_descriptions = {r["table_name"]: r["description"] for r in desc_rows}
            for r in desc_rows:
                d = r["domain"]
                if d not in self.domain_tables:
                    self.domain_tables[d] = []
                self.domain_tables[d].append(r["table_name"])

            col_rows = await connection.fetch(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
                """
            )
            for r in col_rows:
                t = r["table_name"]
                if t not in self.table_columns:
                    self.table_columns[t] = []
                self.table_columns[t].append(r["column_name"])

        logger.info(
            "Załadowano %d relacji FK, %d opisów tabel oraz %d tabel domenowych.",
            len(self.foreign_keys),
            len(self.table_descriptions),
            sum(len(v) for v in self.domain_tables.values()),
        )

    def generate_1000_query_dataset(self, total_queries: int = 1000) -> List[TestQueryCase]:
        """
        Generuje deterministycznie zbiór N = 1000 zróżnicowanych zapytań analitycznych Text-to-SQL.
        Pokrywa 5 domen po 200 zapytań:
        - 30% prostych (60 na domenę, złączenia 2-3 tabel),
        - 50% średnich (100 na domenę, 3-4 tabele, GROUP BY / HAVING),
        - 20% złożonych (40 na domenę, 4-6 tabel, podzapytania, CTE).
        """
        logger.info("Generowanie deterministycznego zbioru %d pytań testowych Text-to-SQL...", total_queries)
        import networkx as nx

        # Deterministyczna kolejność krawędzi i sąsiadów (niezależna od PYTHONHASHSEED)
        G = nx.Graph()
        for (src, tgt) in sorted(self.foreign_keys_pairs):
            G.add_edge(src, tgt)

        domains = ["finance", "inventory", "hr", "crm", "telemetry"]
        if total_queries >= len(domains):
            per_domain = total_queries // len(domains)
            count_simple = int(per_domain * 0.30)   # 60 dla N=1000
            count_medium = int(per_domain * 0.50)   # 100 dla N=1000
            count_complex = per_domain - count_simple - count_medium  # 40 dla N=1000
        else:
            domains = domains[:total_queries]
            per_domain = 1
            count_simple = 1
            count_medium = 0
            count_complex = 0

        rnd = random.Random(self.seed)
        dataset: List[TestQueryCase] = []

        domain_meta = {
            "crm": {"grp": "stage", "metric": "deal_value", "agg": "AVG(public.{t}.deal_value)", "time": "created_at"},
            "finance": {"grp": "status", "metric": "amount", "agg": "SUM(public.{t}.amount)", "time": "created_at"},
            "hr": {"grp": "department_code", "metric": "employee_id", "agg": "COUNT(public.{t}.employee_id)", "time": "created_at"},
            "inventory": {"grp": "warehouse_id", "metric": "quantity", "agg": "SUM(public.{t}.quantity)", "time": "created_at"},
            "telemetry": {"grp": "sensor_type", "metric": "metric_value", "agg": "AVG(public.{t}.metric_value)", "time": "recorded_at"},
        }

        def find_path_dfs(curr: str, target_len: int, visited: List[str]) -> Optional[List[str]]:
            if len(visited) == target_len:
                return visited
            nbrs = sorted(G.neighbors(curr))
            rnd.shuffle(nbrs)
            for n in nbrs:
                if n not in visited:
                    res = find_path_dfs(n, target_len, visited + [n])
                    if res:
                        return res
            return None

        query_counter = 1

        for domain in domains:
            dom_nodes = self.domain_tables.get(domain, [])
            if not dom_nodes:
                continue
            meta = domain_meta[domain]
            grp_col = meta["grp"]
            metric_col = meta["metric"]
            time_col = meta["time"]

            # zapytania proste (30% = 60 zapytań)
            for i in range(count_simple):
                target_len = 2 if i % 2 == 0 else 3
                start_u = rnd.choice(dom_nodes)
                path = find_path_dfs(start_u, target_len, [start_u])
                if not path:
                    nbrs = sorted(G.neighbors(start_u))
                    path = [start_u, nbrs[0]] if nbrs else [start_u]

                t1 = path[0]
                joins = []
                join_pairs: List[Tuple[str, str, str, str]] = []
                req_cols = [f"{t1}.id", f"{t1}.{time_col}"]
                selects = [f"public.{t1}.id AS {t1}_id", f"public.{t1}.{time_col}"]

                for idx in range(len(path) - 1):
                    ta = path[idx]
                    tb = path[idx + 1]
                    ca, cb = self.fks_by_pair.get((ta, tb), ("id", "id"))
                    joins.append(f"JOIN public.{tb} ON public.{ta}.{ca} = public.{tb}.{cb}")
                    join_pairs.append((ta, tb, ca, cb))
                    selects.append(f"public.{tb}.id AS {tb}_id")
                    req_cols.extend([f"{ta}.{ca}", f"{tb}.{cb}", f"{tb}.id"])

                sql = f"SELECT {', '.join(selects)} FROM public.{t1} {' '.join(joins)} WHERE public.{t1}.id > 0 LIMIT 50;"
                requested_attrs = [f"identyfikator {t1}.id AS {t1}_id", f"znacznik czasu {t1}.{time_col}"]
                for p_tbl in path[1:]:
                    requested_attrs.append(f"identyfikator powiązany {p_tbl}.id AS {p_tbl}_id")
                attrs_str = ", ".join(requested_attrs)
                nl = (
                    f"Pobierz jawne atrybuty biznesowe: {attrs_str} poprzez relacyjne złączenie tabeli bazowej {t1} "
                    f"oraz encji powiązanych ({', '.join(path[1:])}), filtrując rekordy o {t1}.id > 0 (LIMIT 50)."
                )

                dataset.append(
                    TestQueryCase(
                        query_id=query_counter,
                        natural_language_query=nl,
                        primary_domain=domain,
                        ground_truth_tables=list(path),
                        join_path=join_pairs,
                        ground_truth_sql=sql,
                        difficulty="simple",
                        required_tables=list(path),
                        required_columns=list(set(req_cols)),
                    )
                )
                query_counter += 1

            # zapytania średnie (50% = 100 zapytań, GROUP BY / HAVING)
            for i in range(count_medium):
                target_len = 3 if i % 2 == 0 else 4
                start_u = rnd.choice(dom_nodes)
                path = find_path_dfs(start_u, target_len, [start_u])
                if not path:
                    nbrs = sorted(G.neighbors(start_u))
                    path = [start_u, nbrs[0], nbrs[1]] if len(nbrs) >= 2 else [start_u, nbrs[0]]

                t1 = path[0]
                joins = []
                join_pairs: List[Tuple[str, str, str, str]] = []
                req_cols = [f"{t1}.{grp_col}", f"{t1}.id"]
                agg_expr = meta["agg"].format(t=t1)

                for idx in range(len(path) - 1):
                    ta = path[idx]
                    tb = path[idx + 1]
                    ca, cb = self.fks_by_pair.get((ta, tb), ("id", "id"))
                    joins.append(f"JOIN public.{tb} ON public.{ta}.{ca} = public.{tb}.{cb}")
                    join_pairs.append((ta, tb, ca, cb))
                    req_cols.extend([f"{ta}.{ca}", f"{tb}.{cb}"])

                sql = (
                    f"SELECT public.{t1}.{grp_col}, COUNT(public.{t1}.id) AS total_count, {agg_expr} AS aggregate_metric "
                    f"FROM public.{t1} {' '.join(joins)} WHERE public.{t1}.id > 0 "
                    f"GROUP BY public.{t1}.{grp_col} HAVING COUNT(public.{t1}.id) >= 1 ORDER BY total_count DESC LIMIT 50;"
                )
                nl = (
                    f"Wyznacz zestawienie atrybutów biznesowych: wymiar grupowania {t1}.{grp_col}, liczebność grupy COUNT({t1}.id) AS total_count "
                    f"oraz zagregowaną metrykę analityczną ({agg_expr} AS aggregate_metric), łącząc tabelę {t1} z tabelami "
                    f"({', '.join(path[1:])}), filtrując rekordy o {t1}.id > 0, grupując po {t1}.{grp_col} "
                    f"z warunkiem HAVING total_count >= 1 i sortowaniem malejącym ORDER BY total_count DESC (LIMIT 50)."
                )

                dataset.append(
                    TestQueryCase(
                        query_id=query_counter,
                        natural_language_query=nl,
                        primary_domain=domain,
                        ground_truth_tables=list(path),
                        join_path=join_pairs,
                        ground_truth_sql=sql,
                        difficulty="medium",
                        required_tables=list(path),
                        required_columns=list(set(req_cols)),
                    )
                )
                query_counter += 1

            # zapytania złożone (20% = 40 zapytań, CTE i relacje wielopoziomowe 4-6 tabel)
            for i in range(count_complex):
                target_len = 4 + (i % 3)  # 4, 5 lub 6 tabel
                start_u = rnd.choice(dom_nodes)
                path = find_path_dfs(start_u, target_len, [start_u])
                if not path:
                    nbrs = sorted(G.neighbors(start_u))
                    path = [start_u] + nbrs[: min(len(nbrs), target_len - 1)]

                t1 = path[0]
                agg_expr = meta["agg"].format(t=t1)
                joins = []
                join_pairs: List[Tuple[str, str, str, str]] = []
                selects = [f"public.{t}.id AS {t}_id" for t in path[1:]]
                req_cols = [f"{t1}.id", f"{t1}.{grp_col}"]

                for idx in range(len(path) - 1):
                    ta = path[idx]
                    tb = path[idx + 1]
                    ca, cb = self.fks_by_pair.get((ta, tb), ("id", "id"))
                    joins.append(f"JOIN public.{tb} ON public.{ta}.{ca} = public.{tb}.{cb}")
                    join_pairs.append((ta, tb, ca, cb))
                    req_cols.extend([f"{ta}.{ca}", f"{tb}.{cb}", f"{tb}.id"])

                sql = (
                    f"WITH base_summary AS (\n"
                    f"    SELECT public.{t1}.id AS base_id, public.{t1}.{grp_col}, {agg_expr} AS metric_val\n"
                    f"    FROM public.{t1}\n"
                    f"    WHERE public.{t1}.id > 0\n"
                    f"    GROUP BY public.{t1}.id, public.{t1}.{grp_col}\n"
                    f")\n"
                    f"SELECT b.base_id, b.{grp_col}, b.metric_val, {', '.join(selects)}\n"
                    f"FROM base_summary b\n"
                    f"JOIN public.{t1} ON b.base_id = public.{t1}.id\n"
                    f"{' '.join(joins)}\n"
                    f"WHERE b.metric_val IS NOT NULL\n"
                    f"ORDER BY b.base_id ASC\n"
                    f"LIMIT 50;"
                )
                cte_attrs = [f"b.base_id ({t1}.id)", f"b.{grp_col}", "b.metric_val (wartość agregacji)"]
                for p_tbl in path[1:]:
                    cte_attrs.append(f"{p_tbl}_id (public.{p_tbl}.id)")
                cte_attrs_str = ", ".join(cte_attrs)
                nl = (
                    f"Sporządź zaawansowane zapytanie analityczne CTE bazujące na tabeli {t1} zwracające jawne kolumny: "
                    f"{cte_attrs_str}, integrujące relacje z tabelami ({', '.join(path[1:])}), "
                    f"filtrując b.metric_val IS NOT NULL z sortowaniem ORDER BY b.base_id ASC (LIMIT 50)."
                )

                dataset.append(
                    TestQueryCase(
                        query_id=query_counter,
                        natural_language_query=nl,
                        primary_domain=domain,
                        ground_truth_tables=list(path),
                        join_path=join_pairs,
                        ground_truth_sql=sql,
                        difficulty="complex",
                        required_tables=list(path),
                        required_columns=list(set(req_cols)),
                    )
                )
                query_counter += 1

        logger.info("Pomyślnie wygenerowano %d deterministycznych przypadków Ground Truth.", len(dataset))
        return dataset

    generate_query_dataset = generate_1000_query_dataset


def dataset_to_records(dataset: List[TestQueryCase]) -> List[Dict[str, Any]]:
    return [asdict(c) for c in dataset]
