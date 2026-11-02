"""
[F1] Poprawność predykatu keyset przy wartościach NULL na granicy strony.

Zapytania generowane przez build_keyset_query są wykonywane na bazie SQLite w pamięci
(ta sama semantyka trójwartościowa porównań z NULL i jawne NULLS FIRST/LAST w ORDER BY),
więc test nie wymaga serwera PostgreSQL. Stronicowanie po stronach o rozmiarze 3 musi dać
dokładnie tę samą sekwencję identyfikatorów co jedno zapytanie z pełnym ORDER BY, dla wszystkich
kombinacji ASC/DESC x NULLS FIRST/LAST, w trybie DNF i UNION ALL. Dane zawierają wiele wartości
NULL i remisów, więc granice stron wypadają także na wierszach z NULL.
"""
import itertools
import re
import sqlite3

import pytest

from src.mcp_server.pagination import build_keyset_query

# (id, c, d): c i d z wartościami NULL i remisami
ROWS = [
    (1, 5, 1), (2, None, 2), (3, 5, None), (4, 1, 1), (5, None, None), (6, 3, 2), (7, 5, 1),
    (8, None, 1), (9, 3, None), (10, 1, 2), (11, None, 2), (12, 5, 2), (13, 3, 1), (14, None, None),
    (15, 1, None), (16, 7, 7), (17, None, 7), (18, 7, None),
]
PAGE = 3


def _db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute('CREATE TABLE t ("id" INTEGER PRIMARY KEY, "c" INTEGER, "d" INTEGER)')
    con.executemany("INSERT INTO t VALUES (?, ?, ?)", ROWS)
    return con


def _to_sqlite(sql: str) -> str:
    """$N -> ?N; podzapytania UNION ALL w nawiasach -> postać akceptowana przez SQLite."""
    sql = re.sub(r"\$(\d+)", r"?\1", sql)
    if "UNION ALL" in sql:
        sql = sql.replace("(SELECT ", "SELECT * FROM (SELECT ")
    return sql.rstrip(";")


def _paginate(con, order_by, use_union_all):
    cols = [c for c, _, _ in order_by]
    seen, cursor = [], None
    for _ in range(len(ROWS) + 2):
        sql, params = build_keyset_query('"t"', columns="*", order_by=order_by, cursor_data=cursor,
                                         fetch_limit=PAGE, use_union_all=use_union_all)
        rows = con.execute(_to_sqlite(sql), params).fetchall()
        if not rows:
            break
        seen.extend(r[0] for r in rows)
        last = dict(zip(("id", "c", "d"), rows[-1]))
        cursor = {"last_values": [last[c] for c in cols]}
    return seen


def _reference(con, order_by):
    clause = ", ".join(f'"{c}" {d} {n}' for c, d, n in order_by)
    return [r[0] for r in con.execute(f"SELECT id FROM t ORDER BY {clause}")]


COMBOS = list(itertools.product(["ASC", "DESC"], ["NULLS FIRST", "NULLS LAST"]))


@pytest.mark.parametrize("use_union_all", [False, True])
@pytest.mark.parametrize("dir_c,nulls_c", COMBOS)
def test_single_nullable_column(dir_c, nulls_c, use_union_all):
    con = _db()
    order_by = [("c", dir_c, nulls_c), ("id", "ASC", "NULLS LAST")]
    got = _paginate(con, order_by, use_union_all)
    assert got == _reference(con, order_by)
    assert len(got) == len(set(got)) == len(ROWS)


@pytest.mark.parametrize("use_union_all", [False, True])
@pytest.mark.parametrize("spec_c,spec_d", list(itertools.product(COMBOS, COMBOS)))
def test_two_nullable_columns_mixed(spec_c, spec_d, use_union_all):
    con = _db()
    order_by = [("c", *spec_c), ("d", *spec_d), ("id", "ASC", "NULLS LAST")]
    got = _paginate(con, order_by, use_union_all)
    assert got == _reference(con, order_by)


def test_nulls_last_boundary_on_null_has_no_progress_condition():
    """Granica strony na NULL przy ASC NULLS LAST: równość przez IS NULL, brak warunku 'c > NULL'."""
    sql, params = build_keyset_query('"t"', order_by=[("c", "ASC", "NULLS LAST"), ("id", "ASC", "NULLS LAST")],
                                     cursor_data={"last_values": [None, 5]}, fetch_limit=3)
    assert sql == ('SELECT * FROM "t" WHERE ("c" IS NULL AND "id" > $1) '
                   'ORDER BY "c" ASC NULLS LAST, "id" ASC LIMIT $2;')
    assert params == [5, 3]


def test_nulls_first_boundary_on_null_moves_to_non_null():
    sql, params = build_keyset_query('"t"', order_by=[("c", "DESC", "NULLS FIRST"), ("id", "ASC", "NULLS LAST")],
                                     cursor_data={"last_values": [None, 5]}, fetch_limit=3)
    assert sql == ('SELECT * FROM "t" WHERE (("c" IS NOT NULL) OR ("c" IS NULL AND "id" > $1)) '
                   'ORDER BY "c" DESC NULLS FIRST, "id" ASC LIMIT $2;')
    assert params == [5, 3]
