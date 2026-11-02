"""
Jedyne miejsce definicji metryk Text-to-SQL: Valid SQL, Execution Accuracy (EX) i taksonomia błędów.

Valid SQL
    Zapytanie wygenerowane przez model (po ekstrakcji z odpowiedzi) zostało zaakceptowane przez
    PostgreSQL w poleceniu `EXPLAIN` ORAZ wykonane bez błędu w limicie `statement_timeout`.
    Nie jest to sama poprawność składniowa (AST): odwołanie do nieistniejącej kolumny daje Valid SQL = False.

Execution Accuracy (EX)
    EX = True wtedy i tylko wtedy, gdy Valid SQL = True, zapytanie wzorcowe (GT) zwróciło co najmniej
    jeden wiersz i wyniki są równe według reguły `ex_comparison`:
      - "multiset_unless_order_by" (domyślnie): porównanie wartości wierszy (bez nazw kolumn) jako
        multizbiorów; jeśli zapytanie GT zawiera ORDER BY na najwyższym poziomie, porównywana jest
        lista wierszy w kolejności;
      - "strict_dict_list": lista słowników {kolumna: wartość} w kolejności (definicja z poprzednich
        przebiegów; zapisywana zawsze jako `ex_match_strict` dla porównywalności).
    Pusty wynik GT oznacza nieważną próbkę (`gt_empty = True`, EX = False); przed przebiegiem
    kontrola wstępna wymaga, by tabele GT nie były puste.

Taksonomia błędów
    Każdy rekord z EX = False otrzymuje dokładnie jedną kategorię z ERROR_CATEGORIES; suma kategorii
    równa się liczbie rekordów z EX = False (sprawdzane przez `assert_taxonomy_consistent`).
"""
from __future__ import annotations

import re
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

SUCCESS = "SUCCESS"
ERROR_CATEGORIES = [
    "TRUNCATION",
    "SYNTAX_ERROR",
    "TABLE_MISSING",
    "COLUMN_HALLUCINATION",
    "JOIN_CONDITION_MISMATCH",
    "SEMANTIC_MISMATCH",
    "GT_EMPTY",
]


def extract_clean_sql(raw_text: str) -> str:
    """Wycina blok ```sql```, tekst przed SELECT/WITH i tekst po pierwszym średniku."""
    text = (raw_text or "").strip()
    if "```" in text:
        matches = re.findall(r"```(?:sql)?\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
        if matches:
            text = matches[0].strip()
        else:  # niedomknięty blok (np. ucięcie przez num_predict)
            text = re.sub(r"^```(?:sql)?", "", text, flags=re.IGNORECASE).strip()
    m = re.search(r"\b(SELECT|WITH)\b", text, re.IGNORECASE)
    if m:
        text = text[m.start():]
    text = text[: text.index(";") + 1] if ";" in text else text.strip() + ";"
    return text.strip()


def _has_top_level_order_by(sql: str) -> bool:
    depth = 0
    s = sql.upper()
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and s.startswith("ORDER BY", i):
            return True
    return False


def _norm_value(v: Any) -> Any:
    if isinstance(v, float):
        return round(v, 6)
    try:
        from decimal import Decimal

        if isinstance(v, Decimal):
            return round(float(v), 6)
    except Exception:
        pass
    return v if isinstance(v, (int, str, bool, type(None))) else str(v)


def results_equal(pred_rows: Sequence[Dict[str, Any]], gt_rows: Sequence[Dict[str, Any]], gt_sql: str,
                  rule: str = "multiset_unless_order_by") -> bool:
    if rule == "strict_dict_list":
        return [dict(r) for r in pred_rows] == [dict(r) for r in gt_rows]
    p = [tuple(_norm_value(v) for v in r.values()) for r in pred_rows]
    g = [tuple(_norm_value(v) for v in r.values()) for r in gt_rows]
    if _has_top_level_order_by(gt_sql):
        return p == g
    return Counter(map(repr, p)) == Counter(map(repr, g))


async def evaluate_sql(conn: Any, sql: str, gt_sql: str, gt_rows: Optional[List[Dict[str, Any]]],
                       rule: str) -> Dict[str, Any]:
    """Wykonuje EXPLAIN i zapytanie modelu, a następnie porównuje wynik z GT (definicje w docstringu modułu)."""
    out: Dict[str, Any] = {"valid_sql": False, "ex_match": False, "ex_match_strict": False, "exec_error": None,
                           "n_rows_pred": None, "n_rows_gt": None if gt_rows is None else len(gt_rows),
                           "gt_empty": gt_rows is not None and len(gt_rows) == 0}
    try:
        await conn.execute(f"EXPLAIN {sql}")
        pred = await conn.fetch(sql)
        out["valid_sql"] = True
        out["n_rows_pred"] = len(pred)
    except Exception as exc:
        out["exec_error"] = str(exc)[:500]
        return out
    if gt_rows is None or len(gt_rows) == 0:
        return out
    pred_d = [dict(r) for r in pred]
    out["ex_match"] = results_equal(pred_d, gt_rows, gt_sql, rule)
    out["ex_match_strict"] = results_equal(pred_d, gt_rows, gt_sql, "strict_dict_list")
    return out


def classify_error(generated_sql: str, valid_sql: bool, ex_match: bool, gt_empty: bool, error_message: Optional[str],
                   done_reason: Optional[str], eval_count: Optional[int], num_predict: int,
                   retrieved_tables: Set[str], foreign_keys_pairs: Set[Tuple[str, str]]) -> str:
    """Przypisuje rekordowi dokładnie jedną kategorię (SUCCESS lub jedną z ERROR_CATEGORIES)."""
    if valid_sql and ex_match:
        return SUCCESS
    if gt_empty:
        return "GT_EMPTY"
    if valid_sql:
        return "SEMANTIC_MISMATCH"
    sql = (generated_sql or "").strip()
    err = (error_message or "").lower()
    capped = done_reason == "length" or (eval_count is not None and eval_count >= num_predict)
    if capped:
        return "TRUNCATION"
    tables = set(re.findall(r"\b(?:FROM|JOIN)\s+(?:public\.)?([a-zA-Z_][a-zA-Z0-9_]*)", sql, re.IGNORECASE))
    ctes = set(re.findall(r"(?:\bWITH|,)\s*(?:RECURSIVE\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(", sql, re.IGNORECASE))
    tables = {t for t in tables if t.lower() not in {"lateral", "select"} and t not in ctes}
    if ("relation" in err and "does not exist" in err) or any(t not in retrieved_tables for t in tables):
        return "TABLE_MISSING"
    if "column" in err and "does not exist" in err:
        return "COLUMN_HALLUCINATION"
    if "missing from-clause entry" in err or "ambiguous" in err:
        return "JOIN_CONDITION_MISMATCH"
    for t1, t2 in re.findall(r"\bON\s+(?:public\.)?([a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+\s*=\s*(?:public\.)?([a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+", sql, re.IGNORECASE):
        if t1 in retrieved_tables and t2 in retrieved_tables and (t1, t2) not in foreign_keys_pairs and (t2, t1) not in foreign_keys_pairs:
            return "JOIN_CONDITION_MISMATCH"
    return "SYNTAX_ERROR"


def assert_taxonomy_consistent(records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    """Każdy rekord z EX = False ma dokładnie jedną kategorię błędu; suma kategorii = liczba rekordów z EX = False."""
    recs = list(records)
    n_false = sum(1 for r in recs if not r["ex_match"])
    counts = Counter(r["error_category"] for r in recs if not r["ex_match"])
    assert all(c in ERROR_CATEGORIES for c in counts), f"nieznane kategorie: {set(counts) - set(ERROR_CATEGORIES)}"
    assert all(r["error_category"] == SUCCESS for r in recs if r["ex_match"]), "rekord z EX = True ma kategorię błędu"
    assert sum(counts.values()) == n_false, f"suma kategorii {sum(counts.values())} != liczba EX=False {n_false}"
    return dict(counts)
