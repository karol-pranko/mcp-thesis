#!/usr/bin/env python3
"""
Etap A, korekta A7: przeliczenie taksonomii błędów Text-to-SQL z danych per zapytanie.

Plik raw_results/schema_error_decomposition.csv agreguje wiersze dla k ∈ {3, 5, 10, 20}, które są
kopiami tego samego wywołania LLM przy k = 5 (bench_schema_quality.py, l. 1169-1202). Każde
zapytanie jest tam policzone 4 razy (stąd liczności > 500). Tu liczymy wyłącznie k = 5 (N = 500
unikalnych zapytań na strategię i profil).

Wynik: out/taksonomia_bledow_k5.csv (liczby bezwzględne i % względem EX = False) oraz
out/bledy_kolumn_komunikaty.csv (struktura komunikatów PostgreSQL dla COLUMN_HALLUCINATION).
"""
import pandas as pd

from common import OUT, load_schema_quality

CATS = [
    "TRUNCATION",
    "SYNTAX_ERROR",
    "TABLE_MISSING",
    "COLUMN_HALLUCINATION",
    "JOIN_CONDITION_MISMATCH",
    "SEMANTIC_MISMATCH",
]


def main() -> None:
    df = load_schema_quality()
    d = df[df["top_k"] == 5]
    fail = d[~d["ex_match"]]

    # Kontrola spójności: kategoria SUCCESS <=> EX = True
    assert (d.loc[d["ex_match"], "error_category"] == "SUCCESS").all()
    assert (fail["error_category"] != "SUCCESS").all()

    cnt = fail.groupby(["eng", "s", "error_category"]).size().unstack(fill_value=0).reindex(columns=CATS, fill_value=0)
    cnt["EX_False"] = cnt.sum(axis=1)
    n_fail = d.groupby(["eng", "s"])["ex_match"].apply(lambda x: int((~x).sum()))
    assert (cnt["EX_False"] == n_fail.reindex(cnt.index)).all()
    pct = cnt[CATS].div(cnt["EX_False"], axis=0) * 100.0

    long = (
        cnt[CATS].stack().rename("count").to_frame()
        .join(pct.stack().rename("pct_of_EX_False"))
        .join(cnt["EX_False"])
        .reset_index()
    )
    OUT.mkdir(parents=True, exist_ok=True)
    long.to_csv(OUT / "taksonomia_bledow_k5.csv", index=False)

    # Agregat po strategiach (dla porównania z dotychczasowym rys. 4, który był agregatem)
    agg = fail.groupby(["eng", "error_category"]).size().unstack(fill_value=0).reindex(columns=CATS, fill_value=0)
    agg_pct = agg.div(agg.sum(axis=1), axis=0) * 100.0

    # Struktura komunikatów dla COLUMN_HALLUCINATION
    ch = fail[fail["error_category"] == "COLUMN_HALLUCINATION"].copy()
    msg = ch["error_message"].fillna("")
    ch["typ"] = "inna kolumna"
    ch.loc[msg.str.contains('column "total_count" does not exist', regex=False), "typ"] = "alias total_count w HAVING"
    ch.loc[msg.str.contains("column b.base_id does not exist", regex=False), "typ"] = "b.base_id (CTE)"
    ch.loc[msg.str.contains(r"\.fk_", regex=True), "typ"] = "kolumna FK fk_*"
    typ = ch.groupby(["eng", "s", "typ"]).size().unstack(fill_value=0)
    typ.to_csv(OUT / "bledy_kolumn_komunikaty.csv")

    pd.set_option("display.width", 220)
    print("Liczby bezwzględne (k = 5, EX = False):\n", cnt, "\n")
    print("Odsetek błędów [% EX = False]:\n", pct.round(1), "\n")
    print("Agregat 5 strategii [% EX = False]:\n", agg_pct.round(1), "\n", agg, "\n")
    print("COLUMN_HALLUCINATION wg komunikatu:\n", typ)


if __name__ == "__main__":
    main()
