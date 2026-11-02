"""
[O9] Ręczna klasyfikacja próby 40 rekordów (derived/o9_proba_bledow.csv) oraz zliczenia wspierające na całym zbiorze.

Przyczyny ustalone przy przeglądzie (opis w REVIEW_FIXES.md, sekcja 4):
  A  pytanie średnie: instrukcja 'HAVING total_count >= 1' (alias w HAVING jest w PostgreSQL niedozwolony);
     model ją wykonuje, więc zapytanie nie może się wykonać niezależnie od wycinka schematu,
  B  model używa tabel wymienionych w pytaniu, lecz nieobecnych w wycinku, i zgaduje kolumnę FK / kierunek złączenia,
  C  błędna kolumna FK lub kierunek złączenia przy wszystkich tabelach w wycinku,
  D  zapytanie CTE: model interpretuje alias 'b.' z treści pytania jako alias tabeli bazowej,
  E  Valid SQL, inny wynik (błędne złączenie lub agregacja).
Reguły przypisania odtwarzają decyzje z przeglądu ręcznego i są stosowane do wszystkich rekordów próby.
Wyjście: derived/o9_klasyfikacja_reczna.csv, derived/o9_podsumowanie.json
"""
import json
import re

import pandas as pd

from wspolne import RUN, record, write_json

d = pd.read_csv(RUN / "derived" / "o9_proba_bledow.csv")


def cause(r):
    sql = str(r.generated_sql)
    used = set(re.findall(r"\b(?:FROM|JOIN)\s+(?:public\.)?([a-z_0-9]+)", sql, re.I))
    outside = used - set(json.loads(r.retrieved_tables))
    causes = []
    if r.difficulty == "medium" and re.search(r"HAVING\s+total_count", sql, re.I):
        causes.append("A")
    if r.error_category == "SEMANTIC_MISMATCH":
        causes.append("E")
    elif r.difficulty == "complex" and "b.base_id" in str(r.exec_error):
        causes.append("D")
    elif outside:
        causes.append("B")
    else:
        causes.append("C")
    return "+".join(causes)


d["przyczyna"] = d.apply(cause, axis=1)
d["blad_postgresql"] = d.exec_error.fillna("").str.split("\n").str[0]
d[["profile", "query_id", "difficulty", "error_category", "przyczyna", "blad_postgresql"]].to_csv(
    RUN / "derived" / "o9_klasyfikacja_reczna.csv", index=False)

g = pd.read_parquet(RUN / "schema_quality_generation.parquet")
med = g[g.difficulty == "medium"]
summ = {
    "sample_causes": d.groupby(["profile", "przyczyna"]).size().unstack(fill_value=0).to_dict(),
    "sample_size": len(d),
    "ex_by_difficulty_pct": (100 * g.groupby("difficulty").ex_match.mean()).to_dict(),
    "medium_records": len(med), "medium_valid_sql": int(med.valid_sql.sum()), "medium_ex": int(med.ex_match.sum()),
    "medium_having_alias_pct": float(100 * med.generated_sql.str.contains(r"HAVING\s+total_count", case=False, regex=True).mean()),
    "medium_question_instructs_having_alias_pct": float(100 * med.question.str.contains("HAVING total_count", regex=False).mean()),
    "fenced_code_pct_by_profile": (100 * g.raw_response.str.contains("```", regex=False).groupby(g.profile).mean()).to_dict(),
    "done_reason": g.done_reason.value_counts().to_dict(),
    "table_missing_pg_error_column_pct": float(100 * g[g.error_category == "TABLE_MISSING"].exec_error.fillna("")
                                               .str.contains("column", case=False).mean()),
}
for p in ("small", "large"):
    x = g[(g.profile == p) & (g.difficulty != "medium")]
    summ[f"ex_without_medium_{p}"] = {f"{s}@{k}": round(v, 2) for (s, k), v in (100 * x.groupby(["strategy", "k"]).ex_match.mean()).items()}
summ = json.loads(json.dumps(summ, default=str))
o = write_json("o9_podsumowanie.json", summ)
record(__file__, [RUN / "derived" / "o9_proba_bledow.csv", RUN / "schema_quality_generation.parquet"],
       [RUN / "derived" / "o9_klasyfikacja_reczna.csv", o])
print(json.dumps({k: v for k, v in summ.items() if not k.startswith("ex_without")}, indent=1))
print(summ["ex_without_medium_small"]); print(summ["ex_without_medium_large"])
