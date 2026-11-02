"""
[O9] Eksport próby rekordów z EX = fałsz wariantu autorskiego (Graph, k=5) do ręcznej inspekcji:
po 20 na profil, losowanie bez zwracania z ziarnem 2026 (seeds.query_generator z manifestu).
Wyjście: derived/o9_proba_bledow.csv (pytanie, odpowiedź modelu, wyekstrahowany SQL, błąd PostgreSQL, kategoria,
tabele GT i tabele w wycinku, liczby wierszy). Klasyfikacja ręczna: derived/o9_klasyfikacja_reczna.csv.
"""
import json

import pandas as pd

from wspolne import RUN, record

SEED = json.loads((RUN / "manifest.json").read_text(encoding="utf-8"))["seeds"]["query_generator"]
g = pd.read_parquet(RUN / "schema_quality_generation.parquet")
f = g[(g.strategy == "Graph") & (g.k == 5) & (~g.ex_match)]
sample = pd.concat([f[f.profile == p].sample(20, random_state=SEED) for p in ("small", "large")])
cols = ["profile", "query_id", "difficulty", "question", "gt_sql", "gt_tables", "retrieved_tables", "raw_response",
        "generated_sql", "exec_error", "error_category", "valid_sql", "n_rows_pred", "n_rows_gt", "done_reason", "eval_count"]
out = RUN / "derived" / "o9_proba_bledow.csv"
sample[cols].to_csv(out, index=False)
record(__file__, [RUN / "schema_quality_generation.parquet", RUN / "manifest.json"], [out])
print(sample.groupby(["profile", "error_category"]).size())
