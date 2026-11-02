"""
[O3] Table-Recall wobec średniej liczby tokenów DDL dla wszystkich strategii i wartości k (porównanie przy równej
liczbie tokenów). Wyznacza front Pareto (maks. czułość, min. tokeny) oraz interpolowaną liniowo czułość
każdej strategii przy budżecie tokenów wariantu autorskiego k=5 (tylko w zakresie pomiarów strategii).
Wejście: schema_retrieval.parquet. Wyjście: derived/o3_pareto.json
"""
import numpy as np
import pandas as pd

from wspolne import RUN, record, write_json

r = pd.read_parquet(RUN / "schema_retrieval.parquet")
pts = (r.groupby(["strategy", "k"]).agg(recall=("table_recall", "mean"), tokens=("ddl_tokens", "mean"), n_tables=("n_tables", "mean"))
       .reset_index())
pts["recall"] *= 100
rows = pts.sort_values("tokens").to_dict("records")
front, best = [], -1
for p in rows:
    if p["recall"] > best:
        front.append(f"{p['strategy']}@{p['k']}"); best = p["recall"]
ref = pts[(pts.strategy == "Graph") & (pts.k == 5)].iloc[0]
interp = {}
for s, d in pts.groupby("strategy"):
    d = d.sort_values("tokens")
    if s == "DomainScoped" or len(d) < 2:
        continue
    if d.tokens.min() <= ref.tokens <= d.tokens.max():
        interp[s] = float(np.interp(ref.tokens, d.tokens, d.recall))
o = write_json("o3_pareto.json", {"points": rows, "pareto_front": front,
                                    "graph5": {"tokens": ref.tokens, "recall": ref.recall},
                                    "recall_at_graph5_tokens_interpolated": interp})
record(__file__, [RUN / "schema_retrieval.parquet"], [o])
for p in rows:
    print(f"{p['strategy']:12s} k={p['k']:2d} tokens={p['tokens']:7.1f} recall={p['recall']:5.1f} tables={p['n_tables']:.2f}")
print("front:", front); print("interp at", round(ref.tokens, 1), {k: round(v, 1) for k, v in interp.items()})
