"""
[O4] Kontrola zbioru Scenariusza A bez LLM i bez bazy.

1. Identifier-Match: wybór tabel katalogu, których identyfikatory występują dosłownie w treści pytania
   (granice identyfikatora: znaki spoza [a-z0-9_]). Table-Recall, Table-Precision, odsetek pełnej czułości.
2. Ekspansja grafowa nałożona na BM25@5 (Graph-on-BM25): algorytm z SchemaEngine.discover_schema_with_graph_pruning
   (pary kandydatów, nx.shortest_path w grafie nieskierowanym, węzeł pośredni ścieżek długości 2, M_exp=3,
   Counter.most_common) zastosowany do rankingu BM25@5 z rundy. Replikację weryfikuje się na RRF@5: wynik musi
   odtworzyć zbiory tabel Graph@5 zapisane w rundzie.
Katalog tabel i graf kluczy obcych: deterministyczny generator testbed/init-scripts/generate_enterprise_schema.py
(Random(42)), bez bazy. EX nie jest liczone (wymaga LLM). Wyjście: derived/o4_identifier_match.json
"""
import importlib.util
import json
import re
from collections import Counter

import networkx as nx
import pandas as pd

from wspolne import ROOT, RUN, record, write_json

gen_path = ROOT / "testbed" / "init-scripts" / "generate_enterprise_schema.py"
spec = importlib.util.spec_from_file_location("gen_schema", gen_path)
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)
tables, relations, _ = gen.generate_enterprise_schema_definitions()
names = [t.table_name if hasattr(t, "table_name") else t.name for t in tables]
G = nx.DiGraph()
G.add_nodes_from(names)
for rel in relations:
    G.add_edge(rel.source_table, rel.target_table)
UG = G.to_undirected(as_view=True)

ds = [json.loads(l) for l in (RUN / "schema_quality_dataset.jsonl").open(encoding="utf-8")]
pat = {n: re.compile(r"(?<![a-z0-9_])" + re.escape(n) + r"(?![a-z0-9_])") for n in names}


def scores(sel, gt):
    sel, gt = set(sel), set(gt)
    rec = len(sel & gt) / len(gt)
    prec = len(sel & gt) / len(sel) if sel else 0.0
    return rec, prec


im = []
for q in ds:
    sel = [n for n in names if pat[n].search(q["natural_language_query"])]
    rec, prec = scores(sel, q["ground_truth_tables"])
    im.append({"query_id": q["query_id"], "n_sel": len(sel), "recall": rec, "precision": prec, "full": rec == 1.0,
               "n_gt": len(q["ground_truth_tables"])})
im = pd.DataFrame(im)


def expand(base, m_exp=3):
    cnt = Counter()
    for i in range(len(base)):
        for j in range(i + 1, len(base)):
            u, v = base[i], base[j]
            if u not in UG or v not in UG:
                continue
            try:
                p = nx.shortest_path(UG, source=u, target=v)
            except nx.NetworkXNoPath:
                continue
            if len(p) == 3 and p[1] not in base:
                cnt[p[1]] += 1
    out = list(base)
    for b, _ in cnt.most_common(m_exp):
        if b not in out:
            out.append(b)
    return out


r = pd.read_parquet(RUN / "schema_retrieval.parquet")
r["tables"] = r.retrieved_tables.map(json.loads)
r["gt"] = r.gt_tables.map(json.loads)
rrf5 = r[(r.strategy == "RRF") & (r.k == 5)].set_index("query_id")
g5 = r[(r.strategy == "Graph") & (r.k == 5)].set_index("query_id")
bm5 = r[(r.strategy == "BM25") & (r.k == 5)].set_index("query_id")
replica_ok = sum(set(expand(rrf5.loc[q, "tables"])) == set(g5.loc[q, "tables"]) for q in rrf5.index)
gob = []
for q in bm5.index:
    sel = expand(bm5.loc[q, "tables"])
    rec, prec = scores(sel, bm5.loc[q, "gt"])
    gob.append({"query_id": int(q), "n_sel": len(sel), "recall": rec, "precision": prec, "full": rec == 1.0})
gob = pd.DataFrame(gob)
res = {
    "catalog": {"tables": len(names), "fk_edges": G.number_of_edges()},
    "identifier_match": {"n": len(im), "recall_pct": 100 * im.recall.mean(), "precision_pct": 100 * im.precision.mean(),
                         "full_recall_pct": 100 * im.full.mean(), "full_recall_n": int(im.full.sum()),
                         "mean_n_tables": im.n_sel.mean(), "max_n_tables": int(im.n_sel.max()),
                         "by_gt_tables": {int(k): {"n": len(d), "recall_pct": 100 * d.recall.mean(), "full_pct": 100 * d.full.mean()}
                                          for k, d in im.groupby("n_gt")}},
    "graph_on_bm25": {"replication_check_rrf5_to_graph5": {"matched": int(replica_ok), "of": len(rrf5)},
                      "n": len(gob), "recall_pct": 100 * gob.recall.mean(), "precision_pct": 100 * gob.precision.mean(),
                      "full_recall_pct": 100 * gob.full.mean(), "mean_n_tables": gob.n_sel.mean()},
}
o = write_json("o4_identifier_match.json", res)
record(__file__, [gen_path, RUN / "schema_quality_dataset.jsonl", RUN / "schema_retrieval.parquet"], [o])
print(json.dumps(res, indent=1, default=float))
