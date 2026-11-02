"""
[O6] Rozkład stopni wierzchołków grafu kluczy obcych schematu syntetycznego (deterministyczny generator
testbed/init-scripts/generate_enterprise_schema.py, Random(42); bez bazy). Liczba węzłów i krawędzi jest
porównywana z logiem rundy (logs/drift.log: 1000 węzłów, 1174 krawędzi). Wyjście: derived/o6_stopnie_grafu.json
i rysunek doc/rysunki/fig_stopnie_grafu.{pdf,png}.
"""
import importlib.util
import re
from collections import Counter

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import networkx as nx  # noqa: E402
import numpy as np  # noqa: E402

from wspolne import ROOT, RUN, record, write_json  # noqa: E402

gen_path = ROOT / "testbed" / "init-scripts" / "generate_enterprise_schema.py"
spec = importlib.util.spec_from_file_location("gen_schema", gen_path)
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)
tables, relations, _ = gen.generate_enterprise_schema_definitions()
G = nx.DiGraph()
G.add_nodes_from(t.table_name if hasattr(t, "table_name") else t.name for t in tables)
for rel in relations:
    G.add_edge(rel.source_table, rel.target_table)
log = (RUN / "logs" / "drift.log").read_text(encoding="utf-8")
m = re.search(r"Graf relacji NetworkX zainicjalizowany: (\d+) węzłów, (\d+) krawędzi", log)
deg = np.array([d for _, d in G.to_undirected().degree()])
hist = Counter(deg.tolist())
res = {"nodes": G.number_of_nodes(), "edges": G.number_of_edges(),
       "matches_round_log": bool(m and int(m.group(1)) == G.number_of_nodes() and int(m.group(2)) == G.number_of_edges()),
       "degree_mean": float(deg.mean()), "degree_median": float(np.median(deg)), "degree_max": int(deg.max()),
       "isolated": int((deg == 0).sum()), "deg_ge_15": int((deg >= 15).sum()), "deg_le_2_pct": float(100 * (deg <= 2).mean()),
       "histogram": {int(k): int(v) for k, v in sorted(hist.items())},
       "components": nx.number_connected_components(G.to_undirected())}
o = write_json("o6_stopnie_grafu.json", res)
plt.rcParams.update({"font.family": "serif", "font.size": 12})
fig, ax = plt.subplots(figsize=(8, 4.6))
ks = sorted(hist); ax.bar(ks, [hist[k] for k in ks], color="#004B87")
ax.set(yscale="log", xlabel="Stopień wierzchołka (liczba kluczy obcych tabeli, oba kierunki)", ylabel="Liczba tabel (skala log.)",
       title="Rozkład stopni grafu kluczy obcych schematu syntetycznego")
ax.grid(alpha=0.3)
figs = [ROOT / "doc" / "rysunki" / "fig_stopnie_grafu.pdf", ROOT / "doc" / "rysunki" / "fig_stopnie_grafu.png"]
fig.savefig(figs[0], bbox_inches="tight"); fig.savefig(figs[1], dpi=200, bbox_inches="tight")
record(__file__, [gen_path, RUN / "logs" / "drift.log"], [o] + figs)
print({k: v for k, v in res.items() if k != "histogram"}); print(res["histogram"])
