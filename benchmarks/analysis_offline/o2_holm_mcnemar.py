"""
[O2] Korekta Holma dla rodziny dokładnych testów McNemara EX (Graph@5 wobec strategii bazowych), osobno dla profilu.
Wejście: statistical_hypothesis_tests.csv (wiersze EX[...]). Wyjście: derived/o2_holm_mcnemar.json
"""
import pandas as pd

from wspolne import RUN, record, write_json

t = pd.read_csv(RUN / "statistical_hypothesis_tests.csv")
ex = t[t.test_id.astype(str).str.startswith("EX[")].copy()
out = {}
for prof, d in ex.groupby("profile"):
    d = d.sort_values("p_value").reset_index(drop=True)
    m = len(d)
    adj, running = [], 0.0
    for i, p in enumerate(d.p_value):
        running = max(running, min(1.0, (m - i) * p))
        adj.append(running)
    d["p_holm"] = adj
    out[prof] = [{"test": r.test_id, "ex_graph_pct": 100 * r.ex_a, "ex_base_pct": 100 * r.ex_b, "only_graph": int(r.only_a),
                  "only_base": int(r.only_b), "p_raw": r.p_value, "p_holm": r.p_holm,
                  "direction": "Graph lepszy" if r.only_a > r.only_b else ("baseline lepszy" if r.only_b > r.only_a else "remis"),
                  "significant_holm_0_05": bool(r.p_holm < 0.05)} for r in d.itertuples()]
o = write_json("o2_holm_mcnemar.json", {"family_size_per_profile": {k: len(v) for k, v in out.items()}, "tests": out})
record(__file__, [RUN / "statistical_hypothesis_tests.csv"], [o])
for p, rows in out.items():
    for r in rows:
        print(p, r["test"], r["only_graph"], r["only_base"], f"{r['p_raw']:.3g}", f"{r['p_holm']:.3g}", r["direction"], r["significant_holm_0_05"])
