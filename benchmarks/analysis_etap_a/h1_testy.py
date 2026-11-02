#!/usr/bin/env python3
"""
Etap A, korekta A4: testy statystyczne hipotezy H1 na istniejących danych per zapytanie.

Źródło: benchmarks/raw_results/schema_quality_results.parquet (bez ponownego uruchamiania eksperymentów).

1. Test konstrukcyjny (dotychczasowy): Graph@5 vs RRF@5. T_final = T_cand(RRF@5) ∪ B_sel, więc
   recall Graph@5 >= recall RRF@5 dla każdego zapytania (W = 0 jest przesądzone).
2. Nowy test główny H1: Graph@5 (|T_final| = 5..8 tabel) vs baseline'y przy k = 10 (10 tabel).
   W danych nie ma przebiegów baseline'ów dla k = 6..8, a rankingi są prefiksowe, więc
   recall@k' (k' = |T_final|) leży w przedziale [recall@5, recall@10]. Porównanie z k = 10 daje
   baseline'om co najmniej tyle tabel co wariantowi autorskiemu (porównanie konserwatywne).
   Dodatkowo Graph@3 (3..5 tabel) vs baseline'y przy k = 5.
3. McNemar (dokładny, dwumianowy) dla EX przy k = 5 (jedyne k z wywołaniami LLM; wiersze dla
   k = 3, 10, 20 są kopiami wyników k = 5), osobno dla profili A i B.

Wynik: out/h1_testy.csv oraz wydruk.
"""
import numpy as np
import pandas as pd
from scipy.stats import binomtest, rankdata, wilcoxon

from common import OUT, load_schema_quality, pivot


def wilcoxon_paired(x: pd.Series, y: pd.Series) -> dict:
    """Dwustronny test Wilcoxona (zero_method='wilcox') z korelacją rangowo-biserialną r = (W+ - W-)/(W+ + W-)."""
    d = (x - y).to_numpy()
    nz = d[d != 0]
    ranks = rankdata(np.abs(nz))
    w_plus = float(ranks[nz > 0].sum())
    w_minus = float(ranks[nz < 0].sum())
    res = wilcoxon(x, y, zero_method="wilcox", alternative="two-sided")
    return {
        "N": len(d),
        "n_eff": int(len(nz)),
        "wins_graph": int((nz > 0).sum()),
        "wins_baseline": int((nz < 0).sum()),
        "W": float(res.statistic),
        "W_plus": w_plus,
        "W_minus": w_minus,
        "p": float(res.pvalue),
        "r_rb": (w_plus - w_minus) / (w_plus + w_minus) if (w_plus + w_minus) > 0 else np.nan,
        "mean_a": float(x.mean()),
        "mean_b": float(y.mean()),
        "median_a": float(x.median()),
        "median_b": float(y.median()),
    }


def mcnemar_exact(a: pd.Series, b: pd.Series) -> dict:
    """Dokładny test McNemara: b01 = A poprawne / B błędne, b10 = A błędne / B poprawne."""
    a = a.astype(bool)
    b = b.astype(bool)
    n01 = int((a & ~b).sum())
    n10 = int((~a & b).sum())
    p = binomtest(n01, n01 + n10, 0.5).pvalue if (n01 + n10) > 0 else 1.0
    return {
        "N": len(a),
        "ex_a": float(a.mean()),
        "ex_b": float(b.mean()),
        "both_true": int((a & b).sum()),
        "only_graph": n01,
        "only_baseline": n10,
        "discordant": n01 + n10,
        "p": float(p),
    }


def main() -> None:
    df = load_schema_quality()
    rows = []

    # Metryki IR są identyczne w obu profilach (selekcja schematu nie zależy od LLM), używamy profilu A.
    for metric in ["table_recall", "column_recall"]:
        p5 = pivot(df, "A", 5, metric)
        p3 = pivot(df, "A", 3, metric)
        p10 = pivot(df, "A", 10, metric)
        comparisons = [
            ("konstrukcyjny", "Graph@5", p5["Graph"], "RRF@5", p5["RRF"]),
            ("konstrukcyjny", "Graph@5", p5["Graph"], "BM25@5", p5["BM25"]),
            ("rowny_budzet_tabel", "Graph@5", p5["Graph"], "RRF@10", p10["RRF"]),
            ("rowny_budzet_tabel", "Graph@5", p5["Graph"], "BM25@10", p10["BM25"]),
            ("rowny_budzet_tabel", "Graph@3", p3["Graph"], "RRF@5", p5["RRF"]),
            ("rowny_budzet_tabel", "Graph@3", p3["Graph"], "BM25@5", p5["BM25"]),
        ]
        for kind, na, xa, nb, xb in comparisons:
            res = wilcoxon_paired(xa, xb)
            rows.append({"test": "wilcoxon", "metric": metric, "kind": kind, "a": na, "b": nb, "profile": "A=B", **res})

    # Liczba tabel w kontekście wariantu autorskiego (|T_final| = k + liczba mostków).
    g = df[(df["eng"] == "A") & (df["s"] == "Graph")]
    tfinal = (g["top_k"] + g["bridge_tables_found"]).groupby(g["top_k"]).describe()

    for eng in ["A", "B"]:
        ex = pivot(df, eng, 5, "ex_match")
        for base in ["RRF", "BM25", "Dense", "A"]:
            res = mcnemar_exact(ex["Graph"], ex[base])
            rows.append({"test": "mcnemar_EX@5", "metric": "ex_match", "kind": "sparowane_EX", "a": "Graph@5", "b": f"{base}@5", "profile": eng, **res})

    out = pd.DataFrame(rows)
    OUT.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT / "h1_testy.csv", index=False)
    tfinal.to_csv(OUT / "graph_tfinal_po_k.csv")

    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 30)
    print("|T_final| wariantu autorskiego w funkcji k:\n", tfinal, "\n")
    w = out[out["test"] == "wilcoxon"]
    print(w[["metric", "kind", "a", "b", "N", "n_eff", "wins_graph", "wins_baseline", "W", "p", "r_rb", "mean_a", "mean_b", "median_a", "median_b"]].to_string(index=False))
    m = out[out["test"] != "wilcoxon"]
    print()
    print(m[["profile", "a", "b", "N", "ex_a", "ex_b", "both_true", "only_graph", "only_baseline", "discordant", "p"]].to_string(index=False))


if __name__ == "__main__":
    main()
