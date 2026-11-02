#!/usr/bin/env python3
"""
Etap A, Krok 0: rozpoznanie danych surowych Scenariusza A (bez zmian w tekście pracy).

Sprawdza:
  (1) jakie warianty selekcji schematu ewaluowano i jaka jest objętość promptu każdego z nich,
  (2) czy wyniki LLM (SQL, Valid SQL, EX, kategoria błędu) dla k = 3, 10, 20 są kopiami k = 5,
  (3) spójność ablacji M_exp z wynikami głównymi (recall i tokeny),
  (4) pochodzenie danych do rys. 10 (llm_inference_latencies.csv): porównanie z deterministycznym
      generatorem np.random w benchmarks/plot_generators/generate_publication_plots.py (l. 1121-1131),
  (5) liczbę odrzuceń "vacuous truth" (zapytanie GT bez wierszy) jako wskaźnik obecności danych w tabelach,
  (6) liczbę tabel w kontekście wariantu autorskiego |T_final| = k + liczba mostków.

Wynik: out/krok0.txt oraz wydruk.
"""
import io
from contextlib import redirect_stdout

import numpy as np
import pandas as pd

from common import OUT, RAW, load_schema_quality


def main() -> None:
    df = load_schema_quality()
    buf = io.StringIO()
    with redirect_stdout(buf):
        print("=== (1) Warianty i liczności ===")
        print(df.groupby(["eng", "s", "top_k"]).size().unstack())
        print("\nŚrednia liczba tokenów promptu (prompt_tokens == ddl_tokens: instrukcja + DDL + pytanie):")
        print((df["prompt_tokens"] == df["ddl_tokens"]).all())
        print(df[df["eng"] == "A"].groupby(["s", "top_k"])["prompt_tokens"].mean().unstack().round(1))
        print("\nMin/max tokenów promptu Baseline A:", df[df["s"] == "A"]["prompt_tokens"].agg(["min", "max"]).to_dict())
        sc = pd.read_csv(RAW / "schema_scaling_analysis.csv")
        print("\nschema_scaling_analysis.csv (pełny zrzut DDL, benchmark_schema.py):\n", sc.to_string(index=False))
        g5 = df[(df["eng"] == "A") & (df["s"] == "Graph") & (df["top_k"] == 5)]["prompt_tokens"].mean()
        full = int(sc.loc[sc["table_count"] == 1000, "baseline_tokens"].iloc[0])
        print(f"Redukcja względem pełnego zrzutu: 1 - {g5:.1f}/{full} = {100 * (1 - g5 / full):.2f}%")

        print("\n=== (2) Czy wyniki LLM dla k != 5 są kopiami k = 5? ===")
        for col in ["generated_sql", "valid_sql", "ex_match", "error_category", "latency_ms"]:
            same = []
            for (e, s), g in df.groupby(["eng", "s"]):
                p = g.pivot(index="query_id", columns="top_k", values=col)
                same.append(all((p[k].fillna("NA") == p[5].fillna("NA")).all() for k in [3, 10, 20]))
            print(f"  {col}: identyczne we wszystkich 10 grupach = {all(same)}")

        print("\n=== (3) Ablacja M_exp vs wyniki główne ===")
        ab = pd.read_csv(RAW / "ablation_m_exp_results.csv")
        print(ab.to_string(index=False))
        rrf5 = df[(df["eng"] == "A") & (df["s"] == "RRF") & (df["top_k"] == 5)]
        print(f"  RRF@5 recall = {rrf5['table_recall'].mean():.4f} (ablacja M_exp=0: {ab.loc[ab.m_exp == 0, 'table_recall_at_5'].iloc[0]})")
        gr5 = df[(df["eng"] == "A") & (df["s"] == "Graph") & (df["top_k"] == 5)]
        print(f"  Graph@5 recall = {gr5['table_recall'].mean():.4f} (ablacja M_exp=3: {ab.loc[ab.m_exp == 3, 'table_recall_at_5'].iloc[0]})")
        print(f"  Graph@5 prompt_tokens = {gr5['prompt_tokens'].mean():.1f}; ablacja M_exp=3 avg_ddl_tokens (sam blok DDL) = "
              f"{ab.loc[ab.m_exp == 3, 'avg_ddl_tokens'].iloc[0]}; różnica = instrukcja + pytanie")
        print("  Uwaga: ablacja liczy count_tokens(ddl) bez instrukcji i pytania (bench_schema_quality.py l. 1364),"
              " a tabela 7 liczy cały prompt (l. 883, 1217); baseline'y B/C/D używają _format_ddl_for_tables"
              " (do 10 kolumn typu TEXT), wariant autorski _format_compact_enterprise_ddl (opisy, wszystkie kolumny, typy, FK).")

        print("\n=== (4) Pochodzenie llm_inference_latencies.csv (rys. 10) ===")
        lat = pd.read_csv(RAW / "llm_inference_latencies.csv")
        m = lat[lat["approach"].str.startswith("MCP")].reset_index(drop=True)
        b = lat[~lat["approach"].str.startswith("MCP")].reset_index(drop=True)
        np.random.seed(2026)
        n = 100
        sim = {
            "ttft_mcp": (np.clip(np.random.normal(34.2, 3.6, n), 25.0, 50.0), m["ttft_ms"]),
            "le2e_mcp": (np.clip(np.random.normal(345.0, 22.8, n), 280.0, 420.0), m["e2e_latency_ms"]),
            "ttft_base": (np.clip(np.random.normal(1380.0, 85.0, n), 1150.0, 1650.0), b["ttft_ms"]),
            "le2e_base": (np.clip(np.random.normal(2420.0, 120.0, n), 2100.0, 2800.0), b["e2e_latency_ms"]),
        }
        for k, (x, y) in sim.items():
            print(f"  {k}: max |plik - np.random(seed=2026)| = {np.abs(np.round(x, 3) - y.to_numpy()).max():.6f}")
        print("  Mediany w pliku: TTFT", m["ttft_ms"].median(), "vs", b["ttft_ms"].median(),
              "; L_E2E", m["e2e_latency_ms"].median(), "vs", b["e2e_latency_ms"].median())

        print("\n=== (5) Odrzucenia 'vacuous truth' (GT bez wierszy) ===")
        print("  ", int(df["error_message"].fillna("").str.contains("VACUOUS_TRUTH").sum()), "z", len(df))
        print("   EX = True (k = 5, oba profile, wszystkie strategie):", int(df[df.top_k == 5]["ex_match"].sum()))
        rows = df[df.top_k == 5]["error_message"].fillna("").str.extract(r"Ground Truth returned (\d+) rows")[0].dropna().astype(int)
        print(f"   Liczba wierszy GT w komunikatach SEMANTIC_MISMATCH (k = 5): n = {len(rows)}, min = {rows.min()}, "
              f"mediana = {rows.median():.0f}, max = {rows.max()} (zapytania GT mają LIMIT 50)")

        print("\n=== (5b) Tokeny promptu przy k = 5 a okno kontekstu (num_ctx nie ustawiono) ===")
        d5 = df[(df["eng"] == "A") & (df["top_k"] == 5)]
        print(d5.groupby("s")["prompt_tokens"].agg(["mean", "max"]).round(1))
        for t in (2048, 4096):
            print(f"   prompty > {t} tokenów:", d5.groupby("s")["prompt_tokens"].apply(lambda x: int((x > t).sum())).to_dict())

        print("\n=== (5c) Czas wywołania LLM latency_ms przy k = 5 [ms] (jedyny rzeczywisty pomiar czasu w Scenariuszu A) ===")
        print(df[df.top_k == 5].groupby(["eng", "s"])["latency_ms"].median().round(1))

        print("\n=== (6) |T_final| wariantu autorskiego ===")
        g = df[(df["eng"] == "A") & (df["s"] == "Graph")]
        print((g["top_k"] + g["bridge_tables_found"]).groupby(g["top_k"]).describe().round(3))

    text = buf.getvalue()
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "krok0.txt").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
