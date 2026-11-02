#!/usr/bin/env python3
"""
Etap A, korekta A5: charakterystyka zbioru ewaluacyjnego Scenariusza A.

1. Rozkład liczby tabel w zapytaniach wzorcowych (|GT|):
   - z szablonu generatora (bench_schema_quality.py, SyntheticQueryBenchmarkGenerator,
     l. 410-636: proste 2/3, średnie 3/4, złożone 4/5/6 tabel naprzemiennie),
   - weryfikacja z danymi surowymi: |GT| = |T| * precision / recall dla każdego wiersza baseline'u
     z recall > 0 (|T| = k dla B/C/D, |T| = 15 dla Baseline A).
2. Liczba zapytań, w których ekspansja grafowa dołączyła co najmniej jedną tabelę z GT
   (recall Graph@5 > recall RRF@5).
3. Przykładowe instancje szablonów pytań i zapytań wzorcowych na rzeczywistych ścieżkach grafu FK.
   Graf FK odtwarzany jest deterministycznie z testbed/init-scripts/generate_enterprise_schema.py
   (random.Random(42)). Samych 500 instancji nie zapisano w raw_results, a kolejność sąsiadów
   w DFS generatora zależy od iteracji po zbiorze (PYTHONHASHSEED), więc przykłady ilustrują
   szablony, ale nie muszą pokrywać się z ewaluowanymi instancjami.

Wynik: out/zbior_gt_rozklad.csv, out/zbior_przyklady.txt oraz wydruk.
"""
import os
import sys

import numpy as np
import pandas as pd

from common import OUT, ROOT, load_schema_quality, pivot


def template_gt_size(query_id: int) -> int:
    """Liczba tabel GT wynikająca z szablonu (100 zapytań na domenę: 30 prostych, 50 średnich, 20 złożonych)."""
    i = (query_id - 1) % 100
    if i < 30:
        return 2 if i % 2 == 0 else 3
    if i < 80:
        return 3 if (i - 30) % 2 == 0 else 4
    return 4 + ((i - 80) % 3)


def gt_size_from_data(df: pd.DataFrame) -> pd.Series:
    est = []
    for (s, k), g in df[df["eng"] == "A"].groupby(["s", "top_k"]):
        if s == "Graph":
            continue
        t = 15 if s == "A" else k
        g = g[g["table_recall"] > 0]
        est.append(pd.DataFrame({"q": g["query_id"], "n": (t * g["table_precision"] / g["table_recall"]).round()}))
    e = pd.concat(est)
    agg = e.groupby("q")["n"].agg(["min", "max"])
    assert (agg["min"] == agg["max"]).all(), "niespójne oszacowania |GT|"
    return agg["min"].astype(int)


def examples() -> str:
    sys.path.insert(0, str(ROOT / "testbed" / "init-scripts"))
    sys.path.insert(0, str(ROOT))
    from generate_enterprise_schema import generate_enterprise_schema_definitions  # noqa: E402
    from benchmarks.scenarios.bench_schema_quality import SyntheticQueryBenchmarkGenerator  # noqa: E402

    tables, relations, _ = generate_enterprise_schema_definitions()
    gen = SyntheticQueryBenchmarkGenerator(pool=None)
    for rel in relations:
        src, tgt = rel.source_table, rel.target_table
        gen.fks_by_pair[(src, tgt)] = (rel.source_column, rel.target_column)
        gen.fks_by_pair[(tgt, src)] = (rel.target_column, rel.source_column)
        gen.foreign_keys_pairs.add((src, tgt))
        gen.foreign_keys_pairs.add((tgt, src))
    for t in tables:
        gen.domain_tables.setdefault(t.domain, []).append(t.table_name)
    ds = gen.generate_1000_query_dataset(total_queries=500)
    # Po jednym przykładzie z każdej kategorii trudności z domeny finance
    picks = [ds[0], ds[30], ds[80]]
    lines = []
    for c in picks:
        lines.append(f"[{c.difficulty}, |GT| = {len(c.ground_truth_tables)}] tabele: {', '.join(c.ground_truth_tables)}")
        lines.append(f"Pytanie: {c.natural_language_query}")
        lines.append(f"SQL wzorcowy: {c.ground_truth_sql}")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    df = load_schema_quality()
    n_data = gt_size_from_data(df)
    n_tmpl = pd.Series({q: template_gt_size(q) for q in n_data.index})
    assert (n_data == n_tmpl).all(), "rozkład |GT| z danych niezgodny z szablonem"

    dist = n_data.value_counts().sort_index().rename("liczba_zapytan").to_frame()
    dist["odsetek_%"] = dist["liczba_zapytan"] / len(n_data) * 100
    diff = pd.Series({q: template_gt_size(q) for q in range(1, 501)}).groupby(
        lambda q: "simple" if (q - 1) % 100 < 30 else ("medium" if (q - 1) % 100 < 80 else "complex")
    ).size()

    r5 = pivot(df, "A", 5, "table_recall")
    gained = int((r5["Graph"] > r5["RRF"]).sum())

    OUT.mkdir(parents=True, exist_ok=True)
    dist.to_csv(OUT / "zbior_gt_rozklad.csv")
    ex = examples()
    (OUT / "zbior_przyklady.txt").write_text(ex, encoding="utf-8")

    print("Zapytań z oszacowanym |GT| (dane):", len(n_data), "- zgodność z szablonem: 100%")
    print(dist.round(1))
    print("Zapytania z >= 3 tabelami:", int((n_data >= 3).sum()), f"({(n_data >= 3).mean() * 100:.1f}%)")
    print("Średnia |GT|:", round(n_data.mean(), 3))
    print("Trudność (szablon):", diff.to_dict())
    print(f"Ekspansja dołączyła >= 1 tabelę GT (recall Graph@5 > RRF@5): {gained} / 500 ({gained / 5:.1f}%)")
    print("\nPrzykłady instancji szablonów (ilustracja):\n" + ex)


if __name__ == "__main__":
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    main()
