"""Wspólne funkcje ładowania danych surowych dla analiz Etapu A (bez ponownego uruchamiania eksperymentów)."""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "benchmarks" / "raw_results"
OUT = Path(__file__).resolve().parent / "out"

SHORT = {
    "Baseline A (Full Schema)": "A",
    "Baseline B (Lexical BM25)": "BM25",
    "Baseline C (Hybrid pgvector+BM25 RRF)": "RRF",
    "Baseline D (Dense-only pgvector k-NN)": "Dense",
    "Autorskie (Graph Pruning & Bridges)": "Graph",
}
ENGINE = {"Profil A (Constrained CPU)": "A", "Profil B (Unconstrained Strong)": "B"}


def load_schema_quality() -> pd.DataFrame:
    """Wczytuje per-zapytaniowe wyniki Scenariusza A (20 000 wierszy: 2 profile x 5 strategii x 4 k x 500)."""
    df = pd.read_parquet(RAW / "schema_quality_results.parquet")
    df["s"] = df["approach"].map(SHORT)
    df["eng"] = df["engine_name"].map(ENGINE)
    return df


def pivot(df: pd.DataFrame, eng: str, k: int, col: str) -> pd.DataFrame:
    """Tabela query_id x strategia dla danego profilu i k."""
    sub = df[(df["eng"] == eng) & (df["top_k"] == k)]
    return sub.pivot(index="query_id", columns="s", values=col).sort_index()
