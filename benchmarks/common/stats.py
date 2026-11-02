"""
Testy statystyczne wykonywane wyłącznie na danych z bieżącego przebiegu.

Jedyne dozwolone losowanie w ścieżce analizy to bootstrap z ziarnem pochodzącym z konfiguracji
(seeds.bootstrap); lista dozwolonych wyjątków jest sprawdzana przez tests/test_no_synthetic_data.py.
"""
from __future__ import annotations

from typing import Any, Dict, Sequence

import numpy as np
from scipy import stats as sps


def mann_whitney(a: Sequence[float], b: Sequence[float]) -> Dict[str, Any]:
    """Dwustronny test Manna–Whitneya U z korelacją rangowo-biserialną r = 1 - 2U/(n1*n2)."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    if len(a) < 2 or len(b) < 2:
        return {"test": "mann_whitney_u", "n_a": int(len(a)), "n_b": int(len(b)), "reported": False,
                "reason": "za mało obserwacji"}
    res = sps.mannwhitneyu(a, b, alternative="two-sided")
    u = float(res.statistic)
    return {"test": "mann_whitney_u", "n_a": int(len(a)), "n_b": int(len(b)), "U": u, "p_value": float(res.pvalue),
            "rank_biserial_r": 1.0 - 2.0 * u / (len(a) * len(b)), "median_a": float(np.median(a)),
            "median_b": float(np.median(b)), "reported": True}


def bootstrap_median_diff_ci(a: Sequence[float], b: Sequence[float], seed: int, n_resamples: int = 5000,
                             level: float = 0.95) -> Dict[str, Any]:
    """Percentylowy bootstrapowy przedział ufności różnicy median (a - b), próby niezależne."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    if len(a) < 2 or len(b) < 2:
        return {"reported": False, "reason": "za mało obserwacji"}
    rng = np.random.default_rng(seed)
    ia = rng.integers(0, len(a), size=(n_resamples, len(a)))
    ib = rng.integers(0, len(b), size=(n_resamples, len(b)))
    diffs = np.median(a[ia], axis=1) - np.median(b[ib], axis=1)
    lo, hi = np.percentile(diffs, [(1 - level) / 2 * 100, (1 + level) / 2 * 100])
    return {"reported": True, "estimate": float(np.median(a) - np.median(b)), "ci_low": float(lo), "ci_high": float(hi),
            "level": level, "n_resamples": n_resamples, "seed": seed}


def bootstrap_mean_ci(x: Sequence[float], seed: int, n_resamples: int = 10000, level: float = 0.95) -> Dict[str, Any]:
    x = np.asarray(x, dtype=float)
    if len(x) < 2:
        return {"reported": False, "reason": "za mało obserwacji"}
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(x), size=(n_resamples, len(x)))
    means = x[idx].mean(axis=1)
    lo, hi = np.percentile(means, [(1 - level) / 2 * 100, (1 + level) / 2 * 100])
    return {"reported": True, "mean": float(x.mean()), "ci_low": float(lo), "ci_high": float(hi),
            "n": int(len(x)), "n_resamples": n_resamples, "seed": seed}


def wilcoxon_paired(a: Sequence[float], b: Sequence[float]) -> Dict[str, Any]:
    """Dwustronny test Wilcoxona dla par (zero_method='wilcox'), r = (W+ - W-)/(W+ + W-)."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    d = a - b
    nz = d[d != 0]
    if len(nz) < 1:
        return {"test": "wilcoxon_signed_rank", "N": int(len(d)), "n_eff": 0, "reported": False, "reason": "brak niezerowych różnic"}
    ranks = sps.rankdata(np.abs(nz))
    wp, wm = float(ranks[nz > 0].sum()), float(ranks[nz < 0].sum())
    res = sps.wilcoxon(a, b, zero_method="wilcox", alternative="two-sided")
    return {"test": "wilcoxon_signed_rank", "N": int(len(d)), "n_eff": int(len(nz)), "W": float(res.statistic),
            "p_value": float(res.pvalue), "rank_biserial_r": (wp - wm) / (wp + wm), "reported": True}


def mcnemar_exact(a: Sequence[bool], b: Sequence[bool]) -> Dict[str, Any]:
    """Dokładny test McNemara (dwumianowy na parach niezgodnych)."""
    a = np.asarray(a, dtype=bool)
    b = np.asarray(b, dtype=bool)
    n01 = int((a & ~b).sum())
    n10 = int((~a & b).sum())
    p = float(sps.binomtest(n01, n01 + n10, 0.5).pvalue) if n01 + n10 else 1.0
    return {"test": "mcnemar_exact", "N": int(len(a)), "only_a": n01, "only_b": n10, "p_value": p, "reported": True}
