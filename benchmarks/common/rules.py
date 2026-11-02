"""Reguły analityczne zapisywane w kodzie i w manifeście (używane przez scenariusze, analizę i wykresy)."""
from __future__ import annotations

from typing import Dict, Sequence

SWEET_SPOT_RULE = (
    "Sweet spot M_exp = najmniejsza badana wartość M_exp, dla której wzrost Table-Recall@5 przy przejściu "
    "do następnej badanej wartości M_exp jest mniejszy niż próg (threshold_pp) punktów procentowych; "
    "jeśli taki punkt nie istnieje, wybierana jest największa badana wartość."
)


def sweet_spot(m_values: Sequence[int], recall_pct: Sequence[float], threshold_pp: float) -> Dict[str, object]:
    """Wyznacza sweet spot zgodnie z SWEET_SPOT_RULE; recall_pct w punktach procentowych."""
    pairs = sorted(zip(m_values, recall_pct))
    for (m, r), (_, r_next) in zip(pairs, pairs[1:]):
        if (r_next - r) < threshold_pp:
            return {"m_exp": int(m), "recall_pct": float(r), "gain_to_next_pp": float(r_next - r),
                    "threshold_pp": threshold_pp, "rule": SWEET_SPOT_RULE}
    m, r = pairs[-1]
    return {"m_exp": int(m), "recall_pct": float(r), "gain_to_next_pp": None, "threshold_pp": threshold_pp, "rule": SWEET_SPOT_RULE}
