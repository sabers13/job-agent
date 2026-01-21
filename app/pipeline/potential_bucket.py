from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class PotentialDecision:
    is_potential: bool
    final_score: Optional[float]
    llm_score: Optional[float]
    reason: str


def _as_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _get_first(d: Mapping[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def decide_potential(
    job: Mapping[str, Any],
    *,
    final_cutoff: float = 70.0,
    llm_cutoff: float = 70.0,
) -> PotentialDecision:
    """
    Decide whether a job should be included in potential_applications/.

    The pipeline uses different key names across stages, so we accept multiple fallbacks:
      - final score: final_score / fit_score / score
      - llm score: llm_score / llm_fit_score / llm_final_score
    """
    final_raw = _get_first(job, "final_score", "fit_score", "score")
    llm_raw = _get_first(job, "llm_score", "llm_fit_score", "llm_final_score")

    final_score = _as_float(final_raw)
    llm_score = _as_float(llm_raw)

    if final_score is None:
        return PotentialDecision(False, final_score, llm_score, "missing final_score")
    if llm_score is None:
        return PotentialDecision(False, final_score, llm_score, "missing llm_score")

    if final_score < final_cutoff and llm_score > llm_cutoff:
        return PotentialDecision(True, final_score, llm_score, f"final<{final_cutoff} and llm>{llm_cutoff}")
    return PotentialDecision(False, final_score, llm_score, "not potential")
