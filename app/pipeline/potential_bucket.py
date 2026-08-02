from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

#: The score at or above which a job is *accepted*. A product decision, not a tuning
#: artefact — which is why it is the one absolute `tests/unit/test_scoring_invariants.py`
#: is allowed to reference, and why that file now imports this name instead of declaring
#: its own copy of the number (CP1-6).
#:
#: It lives here rather than in `output.py` because `output.py` already imports this
#: module; the reverse would be a cycle. It is re-exported from `app.pipeline`, so cross-
#: package callers should use `from app.pipeline import ACCEPT_THRESHOLD`.
#:
#: Distinct from `settings.score_keep_threshold`, which also defaults to 70 but is
#: user-overridable via `JOBAGENT_SCORE_KEEP_THRESHOLD`. Whether those two should be one
#: number is a product question for CP-3; folding them together here would silently turn
#: a configurable knob into a constant.
ACCEPT_THRESHOLD = 70


@dataclass(frozen=True)
class PotentialDecision:
    is_potential: bool
    final_score: float | None
    llm_score: float | None
    reason: str


def _as_float(x: Any) -> float | None:
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
    final_cutoff: float = ACCEPT_THRESHOLD,
    llm_cutoff: float = ACCEPT_THRESHOLD,
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
        return PotentialDecision(
            True, final_score, llm_score, f"final<{final_cutoff} and llm>{llm_cutoff}"
        )
    return PotentialDecision(False, final_score, llm_score, "not potential")
