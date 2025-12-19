from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any, Optional

from app.config.focus import DEFAULT_FOCUS
from app.config.settings import settings
from .llm_enrich import llm_score_job, LLM_SCORING_VERSION


@dataclass
class HeuristicComponentResult:
    name: str
    raw_score: float
    reasons: List[str]
    meta: Dict[str, Any]


@dataclass
class HeuristicWeights:
    base_score: float = 50.0
    max_bonus: float = 50.0
    max_malus: float = 50.0
    components: Dict[str, float] = None

    def __post_init__(self):
        if self.components is None:
            object.__setattr__(self, "components", {})


DEFAULT_HEURISTIC_WEIGHTS = HeuristicWeights(
    base_score=50.0,
    max_bonus=50.0,
    max_malus=50.0,
    components={
        "seniority": 1.0,
        "language": 1.0,
        "skills": 1.0,
        "location": 1.0,
        "employment_type": 1.0,
        "experience": 1.0,
    },
)

HEURISTIC_SCORING_VERSION = "2.0.0"
SCORING_VERSION = "2.1.0"

_LANG_PENALTY = {
    None: 0,
    "None": 0,
    "Unknown": 0,
    "A1": 0,
    "A2": 0,
    "B1": -20,
    "B2": -75,
    "C1": -85,
    "C2": -90,
    "Native": -90,
}

FALLBACK_VAGUE_PENALTY = -8

LANG_PATTERNS = [
    (r"\bdeutsch(?:kenntnisse)?\s*(?:auf\s*)?(?:niveau\s*)?(c2)\b", "C2", 0.95),
    (r"\bdeutsch(?:kenntnisse)?\s*(?:auf\s*)?(?:niveau\s*)?(c1)\b", "C1", 0.95),
    (r"\bdeutsch(?:kenntnisse)?\s*(?:auf\s*)?(?:niveau\s*)?(b2)\b", "B2", 0.95),
    (r"\bdeutsch(?:kenntnisse)?\s*(?:auf\s*)?(?:niveau\s*)?(b1)\b", "B1", 0.95),
    (r"\bmuttersprache(?:lich(?:e|er|es)?)?\b.*\bdeutsch\b", "C2", 0.95),
    (r"\bverhandlungssicher(?:e|er|es)?\b.*\bdeutsch\b", "C1", 0.9),
    (r"\bflie(?:ss|ß)end(?:e|er|es)?\b.*\bdeutsch\b", "C1", 0.7),
    (r"\bsehr\s+gute\s+deutschkenntnisse\b", "B2", 0.7),
    (r"\b(gute|sichere)\s+deutschkenntnisse\b", "B2", 0.6),
    (r"\bgrundkenntnisse\b.*\bdeutsch\b|\bbasiskenntnisse\b.*\bdeutsch\b", "A2", 0.5),
    (r"\bdeutschkenntnisse\b.*\b(von vorteil|w(ü|u)nschenswert)\b", "B1", 0.35),
    # --- English phrasing that implies German requirement ---
    (r"\bfluent\s+in\s+(?:both\s+)?english\s+and\s+german\b", "C1", 0.92),
    (r"\bfluent\s+in\s+german\b", "C1", 0.90),
    (r"\bfluent\s+german\b", "C1", 0.85),
    (r"\bnative\s+german\b", "C2", 0.95),
    (r"\bbusiness\s+fluent\s+german\b", "C1", 0.90),
    (r"\bfluent german\b", "C1", 0.8),
    (r"\bproficient german\b", "B2", 0.7),
    (r"\bgood german\b", "B2", 0.6),
    (r"\bbasic german\b", "A2", 0.5),
    (r"\b(in wort und schrift)\b", None, 0.2),
]

GERMAN_HEAVY_CONTEXT = re.compile(r"\b(kunde|kundenkontakt|beratung|berater|consultant|vertrieb|stakeholder|workshop)\b", re.IGNORECASE)
PUBLIC_SECTOR = re.compile(r"\b(behörde|amt|öffentliche(r|n)? dienst|verwaltung|klin(ik|ikum)|schule|schulen)\b", re.IGNORECASE)

_YEARS_PENALTY = [
    (5, -25),
    (4, -20),
    (3, -15),
]


_CEFR_RANK = {
    "A0": 0,
    "NONE": 0,
    "A1": 1,
    "A2": 2,
    "B1": 3,
    "B2": 4,
    "C1": 5,
    "C2": 6,
    "NATIVE": 7,
    "UNKNOWN": -1,
}


def _rank_cefr(level: str | None) -> int:
    if not level:
        return _CEFR_RANK["UNKNOWN"]
    key = str(level).strip().upper()
    return _CEFR_RANK.get(key, _CEFR_RANK["UNKNOWN"])


def _location_matches_focus(title: str, loc: str, focus) -> bool:
    hits = _contains_any(f"{title} {loc}", list(getattr(focus, "locations_any", []) or []))
    return bool(hits)


def classify_blockers(*, job: Dict[str, Any], focus, llm_part: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    hard: List[str] = []
    soft: List[str] = []

    title = (job.get("title") or "")
    loc = (job.get("location") or "")

    # --- Relocation hard blocker (profile-driven) ---
    relocation_ok = bool(getattr(focus, "relocation_ok", True))
    if not relocation_ok:
        if not _location_matches_focus(title, loc, focus):
            hard.append("relocation_required_but_not_ok")

    # --- Language hard blocker (LLM-driven, profile-driven) ---
    cand_level = str(getattr(focus, "candidate_german_level", "Unknown") or "Unknown").upper()
    strict_unknown = bool(getattr(focus, "strict_language_blocker", True))

    llm_gr = (llm_part or {}).get("german_requirement") or {}
    llm_type = str(llm_gr.get("type") or "").strip().lower()
    llm_min = str(llm_gr.get("min_level") or "Unknown").strip().upper()

    cand_rank = _rank_cefr(cand_level)
    min_rank = _rank_cefr(llm_min)

    if llm_type in ("hard_blocker", "required"):
        if min_rank >= 0:
            if cand_rank >= 0 and cand_rank < min_rank:
                hard.append("language_german_required_unmet")
            elif cand_rank < 0:
                # Candidate German unknown: treat as hard blocker when strict OR LLM explicitly says hard_blocker
                if strict_unknown or llm_type == "hard_blocker":
                    hard.append("language_german_unverified_for_requirement")

    # --- Carry through LLM critical blockers (default to soft unless you map them) ---
    llm_crit = (llm_part or {}).get("critical_blockers") or []
    for b in llm_crit:
        if not b:
            continue
        # keep stable but do not let arbitrary text become "hard" silently
        soft.append(f"llm:{str(b).strip()}")

    hard = sorted(set(hard))
    soft = sorted(set(soft))

    return {
        "hard": hard,
        "soft": soft,
        "llm_german_requirement": llm_gr or None,
    }


def apply_blocker_caps(*, score: float, focus, blockers: Dict[str, Any], enabled: bool) -> Dict[str, Any]:
    hard = blockers.get("hard") or []
    soft = blockers.get("soft") or []

    cap_applied = False
    cap_value = None
    cap_reason = None
    final = float(score)

    if not enabled:
        return {
            "score": final,
            "cap_applied": False,
            "cap_value": None,
            "cap_reason": None,
        }

    hard_cap = int(getattr(focus, "blocker_cap_hard", 35))
    soft_cap = int(getattr(focus, "blocker_cap_soft", 55))

    if hard:
        cap_value = hard_cap
        cap_reason = "hard_blockers:" + ",".join(hard)
    elif soft:
        cap_value = soft_cap
        cap_reason = "soft_blockers:" + ",".join(soft)

    if cap_value is not None:
        new_score = min(final, float(cap_value))
        cap_applied = new_score != final
        final = new_score

    return {
        "score": final,
        "cap_applied": cap_applied,
        "cap_value": cap_value,
        "cap_reason": cap_reason,
    }

def _contains_any(text: str, words: List[str]) -> List[str]:
    hits = []
    low = text.lower()
    for w in words:
        if w and w.lower() in low:
            hits.append(w)
    return hits

def _count_keywords(text: str, keywords: List[str]) -> Dict[str,int]:
    low = text.lower()
    counts = {}
    for k in keywords:
        if not k:
            continue
        parts = k.lower().split()
        if not parts:
            continue
        if len(parts) == 1:
            pattern = rf"\b{re.escape(parts[0])}\b"
        else:
            pattern = r"\b" + r"\s+".join(re.escape(part) for part in parts) + r"\b"
        c = len(re.findall(pattern, low))
        counts[k] = c
    return counts

def _clamp_confidence(value: Any) -> float:
    try:
        conf = float(value)
    except Exception:
        conf = 0.0
    return max(0.0, min(1.0, conf))

def _guess_post_language(text_lower: str) -> str:
    german_hits = len(re.findall(r"\b(und|der|die|das|nicht|ist|mit|für|den|des|auf|zu|vom|nach)\b", text_lower)) + len(re.findall(r"[äöüß]", text_lower))
    english_hits = len(re.findall(r"\b(the|and|with|for|not|is|are|will|from|into|of|in)\b", text_lower))
    if german_hits == 0 and english_hits == 0:
        return "Unknown"
    if german_hits > english_hits * 1.5:
        return "German"
    if english_hits > german_hits * 1.5:
        return "English"
    return "Mixed"

def _regex_guess_german(text_lower: str) -> Optional[Dict[str, Any]]:
    for pattern, level, conf in LANG_PATTERNS:
        m = re.search(pattern, text_lower)
        if m:
            snippet = m.group(0)
            return {
                "language": "German",
                "cefr_guess": (level or "Unknown").upper() if level else "Unknown",
                "confidence": conf,
                "evidence_phrases": [snippet[:160]],
                "customer_facing": bool(GERMAN_HEAVY_CONTEXT.search(text_lower)),
                "job_post_language": _guess_post_language(text_lower),
                "source": "regex",
            }

    post_lang = _guess_post_language(text_lower)
    if post_lang == "German":
        return {
            "language": "German",
            "cefr_guess": "Unknown",
            "confidence": 0.2,
            "evidence_phrases": ["posting_language:German"],
            "customer_facing": bool(GERMAN_HEAVY_CONTEXT.search(text_lower)),
            "job_post_language": post_lang,
            "source": "fallback",
        }
    return None

def _fallback_language_items(text_lower: str, english_hint: bool, english_evidence: Optional[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    german_guess = _regex_guess_german(text_lower)
    if german_guess:
        items.append(german_guess)
    if english_hint:
        items.append({
            "language": "English",
            "cefr_guess": "B2",
            "confidence": 0.7,
            "evidence_phrases": [english_evidence] if english_evidence else [],
            "customer_facing": False,
            "job_post_language": "Unknown",
        })
    return items


def resolve_language_items(
    *,
    text_lower: str,
    job: Dict[str, Any],
    lang_items: Optional[List[Dict[str, Any]]],
    english_hint: bool,
    llm_part: Optional[Dict[str, Any]],
    focus,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (normalized_lang_items, evidence_dict).

    Precedence:
      1) LLM german_requirement (if present & override enabled upstream)
      2) Regex inference from text
      3) Structured language_requirements entries (non-Unknown with reasonable confidence)
      4) Fallback (posting language German -> default)
    Ensures a single authoritative German entry.
    """
    items = [it for it in (lang_items or []) if isinstance(it, dict)]

    # Include legacy german_requirement field as a structured hint (if present)
    legacy_level = str(job.get("german_requirement") or "").strip().upper()
    if legacy_level and legacy_level != "UNKNOWN":
        items = [
            it
            for it in items
            if not (str(it.get("language", "")).lower().startswith("ger") and str(it.get("source", "")).lower() == "legacy_field")
        ]
        items.append(
            {
                "language": "German",
                "cefr_guess": legacy_level,
                "confidence": 0.6,
                "evidence_phrases": ["legacy german_requirement field"],
                "customer_facing": bool(GERMAN_HEAVY_CONTEXT.search(text_lower)),
                "job_post_language": _guess_post_language(text_lower),
                "source": "legacy_field",
            }
        )

    # ----- 1) LLM (if present) -----
    llm_gr = (llm_part or {}).get("german_requirement") or {}
    llm_level = str(llm_gr.get("min_level") or "").strip().upper()
    llm_type = str(llm_gr.get("type") or "").strip().lower()
    llm_just = str(llm_gr.get("justification") or "").strip()

    if llm_level and llm_level != "UNKNOWN":
        german = {
            "language": "German",
            "cefr_guess": llm_level,
            "confidence": float(getattr(settings, "llm_language_override_conf", 0.9)),
            "evidence_phrases": [("LLM: " + llm_just)[:160]] if llm_just else ["LLM override"],
            "customer_facing": True,
            "job_post_language": _guess_post_language(text_lower),
            "source": "llm",
            "llm_type": llm_type,
        }
        items = [it for it in items if not str(it.get("language", "")).lower().startswith("ger")]
        items.append(german)
        return items, {"source": "llm", "level": llm_level, "type": llm_type, "evidence": german["evidence_phrases"]}

    # ----- 2) Regex -----
    regex_guess = _regex_guess_german(text_lower)
    if regex_guess:
        items = [it for it in items if not str(it.get("language", "")).lower().startswith("ger")]
        items.append(regex_guess)
        return items, {"source": "regex", "level": regex_guess.get("cefr_guess"), "evidence": regex_guess.get("evidence_phrases")}

    # ----- 3) Structured (pick best German entry if any) -----
    german_candidates = []
    for it in items:
        if str(it.get("language", "")).lower().startswith("ger"):
            level = str(it.get("cefr_guess") or "Unknown").strip()
            conf = _clamp_confidence(it.get("confidence"))
            if level and level.lower() != "unknown" and conf >= 0.35:
                german_candidates.append((conf, it))

    if german_candidates:
        german_candidates.sort(key=lambda x: x[0], reverse=True)
        best = german_candidates[0][1]
        best["source"] = best.get("source") or "structured"
        items = [it for it in items if not str(it.get("language", "")).lower().startswith("ger")]
        items.append(best)
        return items, {"source": "structured", "level": best.get("cefr_guess"), "evidence": best.get("evidence_phrases")}

    # ----- 4) Fallback: posting is German -> default to focus.min_german_level (or B1) -----
    post_lang = _guess_post_language(text_lower)
    if post_lang == "German":
        default_lvl = str(getattr(focus, "min_german_level", "B1") or "B1").upper()
        fallback = {
            "language": "German",
            "cefr_guess": default_lvl,
            "confidence": 0.5,
            "evidence_phrases": [f"posting_language:German (default {default_lvl})"],
            "customer_facing": bool(GERMAN_HEAVY_CONTEXT.search(text_lower)),
            "job_post_language": post_lang,
            "source": "fallback",
        }
        items = [it for it in items if not str(it.get("language", "")).lower().startswith("ger")]
        items.append(fallback)
        return items, {"source": "fallback", "level": default_lvl, "evidence": fallback["evidence_phrases"]}

    return items, {"source": "none", "level": None, "evidence": []}

def _penalize_language(lang_items: List[Dict[str, Any]], english_hint: Optional[bool]) -> Dict[str, Any]:
    english_detected = bool(english_hint)
    english_bonus = 0
    german_penalty = 0
    reasons: List[str] = []
    german_entry: Optional[Dict[str, Any]] = None
    english_entry: Optional[Dict[str, Any]] = None

    for item in lang_items:
        if not isinstance(item, dict):
            continue
        lang = (item.get("language") or "").strip().lower()
        if not lang:
            continue
        if lang.startswith("ger") and german_entry is None:
            german_entry = item
        elif lang.startswith("eng") and english_entry is None:
            english_entry = item

    if english_entry:
        english_detected = True
        english_bonus = 10
        reasons.append("English explicitly acceptable/required")
    elif english_detected:
        english_bonus = 10
        reasons.append("English acceptable/mentioned")

    delta = english_bonus

    if german_entry:
        level_raw = (german_entry.get("cefr_guess") or "Unknown").strip()
        level = level_raw.upper()
        conf = _clamp_confidence(german_entry.get("confidence"))
        base_penalty = _LANG_PENALTY.get(level, _LANG_PENALTY.get(level_raw, 0))
        penalty_key = level if level in _LANG_PENALTY else level_raw
        if penalty_key in _LANG_PENALTY:
            penalty = int(round(conf * _LANG_PENALTY[penalty_key] + (1 - conf) * FALLBACK_VAGUE_PENALTY))
            if german_entry.get("customer_facing") and str(german_entry.get("job_post_language") or "").lower() in ("german", "mixed") and penalty < 0:
                penalty -= 5
            german_penalty = penalty
            delta += penalty
            reasons.append(f"German ~{level_raw} (conf {conf:.2f}) → {penalty}")
        else:
            evidence = " ".join(german_entry.get("evidence_phrases") or [])[:160]
            if evidence and ("deutsch" in evidence.lower() or "german" in evidence.lower()):
                penalty = FALLBACK_VAGUE_PENALTY
                german_penalty = penalty
                delta += penalty
                reasons.append(f"German vague requirement (conf {conf:.2f}) → {penalty}")
            else:
                reasons.append("language vague, no German-level inference")
    else:
        reasons.append("no German requirement found")

    return {
        "delta": delta,
        "reasons": reasons,
        "english_bonus": english_bonus,
        "german_penalty": german_penalty,
        "german_entry": german_entry,
        "english_detected": english_detected,
    }

def _vagueness_context_nudge(text_lower: str, english_detected: bool) -> Tuple[int, str]:
    delta = 0
    notes: List[str] = []
    if not english_detected:
        if GERMAN_HEAVY_CONTEXT.search(text_lower):
            delta -= 8
            notes.append("German-heavy context (customer-facing/consulting)")
        if PUBLIC_SECTOR.search(text_lower):
            delta -= 10
            notes.append("Public-sector context")
    return delta, "; ".join(notes) if notes else ""

def _experience_delta(text: str, focus) -> Tuple[int, str]:
    """
    Penalise roles that demand multiple years of experience. The slider in the
    profile controls how strict the penalty is:
    - strength 0.0 → no penalty
    - strength 3.0 → full penalty
    """
    low = text.lower()
    strength = getattr(focus, "experience_penalty_strength", 1.0) or 0.0
    try:
        strength = float(strength)
    except Exception:
        strength = 1.0
    strength = max(0.0, min(strength, 3.0))

    matches = re.findall(r"(\d+)\+?\s+(years|jahr|jahre)", low)
    if not matches:
        if any(token in low for token in ("mehrjährige", "mehrjaehrige", "several years")):
            base = -20
            return (int(base * (strength / 3.0)), "experience: several years required")
        return (0, "experience: not specified")

    max_years = max(int(item[0]) for item in matches)
    focus_max = getattr(focus, "max_required_experience_years", None)

    if focus_max is not None:
        try:
            focus_max = int(focus_max)
        except Exception:
            focus_max = None

    # Light nudge when within the preferred cap; otherwise fall back to defaults.
    if focus_max is not None and max_years <= focus_max:
        base = -5
        return (int(base * (strength / 3.0)), f"experience: {max_years}+ years (within preferred cap {focus_max})")

    if max_years >= 5:
        base = -30
    elif max_years >= 3:
        base = -25
    elif max_years == 2:
        base = -18
    elif max_years == 1:
        base = -10
    else:
        base = 0

    return (int(base * (strength / 3.0)), f"experience: {max_years}+ years mentioned (penalty strength {strength:.1f}/3)")

def _seniority_delta(seniority: str|None) -> Tuple[int,str]:
    if not seniority: return (0, "seniority: unknown")
    s = seniority.strip().lower()
    if s in ("senior",): return (-40, "penalty: Senior")
    if s in ("mid", "intermediate"): return (-15, "penalty: Mid")
    if s in ("junior",): return (+15, "bonus: Junior")
    if s in ("working student","werkstudent","student"): return (+20, "bonus: Working Student")
    if s in ("internship","intern"): return (+18, "bonus: Internship")
    return (0, f"seniority: {seniority}")

def _employment_delta(emp: str|None) -> Tuple[int,str]:
    if not emp: return (0, "employment: unknown")
    e = emp.upper()
    if "PART_TIME" in e: return (+2, "employment: PART_TIME (ok for student/junior)")
    if "FULL_TIME" in e: return (+6, "employment: FULL_TIME (good)")
    if "CONTRACT" in e: return (+0, "employment: CONTRACT")
    return (0, f"employment: {emp}")

def apply_seniority(title: str, seniority: Optional[str], focus=DEFAULT_FOCUS) -> HeuristicComponentResult:
    reasons: List[str] = []
    delta = 0
    components: Dict[str, float] = {"seniority": 0}

    excl = _contains_any(title, list(focus.exclude_titles_any))
    if excl:
        delta += -30
        components["exclude_title"] = -30
        reasons.append(f"title excludes: {', '.join(excl)}")

    inferred = seniority
    if not inferred:
        if re.search(r"\b(werkstudent|working student)\b", title.lower()):
            inferred = "Working Student"
        elif re.search(r"\btrainee\b", title.lower()):
            inferred = "Internship"
        elif re.search(r"\b(intern(ship)?)\b", title.lower()):
            inferred = "Internship"
        elif re.search(r"\bjunior\b", title.lower()):
            inferred = "Junior"
        elif re.search(r"\bsenior\b", title.lower()):
            inferred = "Senior"

    sen_delta, sen_reason = _seniority_delta(inferred)
    delta += sen_delta
    components["seniority"] = sen_delta
    reasons.append(sen_reason)

    return HeuristicComponentResult(
        name="seniority",
        raw_score=delta,
        reasons=reasons,
        meta={"components": components, "seniority": inferred},
    )


def apply_language(text: str, lang_items: Optional[List[Dict[str, Any]]], english_hint: Optional[bool]) -> HeuristicComponentResult:
    reasons: List[str] = []
    components: Dict[str, float] = {"english_ok": 0, "german_requirement": 0}
    delta = 0

    lang_result = _penalize_language(lang_items or [], english_hint)
    delta += lang_result["delta"]
    components["language"] = lang_result["delta"]
    components["english_ok"] = lang_result["english_bonus"]
    components["german_requirement"] = lang_result["german_penalty"]
    if lang_result["reasons"]:
        reasons.extend(lang_result["reasons"])

    german_entry = lang_result.get("german_entry")
    conf = _clamp_confidence(german_entry.get("confidence") if german_entry else 0.0)
    level = (german_entry.get("cefr_guess") or "Unknown").upper() if german_entry else "UNKNOWN"
    english_detected = lang_result.get("english_detected", False)
    if (not german_entry) or level in ("UNKNOWN", "", "NONE") or conf < 0.35:
        nudge_delta, nudge_reason = _vagueness_context_nudge(text, english_detected)
        if nudge_delta:
            delta += nudge_delta
            components["language_context_nudge"] = components.get("language_context_nudge", 0) + nudge_delta
            reasons.append(nudge_reason)

    meta = {
        "components": components,
        "german_entry": german_entry,
        "english_detected": english_detected,
        "language_evidence": {
            "source": (german_entry or {}).get("source"),
            "evidence_phrases": (german_entry or {}).get("evidence_phrases") or [],
            "confidence": (german_entry or {}).get("confidence"),
            "cefr_guess": (german_entry or {}).get("cefr_guess"),
        },
    }
    return HeuristicComponentResult(name="language", raw_score=delta, reasons=reasons, meta=meta)


def apply_skills(text: str, focus=DEFAULT_FOCUS) -> HeuristicComponentResult:
    reasons: List[str] = []
    components: Dict[str, float] = {}
    delta = 0

    include_counts = _count_keywords(text, list(focus.include_skills_any))
    must_bonus = 0
    for k, v in include_counts.items():
        if v > 0:
            must_bonus += 10
    if must_bonus == 0:
        must_bonus = -10
        reasons.append("No must-have skills detected (Python/SQL)")
    else:
        reasons.append(f"Must-have skills present: {', '.join([k for k, v in include_counts.items() if v > 0])}")
    must_delta = min(must_bonus, 25)
    delta += must_delta
    components["include_skills"] = must_delta

    nth_counts = _count_keywords(text, list(focus.nice_to_have))
    nth_bonus = 0
    for k, v in nth_counts.items():
        if v > 0:
            nth_bonus += 4
    nth_bonus = min(nth_bonus, 12)
    delta += nth_bonus
    components["nice_to_have"] = nth_bonus
    if nth_bonus > 0:
        reasons.append("Nice-to-have skills present")

    meta = {
        "components": components,
        "must_have_counts": include_counts,
        "nice_to_have_counts": nth_counts,
    }
    return HeuristicComponentResult(name="skills", raw_score=delta, reasons=reasons, meta=meta)


def apply_location(title: str, loc: str, focus=DEFAULT_FOCUS) -> HeuristicComponentResult:
    reasons: List[str] = []
    components: Dict[str, float] = {}
    delta = 0

    loc_hits = _contains_any(f"{title} {loc}", list(focus.locations_any))
    if loc_hits:
        delta += 8
        components["location"] = 8
        reasons.append(f"Location match: {', '.join(loc_hits)}")
    else:
        components["location"] = 0

    return HeuristicComponentResult(name="location", raw_score=delta, reasons=reasons, meta={"components": components})


def apply_employment_type(employment: Optional[str]) -> HeuristicComponentResult:
    reasons: List[str] = []
    components: Dict[str, float] = {}
    delta = 0

    emp_delta, emp_msg = _employment_delta(employment)
    delta += emp_delta
    components["employment_type"] = emp_delta
    reasons.append(emp_msg)

    return HeuristicComponentResult(name="employment_type", raw_score=delta, reasons=reasons, meta={"components": components})


def apply_experience(text: str, focus=DEFAULT_FOCUS) -> HeuristicComponentResult:
    reasons: List[str] = []
    components: Dict[str, float] = {}
    delta = 0

    exp_delta, exp_msg = _experience_delta(text, focus)
    delta += exp_delta
    components["experience"] = exp_delta
    reasons.append(exp_msg)

    return HeuristicComponentResult(name="experience", raw_score=delta, reasons=reasons, meta={"components": components})


COMPONENT_FUNCS = [
    apply_seniority,
    apply_language,
    apply_skills,
    apply_location,
    apply_employment_type,
    apply_experience,
]


def aggregate_heuristic(component_results: List[HeuristicComponentResult], weights: HeuristicWeights = DEFAULT_HEURISTIC_WEIGHTS) -> float:
    total = weights.base_score
    for res in component_results:
        w = weights.components.get(res.name, 1.0)
        total += w * res.raw_score
    return max(0.0, min(100.0, total))


def compute_alpha(
    *,
    llm_ok: bool,
    llm_confidence: float | None,
    text_len: int,
    score_gap: float,
    risk_flags: Any,
    critical_blockers: Any,
) -> float:
    """
    Decide how much to trust heuristics (alpha) vs LLM (1-alpha).
    Alpha closer to 1.0 means heuristic dominance.
    """
    if not llm_ok:
        return 1.0

    alpha = 0.70  # baseline: heuristics dominate (slightly more LLM weight)
    conf = None if llm_confidence is None else float(llm_confidence)

    if conf is not None:
        if conf >= 0.80:
            alpha -= 0.10
        elif conf <= 0.40:
            alpha += 0.10

    if text_len < 1200:
        alpha += 0.05

    if score_gap > 25:
        alpha += 0.05

    if risk_flags:
        alpha += 0.05

    if critical_blockers:
        alpha += 0.05

    return max(0.65, min(0.90, alpha))


def score_job(
    job: Dict[str, Any],
    focus=DEFAULT_FOCUS,
    use_llm_scoring: Optional[bool] = None,
    apply_blocker_cap: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Compute a 0–100 junior-fit score. Returns {score:int, reasons:[...], components:{...}}.
    Uses enrichment fields when present; otherwise falls back to keyword heuristics.
    """
    title = (job.get("title") or "").strip()
    desc = (job.get("description_text") or "")
    loc = (job.get("location") or "")
    employment = job.get("employment_type")

    text = f"{title}\n{desc}\n{loc}".strip()
    text_lower = text.lower()

    english_hint = job.get("english_ok")
    if english_hint is None:
        english_hint = bool(re.search(r"\b(english|englisch)\b", text_lower))

    lang_items = job.get("language_requirements")
    if isinstance(lang_items, list):
        lang_items = [item for item in lang_items if isinstance(item, dict)]
        if not lang_items:
            lang_items = None
    else:
        lang_items = None

    # Normalize language requirements using precedence (LLM=None at this stage)
    lang_items, _lang_evidence = resolve_language_items(
        text_lower=text_lower,
        job=job,
        lang_items=lang_items,
        english_hint=bool(english_hint),
        llm_part=None,
        focus=focus,
    )
    job["language_requirements"] = lang_items

    component_results: List[HeuristicComponentResult] = []
    component_results.append(apply_seniority(title, job.get("seniority"), focus))
    component_results.append(apply_language(text_lower, lang_items, english_hint))
    component_results.append(apply_skills(text, focus))
    component_results.append(apply_location(title, loc, focus))
    component_results.append(apply_employment_type(employment))
    component_results.append(apply_experience(text, focus))

    def _summarize_components(comp_results: List[HeuristicComponentResult]):
        reasons_local: List[str] = []
        components_local: Dict[str, float] = {}
        meta_local: Dict[str, Any] = {}
        derived_english_local = False
        derived_german_local = None
        must_counts_local = {}
        nice_counts_local = {}
        seniority_value_local = job.get("seniority")

        for res in comp_results:
            reasons_local.extend(res.reasons)
            meta_local[res.name] = res.meta
            for k, v in res.meta.get("components", {}).items():
                components_local[k] = v
            if res.name == "language":
                derived_english_local = bool(res.meta.get("english_detected", False))
                german_entry = res.meta.get("german_entry")
                derived_german_local = german_entry.get("cefr_guess") if german_entry else None
            if res.name == "skills":
                must_counts_local = res.meta.get("must_have_counts", {})
                nice_counts_local = res.meta.get("nice_to_have_counts", {})
            if res.name == "seniority" and res.meta.get("seniority"):
                seniority_value_local = res.meta.get("seniority")

        return (
            reasons_local,
            components_local,
            meta_local,
            derived_english_local,
            derived_german_local,
            must_counts_local,
            nice_counts_local,
            seniority_value_local,
        )

    score_val = aggregate_heuristic(component_results)
    (
        reasons,
        components,
        meta,
        derived_english,
        derived_german,
        must_counts,
        nice_counts,
        seniority_value,
    ) = _summarize_components(component_results)

    # Optional LLM-assisted score with dynamic alpha
    llm_part = None
    alpha = 1.0  # default: heuristic only
    do_llm = settings.use_llm_scoring if use_llm_scoring is None else bool(use_llm_scoring)
    do_cap = settings.apply_blocker_cap if apply_blocker_cap is None else bool(apply_blocker_cap)

    if do_llm:
        llm_part = llm_score_job(job, focus, {"heuristic_score": score_val, "components": components, "reasons": reasons, "meta": meta})

    if llm_part and settings.llm_language_override:
        lang_items2, _lang_evidence2 = resolve_language_items(
            text_lower=text_lower,
            job=job,
            lang_items=lang_items,
            english_hint=bool(english_hint),
            llm_part=llm_part,
            focus=focus,
        )
        if lang_items2 != lang_items:
            lang_items = lang_items2
            job["language_requirements"] = lang_items2

            # refresh just the language component (then recompute heuristic aggregate)
            lang_override_res = apply_language(text_lower, lang_items2, english_hint)
            for k, cr in enumerate(component_results):
                if cr.name == "language":
                    component_results[k] = lang_override_res
                    break
            else:
                component_results.append(lang_override_res)

            score_val = aggregate_heuristic(component_results)
            (
                reasons,
                components,
                meta,
                derived_english,
                derived_german,
                must_counts,
                nice_counts,
                seniority_value,
            ) = _summarize_components(component_results)

            german_entry = meta.get("language", {}).get("german_entry")
            if german_entry:
                job["german_requirement"] = str(german_entry.get("cefr_guess") or "").upper()

    final_score = float(score_val)
    if do_llm and llm_part:
        llm_score = llm_part.get("llm_score")
        llm_conf = llm_part.get("confidence")
        llm_ok = isinstance(llm_score, (int, float))

        if llm_ok:
            text_len = len(text)
            gap = abs(float(score_val) - float(llm_score))
            alpha = compute_alpha(
                llm_ok=True,
                llm_confidence=llm_conf,
                text_len=text_len,
                score_gap=gap,
                risk_flags=llm_part.get("risk_flags"),
                critical_blockers=llm_part.get("critical_blockers"),
            )
            final_score = alpha * float(score_val) + (1.0 - alpha) * float(llm_score)
            final_score = max(0.0, min(100.0, final_score))
        else:
            alpha = 1.0
            final_score = float(score_val)

    blockers = classify_blockers(job=job, focus=focus, llm_part=llm_part)
    cap_meta = apply_blocker_caps(score=final_score, focus=focus, blockers=blockers, enabled=do_cap)
    final_score = cap_meta["score"]

    result = {
        "score": int(round(final_score)),
        "heuristic_score": score_val,
        "heuristic_version": HEURISTIC_SCORING_VERSION,
        "version": SCORING_VERSION,
        "reasons": reasons,
        "components": components,
        "derived": {
            "english_ok": derived_english,
            "german_requirement": derived_german,
            "seniority": seniority_value,
            "must_have_counts": must_counts,
            "nice_to_have_counts": nice_counts,
        },
        "alpha": alpha,
        "llm_enabled": do_llm,
        "blocker_cap_enabled": do_cap,
        "blockers_hard": blockers.get("hard"),
        "blockers_soft": blockers.get("soft"),
        "cap_applied": cap_meta.get("cap_applied"),
        "cap_value": cap_meta.get("cap_value"),
        "cap_reason": cap_meta.get("cap_reason"),
    }

    if llm_part:
        result.update(
            {
                "llm_score": llm_part.get("llm_score"),
                "llm_scoring_version": llm_part.get("llm_scoring_version", LLM_SCORING_VERSION),
                "risk_flags": llm_part.get("risk_flags"),
                "critical_blockers": llm_part.get("critical_blockers"),
                "german_requirement_llm": llm_part.get("german_requirement"),
                "llm_summary": llm_part.get("summary"),
                "llm_ok": bool(llm_part.get("llm_ok")) if llm_part.get("llm_ok") is not None else isinstance(llm_part.get("llm_score"), (int, float)),
                "llm_confidence": llm_part.get("confidence"),
                "llm_error_type": llm_part.get("error_type"),
                "llm_error_message": llm_part.get("error_message"),
                "llm_raw_excerpt": llm_part.get("raw_excerpt"),
                "llm_debug": (
                    f"alpha={alpha:.2f} llm_score={llm_part.get('llm_score')} "
                    f"llm_ok={llm_part.get('llm_ok')} err_type={llm_part.get('error_type')} "
                    f"err={llm_part.get('error_message')}"
                ),
            }
        )
    else:
        result["llm_debug"] = f"alpha={alpha:.2f} llm_score=None ok=False err=None"

    return result
