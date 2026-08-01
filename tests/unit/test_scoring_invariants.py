"""The eight scoring invariants from TEST-STRATEGY §5.1.

These hold regardless of tuning, so they never need editing when weights change. That is
the whole point: the old suite asserted `components["experience"] <= -15`, a number that
was a function of `DEFAULT_FOCUS` plus a lookup table that has since been replaced. When
it broke, it could not say whether the cause was a bug, a deliberate change, or a config
edit — flaws **F2** and **F5**.

Rules for this file:

* **Relational assertions only.** `penalty(bigger gap) <= penalty(smaller gap)` survives
  tuning; `== -15` does not.
* **No implicit `DEFAULT_FOCUS`.** Every test builds an explicit profile via
  `profile_factory`. `DEFAULT_FOCUS` is tested on its own in `test_default_focus.py`.
* The **one** permitted absolute is the accept/reject threshold, which is a product
  contract rather than an artefact of the current weights.
"""

from __future__ import annotations

import pytest

from app.pipeline.scoring import score_job

# The only absolute allowed here. This is a product decision, not a tuning artefact.
ACCEPT_THRESHOLD = 70


# --------------------------------------------------------------------------- #
# 1. Determinism
# --------------------------------------------------------------------------- #


def test_scoring_is_deterministic(job_factory, profile_factory) -> None:
    job, focus = job_factory(), profile_factory()

    first, second = score_job(job, focus), score_job(job, focus)

    assert first["score"] == second["score"]
    assert first["components"] == second["components"]


# --------------------------------------------------------------------------- #
# 2. No hidden state
# --------------------------------------------------------------------------- #


def test_scoring_one_job_does_not_affect_the_next(job_factory, profile_factory) -> None:
    """Module-level caches or mutable defaults would show up here and nowhere else."""
    focus = profile_factory()
    target = job_factory(title="Data Analyst", description_text="SQL and Python")
    other = job_factory(title="Head of Engineering", description_text="10+ years leading teams")

    before = score_job(target, focus)
    score_job(other, focus)
    after = score_job(target, focus)

    assert before["score"] == after["score"]
    assert before["components"] == after["components"]


def test_scoring_mutates_the_job_dict_in_exactly_one_known_way(
    job_factory, profile_factory
) -> None:
    """Characterisation, not approval. `score_job` writes into the caller's dict.

    It sets `language_requirements` in place when the key is absent. Pinning the exact
    surface means any *additional* mutation added later fails here loudly, instead of
    silently coupling a caller's dict to scoring internals. Backlog **A11**.

    The profile must not be mutated at all — that one is asserted as an absolute,
    because a shared `FocusConfig` is reused across every job in a run.
    """
    job, focus = job_factory(), profile_factory()
    job_before = dict(job)
    focus_before = {k: getattr(focus, k) for k in ("titles_any", "include_skills_any")}

    score_job(job, focus)

    mutated = {k for k in set(job) | set(job_before) if job.get(k) != job_before.get(k)}
    assert mutated == {"language_requirements"}, f"unexpected input mutation: {mutated}"

    for key, value in focus_before.items():
        assert getattr(focus, key) == value, f"score_job mutated the shared profile: {key}"


# --------------------------------------------------------------------------- #
# 3. Blocker dominance
# --------------------------------------------------------------------------- #


def test_a_hard_blocker_caps_the_score_below_accept(job_factory, profile_factory) -> None:
    """A hard blocker outranks every other signal.

    The job is deliberately built to score *well* on everything else — matching title,
    matching skills, junior seniority — so the only thing that can hold it down is the
    cap. That is what "dominance" has to mean.
    """
    job = job_factory(
        title="Data Analyst",
        location="Munich",
        description_text="SQL, Python and Power BI dashboards for a junior analyst.",
    )
    permissive = profile_factory(relocation_ok=True)
    blocking = profile_factory(relocation_ok=False)

    unblocked = score_job(job, permissive)
    blocked = score_job(job, blocking)

    assert blocked["blockers_hard"], "expected a hard blocker"
    assert blocked["cap_applied"] is True
    assert blocked["score"] < ACCEPT_THRESHOLD
    assert blocked["score"] < unblocked["score"]


def test_the_cap_binds_regardless_of_how_good_the_rest_is(job_factory, profile_factory) -> None:
    """Strengthening every other signal must not lift a hard-blocked job over the cap."""
    blocking = profile_factory(locations_any={"Berlin"}, relocation_ok=False)
    # Munich, so the location genuinely fails to match and the blocker actually fires.
    # With the factory's default Berlin it would not, and this test would pass vacuously.
    weak = score_job(
        job_factory(title="Unrelated Role", location="Munich", description_text="none"), blocking
    )
    strong = score_job(
        job_factory(
            title="Data Analyst",
            location="Munich",
            description_text="SQL, Python, Power BI, DAX, Power Query. Junior friendly.",
            seniority="junior",
        ),
        blocking,
    )

    assert strong["blockers_hard"] and weak["blockers_hard"]

    assert strong["score"] <= blocking.blocker_cap_hard
    assert weak["score"] <= blocking.blocker_cap_hard


# --------------------------------------------------------------------------- #
# 4. Monotonicity — skills
# --------------------------------------------------------------------------- #


def test_adding_a_matching_skill_never_lowers_the_skill_component(
    job_factory, profile_factory
) -> None:
    focus = profile_factory(include_skills_any={"SQL", "Python"}, nice_to_have={"Power BI"})

    none_ = score_job(job_factory(description_text="No relevant technologies."), focus)
    one = score_job(job_factory(description_text="We use SQL."), focus)
    two = score_job(job_factory(description_text="We use SQL and Python."), focus)

    assert (
        none_["components"]["include_skills"]
        <= one["components"]["include_skills"]
        <= two["components"]["include_skills"]
    )


def test_more_matching_skills_never_lowers_the_total(job_factory, profile_factory) -> None:
    focus = profile_factory(include_skills_any={"SQL", "Python"})

    fewer = score_job(job_factory(description_text="We use SQL."), focus)
    more = score_job(job_factory(description_text="We use SQL and Python."), focus)

    assert more["score"] >= fewer["score"]


# --------------------------------------------------------------------------- #
# 5. Monotonicity — experience
# --------------------------------------------------------------------------- #


def test_a_larger_experience_gap_never_scores_better(job_factory, profile_factory) -> None:
    """The property the old `test_experience_penalty_triggers` was reaching for.

    Stated relationally it catches a broken `_experience_delta` without depending on
    `DEFAULT_FOCUS` or on the number 15 — see TEST-STRATEGY §8 for why that test failed.
    """
    focus = profile_factory(max_required_experience_years=3, experience_penalty_strength=1.0)

    deltas = [
        score_job(job_factory(description_text=f"We expect {years}+ years."), focus)["components"][
            "experience"
        ]
        for years in (0, 2, 3, 4, 5, 8)
    ]

    assert deltas == sorted(deltas, reverse=True), f"experience penalty not monotonic: {deltas}"


def test_experience_beyond_the_cap_is_penalised_more_than_within_it(
    job_factory, profile_factory
) -> None:
    focus = profile_factory(max_required_experience_years=3, experience_penalty_strength=1.0)

    within = score_job(job_factory(description_text="We expect 3+ years."), focus)
    beyond = score_job(job_factory(description_text="We expect 8+ years."), focus)

    assert beyond["components"]["experience"] < within["components"]["experience"]


def test_penalty_strength_scales_the_experience_penalty(job_factory, profile_factory) -> None:
    """The slider does something, and zero means zero."""
    job = job_factory(description_text="We expect 8+ years of experience.")

    off = score_job(job, profile_factory(experience_penalty_strength=0.0))
    mild = score_job(job, profile_factory(experience_penalty_strength=1.0))
    full = score_job(job, profile_factory(experience_penalty_strength=3.0))

    assert off["components"]["experience"] == 0
    assert full["components"]["experience"] < mild["components"]["experience"] < 0


def test_raising_the_experience_cap_softens_the_penalty(job_factory, profile_factory) -> None:
    job = job_factory(description_text="We expect 5+ years of experience.")

    strict = score_job(job, profile_factory(max_required_experience_years=3))
    lenient = score_job(job, profile_factory(max_required_experience_years=5))

    assert lenient["components"]["experience"] > strict["components"]["experience"]


# --------------------------------------------------------------------------- #
# 6. Boundedness
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "description",
    [
        "",
        "SQL",
        "SQL, Python, Power BI, DAX and 10+ years of experience required.",
        "Wir suchen eine Führungskraft mit mehrjähriger Erfahrung.",
        "x" * 5000,
    ],
)
def test_total_score_stays_within_range(job_factory, profile_factory, description: str) -> None:
    result = score_job(job_factory(description_text=description), profile_factory())

    assert 0 <= result["score"] <= 100


def test_score_is_an_int_and_components_are_numeric(job_factory, profile_factory) -> None:
    """Shape, not value — the artifact writer and the GUI both depend on it."""
    result = score_job(job_factory(), profile_factory())

    assert isinstance(result["score"], int)
    assert result["components"]
    for name, value in result["components"].items():
        assert isinstance(value, (int, float)), f"{name} is {type(value).__name__}"


# --------------------------------------------------------------------------- #
# 7. Profile sensitivity
# --------------------------------------------------------------------------- #


def test_the_same_job_scores_differently_against_different_profiles(
    job_factory, profile_factory
) -> None:
    """...and in the expected direction, not merely differently."""
    job = job_factory(title="Data Analyst", description_text="SQL reporting.")

    matching = score_job(job, profile_factory(include_skills_any={"SQL"}))
    mismatched = score_job(job, profile_factory(include_skills_any={"Kubernetes", "Rust"}))

    assert matching["score"] > mismatched["score"]


def test_an_excluded_title_scores_below_a_preferred_one(job_factory, profile_factory) -> None:
    focus = profile_factory(titles_any={"Data Analyst"}, exclude_titles_any={"Head"})

    preferred = score_job(job_factory(title="Data Analyst"), focus)
    excluded = score_job(job_factory(title="Head of Data"), focus)

    assert excluded["score"] < preferred["score"]


# --------------------------------------------------------------------------- #
# 8. LLM-off baseline
# --------------------------------------------------------------------------- #


def test_llm_disabled_yields_the_pure_heuristic(job_factory, profile_factory) -> None:
    result = score_job(job_factory(), profile_factory(), use_llm_scoring=False)

    assert result["llm_enabled"] is False
    assert result["score"] == result["heuristic_score"]
    assert result["heuristic_version"]


def test_llm_disabled_is_the_default(job_factory, profile_factory) -> None:
    """The offline suite must not depend on an env var to stay offline."""
    assert score_job(job_factory(), profile_factory())["llm_enabled"] is False


def test_blockers_still_dominate_when_the_llm_is_mocked(
    job_factory, profile_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A maximally enthusiastic LLM must not lift a hard-blocked job over the cap.

    This is the invariant that matters for LLM scoring: the model advises, the blocker
    rules decide.
    """
    from app.pipeline import scoring as scoring_module

    monkeypatch.setattr(
        scoring_module,
        "llm_score_job",
        lambda *args, **kwargs: {
            "llm_score": 100,
            "confidence": 1.0,
            "reasons": ["perfect"],
            "critical_blockers": [],
        },
        raising=False,
    )

    blocked = score_job(
        job_factory(title="Data Analyst", location="Munich", description_text="SQL, Python."),
        profile_factory(relocation_ok=False),
        use_llm_scoring=True,
    )

    assert blocked["cap_applied"] is True
    assert blocked["score"] <= 35
    assert blocked["score"] < ACCEPT_THRESHOLD


def test_blocker_cap_can_be_disabled_explicitly(job_factory, profile_factory) -> None:
    """`apply_blocker_cap=False` is a real switch, so its effect is pinned."""
    job = job_factory(title="Data Analyst", location="Munich", description_text="SQL, Python.")
    focus = profile_factory(relocation_ok=False)

    capped = score_job(job, focus, apply_blocker_cap=True)
    uncapped = score_job(job, focus, apply_blocker_cap=False)

    assert capped["cap_applied"] is True
    assert uncapped["cap_applied"] is False
    assert uncapped["score"] >= capped["score"]


# --------------------------------------------------------------------------- #
# Characterisation of a known gap — see TEST-STRATEGY §8 "Noted, not fixed"
# --------------------------------------------------------------------------- #


def test_singular_english_year_is_not_matched(job_factory, profile_factory) -> None:
    """Characterises a real gap rather than asserting it is correct.

    The regex is `(\\d+)\\+?\\s+(years|jahr|jahre)`, so singular English "year" never
    matches and "1 year of experience" attracts no penalty. Pinned so that fixing it is
    a visible, deliberate behaviour change (bucket C) rather than a silent one.
    """
    focus = profile_factory(max_required_experience_years=0, experience_penalty_strength=3.0)

    singular = score_job(job_factory(description_text="We expect 1 year."), focus)
    plural = score_job(job_factory(description_text="We expect 1 years."), focus)

    assert singular["components"]["experience"] == 0
    assert plural["components"]["experience"] < 0
