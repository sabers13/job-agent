"""`DEFAULT_FOCUS` is a thing under test, not a precondition of other tests.

TEST-STRATEGY §6: "`DEFAULT_FOCUS` gets its own dedicated test asserting its shape, so a
change to it fails one obvious test instead of silently shifting a dozen unrelated ones."

That is the direct fix for flaw **F2**. The old `test_experience_penalty_triggers`
asserted `components["experience"] <= -15`, a number that was a function of
`DEFAULT_FOCUS.max_required_experience_years` — so an edit to a config default broke a
test that claimed to be about code, and the failure could not say which had changed.
See TEST-STRATEGY §8.

This file is the *only* place in the new suite that touches `DEFAULT_FOCUS`. Everything
else builds an explicit profile via `profile_factory`. A guard test at the bottom enforces
that.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from app.config.focus import DEFAULT_FOCUS, FocusConfig

TESTS_ROOT = Path(__file__).resolve().parents[1]


def test_default_focus_is_a_focus_config() -> None:
    assert isinstance(DEFAULT_FOCUS, FocusConfig)


@pytest.mark.parametrize(
    "attribute",
    [
        "profile_name",
        "target_seniority",
        "max_allowed_seniority",
        "max_required_experience_years",
        "experience_penalty_strength",
        "titles_any",
        "exclude_titles_any",
        "locations_any",
        "excluded_locations",
        "include_skills_any",
        "nice_to_have",
        "min_german_level",
        "requires_student_status",
        "candidate_german_level",
        "relocation_ok",
        "strict_language_blocker",
        "blocker_cap_hard",
        "blocker_cap_soft",
    ],
)
def test_default_focus_exposes_every_attribute_scoring_reads(attribute: str) -> None:
    """Shape, not values. Scoring reads these by name; a rename must fail here first."""
    assert hasattr(DEFAULT_FOCUS, attribute), f"DEFAULT_FOCUS lost {attribute!r}"


def test_default_focus_types_are_what_scoring_expects() -> None:
    assert isinstance(DEFAULT_FOCUS.titles_any, (set, frozenset))
    assert isinstance(DEFAULT_FOCUS.exclude_titles_any, (set, frozenset))
    assert isinstance(DEFAULT_FOCUS.locations_any, (set, frozenset))
    assert isinstance(DEFAULT_FOCUS.include_skills_any, (set, frozenset))
    assert isinstance(DEFAULT_FOCUS.nice_to_have, (set, frozenset))
    assert isinstance(DEFAULT_FOCUS.experience_penalty_strength, (int, float))
    assert isinstance(DEFAULT_FOCUS.relocation_ok, bool)


def test_default_focus_values_are_self_consistent() -> None:
    """Relational, so tuning the defaults does not break this.

    A hard blocker must cap harder than a soft one, and the experience slider must sit
    inside the 0–3 range `_experience_delta` clamps to — otherwise the configured value
    silently means something other than what it says.
    """
    assert DEFAULT_FOCUS.blocker_cap_hard < DEFAULT_FOCUS.blocker_cap_soft
    assert 0.0 <= DEFAULT_FOCUS.experience_penalty_strength <= 3.0
    if DEFAULT_FOCUS.max_required_experience_years is not None:
        assert DEFAULT_FOCUS.max_required_experience_years >= 0


def test_default_focus_is_usable_for_scoring() -> None:
    """It must actually work — a broken default is a broken first-run experience."""
    from app.pipeline.models import UnifiedJobPosting
    from app.pipeline.scoring import score_job

    job = UnifiedJobPosting(
        title="Data Analyst", company="X", location="Berlin", description_text="SQL"
    ).model_dump()

    result = score_job(job, DEFAULT_FOCUS)

    assert 0 <= result["score"] <= 100


def test_no_other_test_depends_on_default_focus_implicitly() -> None:
    """Enforces the checklist item rather than trusting review to catch it.

    Any new test importing `DEFAULT_FOCUS` re-creates flaw F2, so the rule is executable:
    this file may reference it, and nothing else under `tests/unit` or `tests/contracts`
    may.
    """
    import ast

    offenders: list[str] = []
    for path in [*(TESTS_ROOT / "unit").rglob("*.py"), *(TESTS_ROOT / "contracts").rglob("*.py")]:
        if path.resolve() == Path(__file__).resolve():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        # AST, not a substring search: prose in a docstring explaining *why* a test
        # avoids DEFAULT_FOCUS is not a dependency on it.
        referenced = any(
            (isinstance(node, ast.Name) and node.id == "DEFAULT_FOCUS")
            or (isinstance(node, ast.Attribute) and node.attr == "DEFAULT_FOCUS")
            or (
                isinstance(node, ast.ImportFrom)
                and any(alias.name == "DEFAULT_FOCUS" for alias in node.names)
            )
            for node in ast.walk(tree)
        )
        if referenced:
            offenders.append(str(path.relative_to(TESTS_ROOT)))

    assert not offenders, (
        f"these tests reference DEFAULT_FOCUS implicitly: {offenders}. "
        "Build an explicit profile with profile_factory instead (TEST-STRATEGY §6)."
    )
