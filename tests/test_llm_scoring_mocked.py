from __future__ import annotations

from app.pipeline import scoring as scoring_mod


def test_llm_scoring_hard_blocker_caps(monkeypatch):
    def fake_llm_score_job(job, focus, heuristic_payload):
        return {
            "llm_ok": True,
            "llm_score": 95,
            "llm_scoring_version": "test",
            "risk_flags": ["German required"],
            "critical_blockers": ["unverified_german_proficiency_at_required_level"],
            "german_requirement": {"type": "hard_blocker", "min_level": "C1", "justification": "Fluent German required"},
            "summary": "Hard blocker unless German is verified",
        }

    monkeypatch.setattr(scoring_mod, "llm_score_job", fake_llm_score_job)

    job = {
        "title": "Intern Consultant",
        "company": "Example AG",
        "location": "München, DE",
        "employment_type": "FULL_TIME",
        "description_text": "Fluent in English and German. SQL required.",
        "seniority": "Internship",
        "language_requirements": [],
    }

    result = scoring_mod.score_job(job, use_llm_scoring=True, apply_blocker_cap=True)

    assert result["llm_enabled"] is True
    assert result["blocker_cap_enabled"] is True
    assert result["llm_ok"] is True
    assert result["score"] <= 35
    assert result["german_requirement_llm"]["min_level"] == "C1"

