from __future__ import annotations

from app.pipeline.scoring import score_job


def test_score_job_minimal_payload_no_llm():
    job = {
        "title": "Junior Data Analyst",
        "company": "Example GmbH",
        "location": "Dortmund, DE",
        "employment_type": "FULL_TIME",
        "description_text": "We need SQL and ETL skills. BI dashboards and analytics are a plus.",
        "seniority": "Internship",
        "language_requirements": [],
    }

    scoring = score_job(job, use_llm_scoring=False, apply_blocker_cap=False)

    assert isinstance(scoring, dict)
    assert 0 <= scoring["score"] <= 100
    assert "heuristic_score" in scoring
    assert "components" in scoring
    assert "reasons" in scoring

