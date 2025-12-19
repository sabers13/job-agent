from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.pipeline.scoring import score_job


def _sample_job(**overrides):
    base = {
        "title": "",
        "description_text": "",
        "location": "",
        "seniority": None,
        "english_ok": None,
        "german_requirement": None,
        "employment_type": None,
        "language_requirements": [],
    }
    base.update(overrides)
    return base


def test_junior_english_low_german():
    job = _sample_job(
        title="Junior Data Analyst",
        description_text="We work in English. Python and SQL required.",
        language_requirements=[
            {
                "language": "English",
                "cefr_guess": "B2",
                "confidence": 0.8,
                "evidence_phrases": ["working language English"],
                "customer_facing": False,
                "job_post_language": "English",
            }
        ],
    )
    assert score_job(job)["score"] >= 75


def test_senior_b2_penalised():
    job = _sample_job(
        title="Senior BI Consultant (m/w/d)",
        description_text="Fließendes Deutsch B2 und 5+ Jahre Erfahrung Pflicht.",
        language_requirements=[
            {
                "language": "German",
                "cefr_guess": "B2",
                "confidence": 0.9,
                "evidence_phrases": ["Fließendes Deutsch B2"],
                "customer_facing": True,
                "job_post_language": "German",
            },
            {
                "language": "English",
                "cefr_guess": "B2",
                "confidence": 0.8,
                "evidence_phrases": ["Englischkenntnisse von Vorteil"],
                "customer_facing": False,
                "job_post_language": "Mixed",
            },
        ],
    )
    assert score_job(job)["score"] <= 40


def test_powerbi_bonus():
    base = _sample_job(
        title="Data Analyst",
        description_text="Python and SQL for dashboards.",
        language_requirements=[
            {
                "language": "English",
                "cefr_guess": "B2",
                "confidence": 0.8,
                "evidence_phrases": ["English documentation"],
                "customer_facing": False,
                "job_post_language": "English",
            }
        ],
    )
    enriched = _sample_job(
        title="Data Analyst",
        description_text="Python, SQL, Power BI dashboards with DAX.",
        language_requirements=base["language_requirements"],
    )
    assert score_job(enriched)["score"] > score_job(base)["score"]


def test_experience_penalty_triggers():
    job = _sample_job(
        title="Data Analyst",
        description_text="We expect 3+ years of experience building dashboards.",
        language_requirements=[
            {
                "language": "English",
                "cefr_guess": "B2",
                "confidence": 0.8,
                "evidence_phrases": ["English-speaking team"],
                "customer_facing": False,
                "job_post_language": "English",
            }
        ],
    )
    components = score_job(job)["components"]
    assert components["experience"] <= -15


def test_low_confidence_german_soft_penalty():
    job = _sample_job(
        title="Data Consultant",
        description_text="Wir suchen eine Person mit guten Kommunikationsfähigkeiten und Kundenkontakt.",
        language_requirements=[
            {
                "language": "German",
                "cefr_guess": "B2",
                "confidence": 0.25,
                "evidence_phrases": ["gute Kommunikationsfähigkeiten"],
                "customer_facing": True,
                "job_post_language": "German",
            }
        ],
    )
    components = score_job(job)["components"]
    assert -40 < components["german_requirement"] <= -10
