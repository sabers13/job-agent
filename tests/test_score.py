from app.pipeline.scoring import score_job

def test_scoring_junior_english_python_sql():
    job = {
        "title": "Junior Data Analyst",
        "company": "TestCo",
        "location": "Dortmund, DE",
        "employment_type": "FULL_TIME",
        "description_text": "We need Python and SQL. English is OK.",
        "seniority": "Junior",
        "english_ok": True,
        "german_requirement": None
    }
    res = score_job(job)
    assert 70 <= res["score"] <= 100
    assert res["derived"]["must_have_counts"]["Python"] >= 1
    assert res["derived"]["must_have_counts"]["SQL"] >= 1

def test_scoring_senior_german_high():
    job = {
        "title": "Senior Data Analyst (Team Lead)",
        "company": "TestCo",
        "location": "Munich, DE",
        "employment_type": "FULL_TIME",
        "description_text": "German C1 required. Lead the team.",
        "seniority": "Senior",
        "english_ok": False,
        "german_requirement": "C1"
    }
    res = score_job(job)
    assert 0 <= res["score"] <= 60
