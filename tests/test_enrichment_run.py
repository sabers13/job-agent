import importlib
from typing import Any, Dict

import pytest


def _fake_openai_response(json_text: str):
    """Build an object shaped like OpenAI chat.completions.create response."""
    class _Msg:
        def __init__(self, content: str):
            self.content = content

    class _Choice:
        def __init__(self, content: str):
            self.message = _Msg(content)

    class _Resp:
        def __init__(self, content: str):
            self.choices = [_Choice(content)]

    return _Resp(json_text)


def test_enrich_jobposting_uses_openai_model_env_and_merges_fields(monkeypatch):
    """
    Verifies:
      - llm_enrich reads OPENAI_MODEL (reload needed)
      - enrich_jobposting calls client.chat.completions.create(model=...)
      - returned job contains merged enrichment keys
    """
    monkeypatch.setenv("OPENAI_MODEL", "gpt-5-nano")

    # Import + reload to pick up OPENAI_MODEL at import time
    import app.pipeline.llm_enrich as llm_enrich
    importlib.reload(llm_enrich)

    called = {"model": None, "temperature": None, "messages": None}

    class FakeChatCompletions:
        def create(self, model: str, temperature: float, messages: Any):
            called["model"] = model
            called["temperature"] = temperature
            called["messages"] = messages

            # Minimal JSON payload that your enrich_jobposting merges
            return _fake_openai_response(
                """
                {
                  "seniority": "Junior",
                  "english_ok": true,
                  "german_requirement": "B2",
                  "skills_detected": ["Python", "SQL"],
                  "skill_hits": {"Python": 2, "SQL": 1},
                  "reasons_include": ["Has Python/SQL"],
                  "reasons_exclude": [],
                  "language_requirements": [
                    {
                      "language": "German",
                      "cefr_guess": "B2",
                      "confidence": 0.7,
                      "evidence_phrases": ["sehr gute Deutschkenntnisse"],
                      "customer_facing": false,
                      "job_post_language": "German"
                    }
                  ]
                }
                """
            )

    class FakeChat:
        def __init__(self):
            self.completions = FakeChatCompletions()

    class FakeClient:
        def __init__(self):
            self.chat = FakeChat()

    # Force llm_enrich to use our fake client (no network)
    monkeypatch.setattr(llm_enrich, "_client", lambda: FakeClient())

    job_in: Dict[str, Any] = {
        "title": "Data Analyst",
        "company": "Example GmbH",
        "location": "Dortmund",
        "description_text": "We need Python and SQL. Sehr gute Deutschkenntnisse.",
        "url": "https://example.com/job",
    }

    out = llm_enrich.enrich_jobposting(job_in)

    # Assert OpenAI was called with the model you want
    assert called["model"] == "gpt-5-nano"
    assert called["temperature"] == 0.2
    assert isinstance(called["messages"], list) and len(called["messages"]) >= 2

    # Assert enrichment merged expected keys
    assert out["seniority"] == "Junior"
    assert out["english_ok"] is True
    assert out["german_requirement"] == "B2"
    assert "skills_detected" in out and "Python" in out["skills_detected"]
    assert "language_requirements" in out and isinstance(out["language_requirements"], list)


@pytest.mark.asyncio
async def test_fetch_job_details_calls_enrichment_when_enrich_true(monkeypatch):
    """
    Verifies:
      - pipeline.fetch_job_details(enrich=True) invokes enrich_jobposting
      - no network is used (fetch_job_html is mocked)
    """
    import app.pipeline.pipeline as pipeline_mod

    # Mock fetch_job_html (async) to avoid network
    async def fake_fetch_job_html(url: str, preferred_backend=None):
        html = "<html><body>fake</body></html>"
        fetch_meta = {"backend": "http", "attempts": 1, "status": 200}
        return html, fetch_meta

    # Mock parser to avoid HTML dependencies
    def fake_extract_jobposting_from_html(html: str):
        return {
            "title": "Working Student Data",
            "company": "Example AG",
            "location": "Berlin",
            "description_text": "English ok. German nice to have. Python SQL.",
            "date_posted": "2025-12-01T00:00:00Z",
        }

    # Track whether enrichment ran
    flag = {"called": False}

    def fake_enrich_jobposting(job: Dict[str, Any]) -> Dict[str, Any]:
        flag["called"] = True
        # Simulate enrichment output (what scoring/pipeline would receive)
        enriched = dict(job)
        enriched["german_requirement"] = "B1"
        enriched["skills_detected"] = ["Python", "SQL"]
        return enriched

    monkeypatch.setattr(pipeline_mod, "fetch_job_html", fake_fetch_job_html)
    monkeypatch.setattr(pipeline_mod, "extract_jobposting_from_html", fake_extract_jobposting_from_html)
    monkeypatch.setattr(pipeline_mod, "enrich_jobposting", fake_enrich_jobposting)

    result = await pipeline_mod.fetch_job_details(
        "https://example.com/job",
        enrich=True,
        score=False,
        cutoff_iso=None,
        use_cache=False,
    )

    assert result["ok"] is True
    assert flag["called"] is True
    assert result["job"]["german_requirement"] == "B1"
    assert "skills_detected" in result["job"]
