import asyncio
from pathlib import Path

from app.pipeline import pipeline
from app.fetching import polite_fetch


def test_fetch_job_details_with_fixture(monkeypatch):
    html_path = Path(__file__).parent / "data" / "job_stepstone_1.html"
    html = html_path.read_text(encoding="utf-8")
    test_url = "https://example.com/job/abc-123"

    async def fake_fetch_job_html(url: str, preferred_backend=None):
        assert url == test_url
        return html, {"backend": preferred_backend or "http", "attempts": 1, "ok": True}

    monkeypatch.setattr(pipeline, "fetch_job_html", fake_fetch_job_html)
    monkeypatch.setattr(polite_fetch, "fetch_job_html", fake_fetch_job_html)

    result = asyncio.run(
        pipeline.fetch_job_details(
            url=test_url,
            backend="http",
            enrich=False,
            score=True,
            cutoff_iso=None,
            use_cache=False,
        )
    )

    assert result["ok"] is True
    assert result["job"]["title"]
    assert "scoring" in result
    assert 0.0 <= result["scoring"]["score"] <= 100.0
