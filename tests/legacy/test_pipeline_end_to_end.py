from pathlib import Path
import json

from app.pipeline.parsers import extract_jobposting_from_html
from app.pipeline.models import UnifiedJobPosting
from app.pipeline.scoring import score_job
from app.pipeline.output import write_bundle
from app.config.focus import DEFAULT_FOCUS


def _load_html(name: str) -> str:
    fixture = Path(__file__).parent / "data" / name
    return fixture.read_text(encoding="utf-8")


def test_html_to_bundle(tmp_path: Path):
    html = _load_html("job_stepstone_1.html")

    parsed = extract_jobposting_from_html(html)
    assert parsed.get("title")
    assert parsed.get("company")

    job = UnifiedJobPosting(
        title=parsed.get("title"),
        company=parsed.get("company"),
        location=parsed.get("location") or "",
        employment_type=parsed.get("employment_type"),
        date_posted=parsed.get("date_posted"),
        valid_through=parsed.get("valid_through"),
        url=parsed.get("url") or "https://example.com/job1",
        job_id=parsed.get("job_id"),
        salary=parsed.get("salary"),
        description_text=parsed.get("description_text") or "",
        description_html=parsed.get("description_html") or "",
    )

    scoring = score_job(job.model_dump(mode="json"), DEFAULT_FOCUS)
    assert 0.0 <= scoring["score"] <= 100.0

    out_dir = write_bundle(
        root=str(tmp_path),
        job=job.model_dump(mode="json"),
        assets={"REPORT.md": "# Report"},
        scoring=scoring,
        enrichment_meta={"ok": False, "error_type": "not_run", "error_message": None},
    )

    out_path = Path(out_dir)
    assert out_path.exists()
    meta_path = out_path / "metadata.json"
    assert meta_path.is_file()
    data = json.loads(meta_path.read_text(encoding="utf-8"))
    assert data["job"]["title"]
    assert "score" in data["scoring"]
