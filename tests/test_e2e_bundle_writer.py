from __future__ import annotations

import json
from pathlib import Path

from app.pipeline.output import write_bundle
from app.pipeline.templating import generate_bundle


def test_bundle_writer_creates_valid_metadata_json(tmp_path: Path):
    job = {
        "title": "Junior Data Analyst",
        "company": "Example GmbH",
        "location": "Dortmund, DE",
        "description_text": "SQL required. ETL and BI helpful.",
        "seniority": "Internship",
        "language_requirements": [],
    }
    scoring = {"score": 77, "reasons": ["test"], "components": {}, "heuristic_score": 77}

    enrichment_meta = {"ok": True, "debug_tags": {"a", "b"}}

    assets = generate_bundle(job, scoring)
    out_dir = write_bundle(str(tmp_path), job, assets, scoring, seed_slug="seed", enrichment_meta=enrichment_meta)

    meta_path = Path(out_dir) / "metadata.json"
    assert meta_path.exists()

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["job"]["title"] == "Junior Data Analyst"
    assert meta["scoring"]["score"] == 77
    assert meta["enrichment_meta"]["debug_tags"] == ["a", "b"]

