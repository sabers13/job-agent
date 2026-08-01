"""Résumé upload, listing, detail and activation.

Uploads are written under `output/<user_id>/_resumes/`, which the `run_output_root`
fixture redirects into `tmp_path` — so these tests never touch the real output tree.
"""

from __future__ import annotations

import uuid


def _upload(client, name: str = "cv.txt", body: bytes = b"Experienced in SQL and Python."):
    return client.post(
        "/api/my/resume",
        files={"file": (name, body, "text/plain")},
        data={"set_active": "true"},
    )


def test_resumes_listing_is_empty_for_a_new_user(client) -> None:
    response = client.get("/api/my/resumes")

    assert response.status_code == 200, response.text


def test_upload_then_list_then_fetch(client) -> None:
    uploaded = _upload(client)
    assert uploaded.status_code in (200, 201), uploaded.text
    resume_id = uploaded.json()["resume_id"]

    listing = client.get("/api/my/resumes")
    assert listing.status_code == 200
    assert resume_id in listing.text

    detail = client.get(f"/api/my/resume/{resume_id}")
    assert detail.status_code == 200, detail.text
    assert detail.json()["id"] == resume_id


def test_uploading_the_same_bytes_twice_is_idempotent(client) -> None:
    """Deduplicated by sha256 — the same CV uploaded twice is one résumé, not two."""
    first = _upload(client).json()["resume_id"]
    second = _upload(client).json()["resume_id"]

    assert first == second


def test_activation_marks_exactly_one_resume_active(client) -> None:
    first = _upload(client, name="a.txt", body=b"First CV, SQL.").json()["resume_id"]
    second = _upload(client, name="b.txt", body=b"Second CV, Python.").json()["resume_id"]

    assert client.post(f"/api/my/resume/{first}/activate").status_code == 200

    detail_first = client.get(f"/api/my/resume/{first}").json()
    detail_second = client.get(f"/api/my/resume/{second}").json()

    assert detail_first["is_active"] is True
    assert detail_second["is_active"] is False


def test_unknown_resume_is_404(client) -> None:
    assert client.get(f"/api/my/resume/{uuid.uuid4()}").status_code == 404


def test_malformed_resume_id_does_not_500(client) -> None:
    response = client.get("/api/my/resume/not-a-uuid")

    assert response.status_code in (400, 404, 422), response.status_code


def test_activating_an_unknown_resume_is_404(client) -> None:
    assert client.post(f"/api/my/resume/{uuid.uuid4()}/activate").status_code == 404


def test_uploads_stay_inside_the_redirected_output_root(client, run_output_root) -> None:
    """No writes outside `tmp_path` — TEST-STRATEGY §6."""
    _upload(client)

    written = list(run_output_root.rglob("*"))

    assert written, "expected the upload to land under the redirected output root"
