"""The offset-based log-streaming protocol.

One of the four things AGENTS.md says must never break, and the GUI polls it in a loop:
read a chunk, keep `next_offset`, pass it back next time. If offsets drift the UI either
silently loses log lines or replays them forever.

Tested at both levels deliberately. `read_log_chunk` is the protocol and is exercised
exhaustively; the HTTP endpoint is the contract the GUI actually depends on, and it must
keep behaving identically after Slices 5 and 7 move the code.

Offsets are **byte** offsets, not character offsets — there is a test below pinning that,
because multi-byte UTF-8 is where an offset protocol usually breaks first.
"""

from __future__ import annotations

from pathlib import Path

import pytest


def _write_log(run_dir: Path, content: str) -> Path:
    log = run_dir / "run.log"
    log.write_text(content, encoding="utf-8")
    return log


# --------------------------------------------------------------------------- #
# The protocol
# --------------------------------------------------------------------------- #


def test_offset_zero_returns_from_the_start(make_run) -> None:
    from app.gui_runs import run_manager

    run_id, run_dir = make_run()
    _write_log(run_dir, "hello world")

    chunk, next_offset = run_manager.read_log_chunk(run_id, offset=0)

    assert chunk == "hello world"
    assert next_offset == len("hello world")


def test_mid_file_offset_returns_the_right_slice(make_run) -> None:
    from app.gui_runs import run_manager

    run_id, run_dir = make_run()
    _write_log(run_dir, "0123456789")

    chunk, next_offset = run_manager.read_log_chunk(run_id, offset=4)

    assert chunk == "456789"
    assert next_offset == 10


def test_offset_at_eof_returns_empty_and_the_same_offset(make_run) -> None:
    from app.gui_runs import run_manager

    run_id, run_dir = make_run()
    body = "some log output"
    _write_log(run_dir, body)

    chunk, next_offset = run_manager.read_log_chunk(run_id, offset=len(body))

    assert chunk == ""
    assert next_offset == len(body), "an idle poll must not move the offset"


def test_offset_past_eof_does_not_error(make_run) -> None:
    """A client that over-reports its offset gets an empty chunk, not an exception.

    It is also clamped back to the true size rather than echoed, so the next poll is
    correct instead of permanently stuck past the end.
    """
    from app.gui_runs import run_manager

    run_id, run_dir = make_run()
    body = "short"
    _write_log(run_dir, body)

    chunk, next_offset = run_manager.read_log_chunk(run_id, offset=10_000)

    assert chunk == ""
    assert next_offset == len(body)


def test_appends_between_reads_are_picked_up(make_run) -> None:
    """The whole point of the protocol: poll, append, poll again, lose nothing."""
    from app.gui_runs import run_manager

    run_id, run_dir = make_run()
    log = _write_log(run_dir, "first\n")

    first_chunk, offset = run_manager.read_log_chunk(run_id, offset=0)
    assert first_chunk == "first\n"

    with log.open("a", encoding="utf-8") as fh:
        fh.write("second\n")

    second_chunk, second_offset = run_manager.read_log_chunk(run_id, offset=offset)

    assert second_chunk == "second\n"
    assert second_offset == len("first\nsecond\n")


def test_max_bytes_is_respected_and_resumable(make_run) -> None:
    """A truncated read reports the offset it actually reached, so the next call resumes."""
    from app.gui_runs import run_manager

    run_id, run_dir = make_run()
    _write_log(run_dir, "abcdefghij")

    chunk, offset = run_manager.read_log_chunk(run_id, offset=0, max_bytes=4)
    assert chunk == "abcd"
    assert offset == 4

    rest, final = run_manager.read_log_chunk(run_id, offset=offset, max_bytes=1024)
    assert rest == "efghij"
    assert final == 10


def test_max_bytes_is_capped(make_run) -> None:
    """`LOG_CHUNK_MAX_BYTES` caps a client asking for more than the server will give."""
    from app.gui_runs import run_manager

    run_id, run_dir = make_run()
    body = "x" * (run_manager.LOG_CHUNK_MAX_BYTES + 5_000)
    _write_log(run_dir, body)

    chunk, offset = run_manager.read_log_chunk(run_id, offset=0, max_bytes=10**9)

    assert len(chunk) == run_manager.LOG_CHUNK_MAX_BYTES
    assert offset == run_manager.LOG_CHUNK_MAX_BYTES


def test_missing_log_file_is_not_an_error(make_run) -> None:
    """Polling starts before the run writes anything. That must not 500."""
    from app.gui_runs import run_manager

    run_id, _ = make_run()

    chunk, offset = run_manager.read_log_chunk(run_id, offset=0)

    assert chunk == ""
    assert offset == 0


def test_offsets_are_byte_offsets_not_character_offsets(make_run) -> None:
    """Pinned because this is where an offset protocol usually breaks.

    "ü" is two bytes in UTF-8. If offsets were character offsets, resuming mid-file
    would slice into the middle of a codepoint and corrupt every later chunk.
    """
    from app.gui_runs import run_manager

    run_id, run_dir = make_run()
    body = "üüü"  # 3 chars, 6 bytes
    _write_log(run_dir, body)

    chunk, offset = run_manager.read_log_chunk(run_id, offset=0)

    assert chunk == body
    assert offset == 6, "next_offset must be a byte count, not a character count"


# --------------------------------------------------------------------------- #
# The HTTP contract — must survive Slices 5 and 7 unchanged
# --------------------------------------------------------------------------- #


def _seed_status(run_id: str, user_id: str, status: str = "running") -> None:
    from app.gui_runs import run_manager

    run_manager.write_status(
        run_id,
        {
            "run_id": run_id,
            "user_id": user_id,
            "profile_key": "profile-1",
            "status": status,
        },
    )


def test_http_log_endpoint_walks_the_file(client, test_user, make_run) -> None:
    run_id, run_dir = make_run(user_id=str(test_user.id))
    _write_log(run_dir, "line one\nline two\n")
    _seed_status(run_id, str(test_user.id))

    first = client.get(f"/api/run_logs/{run_id}", params={"offset": 0, "max_bytes": 9})
    assert first.status_code == 200
    body = first.json()
    assert body["ok"] is True
    assert body["run_id"] == run_id
    assert body["chunk"] == "line one\n"
    assert body["next_offset"] == 9

    second = client.get(f"/api/run_logs/{run_id}", params={"offset": body["next_offset"]})
    assert second.json()["chunk"] == "line two\n"


def test_http_finished_only_when_terminal_and_drained(client, test_user, make_run) -> None:
    """`finished` is the GUI's signal to stop polling.

    It must stay false while bytes remain even after the run reaches a terminal state,
    or the UI stops polling with the tail of the log still unread.
    """
    run_id, run_dir = make_run(user_id=str(test_user.id))
    _write_log(run_dir, "tail output\n")
    _seed_status(run_id, str(test_user.id), status="completed")

    mid = client.get(f"/api/run_logs/{run_id}", params={"offset": 0}).json()
    assert mid["chunk"] != ""
    assert mid["finished"] is False, "still had bytes to deliver"

    drained = client.get(f"/api/run_logs/{run_id}", params={"offset": mid["next_offset"]}).json()
    assert drained["chunk"] == ""
    assert drained["finished"] is True


def test_http_running_run_is_never_finished(client, test_user, make_run) -> None:
    run_id, run_dir = make_run(user_id=str(test_user.id))
    _write_log(run_dir, "")
    _seed_status(run_id, str(test_user.id), status="running")

    body = client.get(f"/api/run_logs/{run_id}", params={"offset": 0}).json()

    assert body["chunk"] == ""
    assert body["finished"] is False


def test_http_unknown_run_is_404(client) -> None:
    assert client.get("/api/run_logs/does-not-exist").status_code == 404


def test_http_another_users_run_is_404_not_403(client, make_run) -> None:
    """Ownership failures must be indistinguishable from absence.

    A 403 would confirm the run exists, letting a caller enumerate other users' run ids.
    """
    run_id, run_dir = make_run(user_id="someone-else")
    _write_log(run_dir, "secret\n")
    _seed_status(run_id, "someone-else")

    assert client.get(f"/api/run_logs/{run_id}").status_code == 404


@pytest.mark.parametrize("offset", [-5, 0])
def test_http_negative_offset_is_clamped(client, test_user, make_run, offset: int) -> None:
    run_id, run_dir = make_run(user_id=str(test_user.id))
    _write_log(run_dir, "abc")
    _seed_status(run_id, str(test_user.id))

    body = client.get(f"/api/run_logs/{run_id}", params={"offset": offset}).json()

    assert body["chunk"] == "abc"
    assert body["next_offset"] == 3


def test_http_requires_authentication(client_unauthed, make_run) -> None:
    run_id, _ = make_run()
    assert client_unauthed.get(f"/api/run_logs/{run_id}").status_code == 401
