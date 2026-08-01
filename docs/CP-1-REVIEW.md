# CP-1 — Oracle review

_Reviewed 2026-08-01, against `8b0116e` (Slice 1 green: 208 passed, 0 failed, 0 skipped)._

**Verdict: not trustworthy as-is.** Fix list below.

The structural work is sound — the route inventory derives from the live app, the
`DEFAULT_FOCUS` guard is executable rather than conventional, the scoring invariants are
mostly relational, and the fixture comments record real traps instead of restating the
code. None of that is in question.

What fails the CP-1 bar is narrower and worse: **six places where a test's name and
docstring claim a property the test cannot fail to detect the absence of.** An oracle
that is merely incomplete is safe — the gap is visible as low coverage. An oracle that
asserts a property it does not check is not, because it spends the reviewer's attention
and returns nothing. All six read as covered.

One of the six (**B4**) is not a test defect but a live data-corruption bug in the
log-streaming contract, found by following the test that claims to pin it.

Severities: **B** blocks Slice 3. **S** should land before Slice 5 (the first slice that
moves run-lifecycle code). **L** is hardening — latent today, cheap now, expensive after
the routers move.

---

## B1 — The determinism invariant cannot detect A11, by construction

`tests/unit/test_scoring_invariants.py`

```python
def test_scoring_is_deterministic(job_factory, profile_factory) -> None:
    job, focus = job_factory(), profile_factory()
    first, second = score_job(job, focus), score_job(job, focus)
```

`score_job` writes `job["language_requirements"]` unconditionally
(`app/pipeline/scoring.py:892`). The second call therefore receives a **different input
object** than the first. What is asserted is not `f(x) == f(x)` but
`f(x) == f(mutate(x))` — idempotence of the mutation, not determinism.

Two things hide behind this:

1. **The mutation is invisible here.** This test is listed under "2. No hidden state"
   and reads like the guard against A11. It is not one.
2. **A genuine non-determinism would pass** if the cached `language_requirements`
   stabilises it. Concretely: if the regex path in `resolve_language_items` were
   replaced by anything order- or dict-iteration-dependent, call 1 would seed the key,
   call 2 would take the *structured* branch (`scoring.py:407`) off the seeded value, and
   the two would agree. The test would stay green while scoring had become
   input-order-dependent.

`test_scoring_one_job_does_not_affect_the_next` has the same defect — `target` is mutated
by its own first call before `after` is computed. Its stated purpose ("module-level caches
or mutable defaults would show up here and nowhere else") is only partly served.

**Fix.** Deep-copy per call, so the two inputs are equal but not identical:

```python
import copy

def test_scoring_is_deterministic(job_factory, profile_factory) -> None:
    job, focus = job_factory(), profile_factory()
    first = score_job(copy.deepcopy(job), focus)
    second = score_job(copy.deepcopy(job), focus)
    assert first["score"] == second["score"]
    assert first["components"] == second["components"]
```

Same change in `test_scoring_one_job_does_not_affect_the_next`. Once both pass fresh
copies, A11 is quarantined to exactly one test — which is where the characterisation
belongs.

---

## B2 — The profile-mutation assertion is a no-op

Same file, `test_scoring_mutates_the_job_dict_in_exactly_one_known_way`:

```python
focus_before = {k: getattr(focus, k) for k in ("titles_any", "include_skills_any")}
...
for key, value in focus_before.items():
    assert getattr(focus, key) == value, f"score_job mutated the shared profile: {key}"
```

`value` is a **reference** to the same `set` object, not a copy. After an in-place
mutation, `getattr(focus, key)` and `value` are still the same object, so the comparison
is `s == s` and passes unconditionally. The assertion the docstring calls out as "asserted
as an absolute, because a shared `FocusConfig` is reused across every job in a run" is the
one assertion in the file that can never fire.

`@dataclass(frozen=True)` does not help: it blocks rebinding `focus.titles_any = ...`, not
`focus.titles_any.add(...)`. And because `DEFAULT_FOCUS` is a module-level instance whose
sets are shared process-wide, an in-place mutation there would corrupt every subsequent
run in the same process — the exact failure class this test was written to catch.

No current mutation exists (verified: no `focus.<attr>.{add,update,remove,discard,clear}`
anywhere in `scoring.py`), so this is a **guard that does not guard**, not a live bug.
Worth fixing precisely because it will be trusted through Slices 3–7.

**Fix.** Copy the values, and cover every mutable attribute rather than two:

```python
MUTABLE_FOCUS_FIELDS = (
    "titles_any", "exclude_titles_any", "locations_any",
    "excluded_locations", "include_skills_any", "nice_to_have", "search_seeds",
)
focus_before = {k: copy.deepcopy(getattr(focus, k)) for k in MUTABLE_FOCUS_FIELDS}
```

---

## B3 — The A11 characterisation is shallow, and misses a real second mutation

Same test:

```python
job_before = dict(job)
...
mutated = {k for k in set(job) | set(job_before) if job.get(k) != job_before.get(k)}
assert mutated == {"language_requirements"}
```

`dict(job)` is a shallow copy, so `job.get(k)` and `job_before.get(k)` are the same object
for every mutable value. Any in-place edit of a nested list or dict compares equal to
itself and is reported as unmutated.

This is not hypothetical. `resolve_language_items` mutates a dict **inside** the caller's
list:

```python
# app/pipeline/scoring.py:419
best["source"] = best.get("source") or "structured"
```

`best` is an element of `items`, which is a new list holding the *same dict objects* as
`lang_items` — which on any call after the first is `job["language_requirements"]`, the
caller's data. So `score_job` mutates the caller's nested structures, and A11 is
understated: it is not one top-level key, it is one top-level key plus in-place edits to
that key's contents on every subsequent call.

The test misses this for two compounding reasons: shallow copy, and it only ever calls
`score_job` once (so the structured branch, which is only reachable with a pre-seeded
`language_requirements`, is never entered under assertion).

Also worth correcting: the docstring says "It sets `language_requirements` in place when
the key is absent." The assignment at line 892 is unconditional. A characterisation test
that mis-describes the behaviour it pins is worse than no docstring — this one will be
read as authoritative during Slice 6.

**Fix.**

```python
job_before = copy.deepcopy(job)
```

...and add a second scoring pass to cover the re-entrant path:

```python
def test_rescoring_an_already_scored_job_is_stable(job_factory, profile_factory) -> None:
    """The pipeline scores, persists, and rescores. The second pass must agree."""
    job, focus = job_factory(description_text="Sehr gute Deutschkenntnisse."), profile_factory()
    first = score_job(job, focus)          # seeds job["language_requirements"]
    second = score_job(job, focus)         # now takes the structured branch
    assert first["score"] == second["score"]
    assert first["components"] == second["components"]
```

That test is the one that should own the mutation-tolerance property. Once it exists, B1's
fix is free of risk.

---

## B4 — Live bug: log streaming corrupts multi-byte characters at chunk boundaries

`app/gui_runs/run_manager.py:161-166`

```python
read_upto = min(size, offset + max_bytes)
with lp.open("rb") as f:
    f.seek(offset)
    data = f.read(read_upto - offset)
chunk = data.decode("utf-8", errors="replace")
return chunk, read_upto
```

When `max_bytes` lands mid-codepoint, `errors="replace"` silently substitutes U+FFFD, and
the next read resumes at `read_upto` — still mid-codepoint — producing more replacement
characters. The bytes are never recovered. Reproduced against the exact slicing logic:

```
max_bytes=3   'üüü'                  -> 'ü��ü'
max_bytes=4   '完了\n'                -> '完���\n'
max_bytes=5   'Düsseldorf für Köln'  -> lossless (aligned by luck)
```

This is not a corner case for this project. It is a German-market job board; run logs
carry job titles, city names and posting text. Whether a given boundary corrupts is
effectively a coin flip per chunk, which is why it presents as intermittent garbling
rather than a reproducible failure.

It sits inside one of the four things `AGENTS.md` says must never break, and the suite
certifies it green.

**Why the oracle misses it.** The two relevant tests do not intersect:

- `test_offsets_are_byte_offsets_not_character_offsets` uses `"üüü"` but leaves
  `max_bytes` at its 4096 default, so the whole file comes back in one read — no boundary.
  Its docstring describes precisely this failure mode ("resuming mid-file would slice into
  the middle of a codepoint") and then does not construct it.
- `test_max_bytes_is_respected_and_resumable` constructs the boundary but uses
  `"abcdefghij"` — pure ASCII, where every boundary is safe.

**Fix — test.** The missing intersection:

```python
@pytest.mark.parametrize("max_bytes", [1, 2, 3, 4, 5, 7])
def test_chunked_reads_reassemble_multibyte_text_losslessly(make_run, max_bytes: int) -> None:
    from app.gui_runs import run_manager

    run_id, run_dir = make_run()
    body = "Düsseldorf – 完了 – für Köln\n"
    _write_log(run_dir, body)

    out, offset = "", 0
    while offset < len(body.encode("utf-8")):
        chunk, offset = run_manager.read_log_chunk(run_id, offset=offset, max_bytes=max_bytes)
        out += chunk

    assert out == body, "chunk boundary corrupted a multi-byte character"
```

**Fix — code.** Retreat the boundary to the last complete codepoint and report *that* as
`next_offset`, so the remainder is delivered by the following poll:

```python
data = f.read(read_upto - offset)
# Never split a codepoint: back off to the last complete one and let the
# next poll pick up the remainder. Only when more bytes actually follow --
# at EOF a truncated tail is genuinely truncated and `replace` is correct.
if read_upto < size:
    while data and (data[-1] & 0xC0) == 0x80:
        data = data[:-1]
    if data and (data[-1] & 0x80):
        data = data[:-1]
    read_upto = offset + len(data)
chunk = data.decode("utf-8", errors="replace")
```

Then `next_offset` is always a codepoint boundary and the protocol is lossless. Note this
also makes `test_max_bytes_is_capped`'s `len(chunk) == LOG_CHUNK_MAX_BYTES` correct only
for ASCII — assert `<=` there, or assert on the byte length.

**Escalation note.** Per `CHAT-CHECKPOINTS.md` §Unplanned escalation, "gate green, outcome
visibly wrong — close it with a new executable check, not a note." That is what the test
above is. The code fix is a behaviour change to a must-never-break contract and wants its
own test-first commit, not a Slice 5 rider.

---

## B5 — Cross-tenant isolation is asserted for 3 of 24 protected routes

`tests/conftest.py` defines exactly one user fixture. No second-user fixture exists
anywhere under `tests/unit` or `tests/contracts` (`test_db_portability.py` builds `User`
rows, but for DDL round-trips, not for authorisation).

Ownership is therefore tested only where a *fake* user id can be forged on the filesystem:

```python
run_id, _ = make_run(user_id="someone-else")   # a string, not a User row
```

That covers `/api/run_status`, `/api/run_logs`, `/api/run_summary` — three routes whose
handlers compare `status["user_id"]` to `user.id` and 404. Good tests; the
404-not-403 reasoning is right.

Every DB-backed protected route is untested for cross-tenant access:
`/api/my/resume/{resume_id}`, `/api/my/resume/{resume_id}/activate`,
`/api/my/profile/{key}` (GET/POST/DELETE), `/api/my/profile/{profile_key}/latest`,
`/api/my/profile/{profile_key}/url_pool/prune_stepstone`,
`/api/run_artifacts/{run_id}/potential_applications{,/{job_key}}`.

The 401 sweep makes this hole hard to see. It is exhaustive by construction and covers all
24 protected routes, so the auth story *reads* as complete — but "rejects a stranger" and
"rejects a logged-in stranger" are different properties, and only the first is checked.
This is the CP-1 question "routes silently missing from the sweep" with a different
answer than expected: none are missing from the *inventory*, but the sweep tests a weaker
property than its coverage implies.

It also matters for CP-3 agenda item 3. Deciding which of the 18 public routes should stay
public is a smaller question than whether the 24 protected ones are scoped to their owner,
and right now the suite cannot answer the second.

**Fix.** Add an `other_user` fixture and a parametrised sweep:

```python
@pytest.fixture
def other_user(db_session):
    """A second persisted user. 401 and 404-for-a-stranger are different contracts."""
    import uuid
    from datetime import UTC, datetime
    from app.db.models import User

    user = User(
        id=uuid.uuid4(),
        email="other@example.com",
        password_hash="not-a-real-hash",
        created_at=datetime.now(UTC),
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user
```

...then, for each DB-backed resource, create it as `other_user` and assert the `client`
(authenticated as `test_user`) gets 404. If any return 200, that is a live
authorisation bug and an escalation, not a test gap.

---

## B6 — The accept threshold exists in three unlinked places

`test_scoring_invariants.py` declares:

```python
# The only absolute allowed here. This is a product decision, not a tuning artefact.
ACCEPT_THRESHOLD = 70
```

The reasoning is right. The implementation makes it a **duplicate of** the production
constant rather than an assertion **about** it:

| Location | Value |
| --- | --- |
| `tests/unit/test_scoring_invariants.py:26` | `ACCEPT_THRESHOLD = 70` |
| `app/pipeline/output.py:22` | `if score >= 70:` |
| `app/pipeline/potential_bucket.py:34-35` | `final_cutoff: float = 70.0, llm_cutoff: float = 70.0` |

Change `output.py` to 75 and the accept/reject bucketing — the product's actual output —
changes while all 208 tests stay green. That is the F2 failure mode the whole file was
written to eliminate, one level up: not a test bound to a config value, but a test bound
to a *copy* of one.

The same pattern appears in the log-chunk cap:

| Location | Value |
| --- | --- |
| `app/gui_runs/run_manager.py:15` | `LOG_CHUNK_MAX_BYTES = 64 * 1024` |
| `app/fastapi_run.py:1832` | `max_bytes = min(max_bytes, 64 * 1024)` |

`test_max_bytes_is_capped` asserts against the named constant at the function level. The
HTTP layer's literal is unchecked, so the two can diverge silently.

**Fix.** Promote the threshold to a single named constant the app owns, import it in the
test, and assert the relation rather than the number:

```python
from app.pipeline.output import ACCEPT_THRESHOLD   # or app.config
...
assert blocked["score"] < ACCEPT_THRESHOLD
```

Do the same for `LOG_CHUNK_MAX_BYTES` in `fastapi_run.py`. Both are one-line changes and
both convert an untestable duplication into a testable one. If promoting the constant is
Slice-scope creep, the minimum acceptable alternative is an executable check that the
three copies agree.

---

## Should fix

### S1 — Eight assertions that cannot fail

```python
assert response.status_code in (200, 404)                 # start_batch_run
assert response.status_code in (200, 400, 404, 422)       # run_single
assert response.status_code in (200, 422, 500)            # search_stepstone
assert response.status_code in (200, 400, 422)            # search_stepstone_list, job_details, bundle
assert response.status_code in (200, 400, 422, 404)       # aggregate_report
```

These pin nothing. A route that starts 404ing because Slice 7 mis-wired its router passes
`in (200, 404)`. `search_stepstone` accepts **500** — the suite explicitly permits an
unhandled server error on a route that is both public and network-facing.

`test_run_single_is_reachable_and_validates_its_body` is the honest one: `!= 500` is a
real, if weak, assertion. The rest should follow that pattern or commit to a single code.

These are the routes where "reachable" is genuinely all that can be checked offline, which
is fair — but then the tests should say `!= 404 and != 500` rather than enumerating an
accept set broad enough to include the failure.

### S2 — `test_status_write_is_atomic` cannot observe non-atomicity

```python
for _ in range(20):
    run_manager.write_status(run_id, ...)
    loaded = json.loads((run_dir / "status.json").read_text(encoding="utf-8"))
    assert loaded["run_id"] == run_id, "observed a partially-written status file"
```

Single-threaded: the read happens after `write_status` returns. A naive
`open(path, "w").write(...)` implementation passes this 20/20, every time. The property —
a concurrent reader never sees a truncated file — needs either a reader thread racing a
writer loop, or (better, and deterministic) an assertion about the mechanism:

```python
def test_status_write_never_creates_a_partial_file_under_the_real_name(...):
    """No writer ever opens status.json for truncating write; it renames into place."""
    # assert the temp-then-rename path, e.g. by observing that a sibling temp file
    # appears and no truncation of the target occurs -- os.replace is atomic on both
    # POSIX and Windows, which is the actual guarantee being relied on.
```

`atomic_write_json` → `atomic_write_text` does do the right thing today. The test just
does not know that.

### S3 — The "no writes outside tmp_path" test asserts the opposite

```python
def test_uploads_stay_inside_the_redirected_output_root(client, run_output_root) -> None:
    """No writes outside `tmp_path` — TEST-STRATEGY §6."""
    _upload(client)
    written = list(run_output_root.rglob("*"))
    assert written, "expected the upload to land under the redirected output root"
```

This asserts something *was* written **inside** the root. It cannot detect a write
**outside** it — which is the stated contract, and the failure that actually happened once
(per the `run_output_root` docstring: "which is exactly what happened the first time this
fixture was written").

**Fix.** Snapshot the repo's real `output/` mtime, or assert on the resolved path of the
stored résumé:

```python
resume_id = _upload(client).json()["resume_id"]
detail = client.get(f"/api/my/resume/{resume_id}").json()
stored = Path(detail["path"]).resolve()
assert stored.is_relative_to(run_output_root.resolve()), f"escaped tmp_path: {stored}"
```

### S4 — The dedup test cannot distinguish sha256 from filename

```python
first = _upload(client).json()["resume_id"]
second = _upload(client).json()["resume_id"]
assert first == second
```

Both calls use `_upload`'s defaults, so name **and** bytes are identical. The docstring
claims "Deduplicated by sha256". A filename-keyed implementation passes identically.

**Fix.** Same bytes, different name — and the converse:

```python
a = _upload(client, name="cv.txt",     body=b"Same bytes.").json()["resume_id"]
b = _upload(client, name="cv-v2.txt",  body=b"Same bytes.").json()["resume_id"]
c = _upload(client, name="cv.txt",     body=b"Different bytes.").json()["resume_id"]
assert a == b, "dedup must key on content, not filename"
assert a != c, "different content must not collapse onto one resume"
```

### S5 — A magic 35 in a file that forbids magic numbers

```python
def test_blockers_still_dominate_when_the_llm_is_mocked(...):
    assert blocked["score"] <= 35
```

35 is `profile_factory`'s `blocker_cap_hard`. The file's own rules say relational only,
and `test_the_cap_binds_regardless_of_how_good_the_rest_is` gets this right two functions
earlier:

```python
assert strong["score"] <= blocking.blocker_cap_hard
```

Same property, two spellings, one of which breaks if the fixture default is tuned.

**Fix.** Bind the profile to a name and assert against it:

```python
focus = profile_factory(relocation_ok=False)
blocked = score_job(..., focus, use_llm_scoring=True)
assert blocked["score"] <= focus.blocker_cap_hard
```

### S6 — Staged parse coverage that was never wired up

`conftest.py` defines `job_html` — "Raw HTML fixture, for the one place parsing itself is
under test." **No test uses it.** `tests/fixtures/jobs/job_stepstone_1.html` is read by
nothing; `tests/fixtures/profiles/` is empty. Unused fixtures in `conftest.py`:
`job_html`, `db_engine`, `sqlite_url`, `file_profile_store` (the last three are legitimate
— they are consumed transitively by `db_session` and `client`; `job_html` is not).

`app/pipeline/parsers.py` therefore has zero coverage in the new suite. Combined with the
missing `tests/integration/test_pipeline_offline.py`, the parse → score → artifact path is
covered at exactly one of its three stages.

Either wire it up or delete the fixture and the HTML file. Dead scaffolding in `conftest.py`
reads as coverage that does not exist — which is the same failure as B1–B6, in a cheaper
form.

---

## Latent — cheap now, expensive after Slice 7

**L1 — `_iter_routes` yields one method per route object.**

```python
yield sorted(methods - {"HEAD", "OPTIONS"})[0], path, route
```

A route declared `@app.api_route(path, methods=["GET", "POST"])` contributes only `GET` to
both the inventory and the 401 sweep; the `POST` is silently outside the gate. No such
route exists today (verified — no `api_route` or `methods=[` usage), so this is latent.
It is also exactly the "silently shrinking parametrisation" the module docstring says the
declared list exists to prevent. Yield one entry per method.

**L2 — `Mount` objects are skipped without comment.** `if not methods: continue` silently
drops anything without a `.methods` attribute, which includes `StaticFiles` mounts and
sub-applications. None exist today; Phase 5 will add at least one. A mounted static tree is
unauthenticated by default and would never appear in the inventory. An explicit
`isinstance(route, Mount)` branch that fails loudly is a two-line change now.

**L3 — Two `get_current_user` paths are untested.** The dependency has four outcomes; the
sweep covers two:

| Path | Covered |
| --- | --- |
| no token → 401 | ✅ `test_protected_routes_require_authentication` |
| undecodable token → 401 | ✅ `test_protected_routes_reject_a_garbage_token` |
| valid signed token, user row deleted → 401 | ❌ |
| transient DB error → 503 | ❌ |

The third is the security-relevant one — a correctly signed token outliving its user. It
needs a real token, so it does not fit the `dependency_overrides` pattern the `client`
fixture uses; one dedicated test with a genuine `/auth/login` round-trip followed by a
user delete covers it.

**L4 — `test_db_portability.py` tests `Base.metadata`, not the migration chain.** It
compiles DDL from the ORM models across all three dialects, which is the right check for
A6/A8 as written. But the deployed schema comes from `alembic upgrade head`, not
`create_all`. Model/migration drift — a column added to `models.py` without a revision —
is invisible to this file **and** to every DB-backed contract test, because
`db_engine` builds its schema with `Base.metadata.create_all`.

That is a defensible scoping decision for Slice 1 (a migration test needs `upgrade head`
against a temp DB, which is slower and closer to `external`). It should be a recorded
decision rather than an implicit one, because "migrations are tested" is what the file's
title implies. `STATE.md` notes both dialects run `upgrade head` clean *manually, on a real
container* — that verification is not in the gate.

---

## Already flagged, restated with a judgement

- **`tests/integration/test_pipeline_offline.py` missing; `pipeline/pipeline.py` at 22%.**
  Agreed, and it is the largest single gap — but it is a *visible* one, which is why it
  ranks below B1–B6. Note the interaction with **S6**: `parsers.py` is also uncovered, so
  the gap is wider than the 22% figure suggests. Recommend writing the integration test
  before Slice 3 rather than accepting 22%, on the grounds that Slice 3 moves
  `stepstone/` into `sources/` and this is the only test that would notice if the
  parse → score handoff broke during the move.

- **A11 (`score_job` mutates the caller's dict).** The determinism invariant does **not**
  catch it — see B1. The characterisation test that does catch it understates the surface
  — see B3. Net: A11 is currently *under*-characterised, not over-.

- **A12 (18 of 42 unauthenticated, pinned as current contract).** The pinning mechanism is
  good: `PUBLIC_ROUTES` as a literal compared against the live app catches drift in both
  directions, and `test_public_routes_do_not_401` documents the reasoning without
  endorsing it. The `42 ≠ 38` reconciliation in the module docstring is correct (38 in
  `fastapi_run.py` + 4 from `auth_routes.py`; verified by decorator count). No change
  needed. The real auth gap is B5, which is about the other 24.

---

## Exit criteria

Not trustworthy as-is. Trustworthy after **B1–B6**.

B1, B2, B3, B5, B6 are test-side and can land as one commit against `tests/` — none of
them touch `app/`, so they do not collide with Slice 2's ruff pass on `slice/02`.

**B4 is different and should not ride along.** The test is test-side; the fix in
`read_log_chunk` is a behaviour change to a contract `AGENTS.md` names as
must-never-break. Test-first, own commit, own branch — same treatment A1 gets.

Suggested ordering:

1. **B4 test only**, committed red, on `fix/log-chunk-utf8`. Proves the bug from the gate.
2. **B4 code fix** on the same branch. Gate green again.
3. **B1, B2, B3, B6** — one commit, `tests/` plus two constant promotions in `app/`.
4. **B5** — `other_user` fixture plus the cross-tenant sweep. If any route returns 200,
   stop: that is a live authorisation bug and an escalation, not a test fix.
5. **S1–S6** before Slice 5.
6. **L1–L4** before Slice 7.

`ci/baseline.json` moves down, not up: B4's test and B5's sweep add tests, so the pytest
count rises, which is the ratchet moving in the permitted direction.
