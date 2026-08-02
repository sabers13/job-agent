# CP-1 — Oracle review

_First pass 2026-08-01, against `8b0116e` (Slice 1 green: 208 passed, 0 failed, 0 skipped)._
_Second pass 2026-08-02, against `db3b908` (291 passed, 0 failed, 1 deselected)._

**First-pass verdict: not trustworthy as-is.** Fix list below (CP1-1 … CP1-7, all closed).

**Second-pass verdict: trustworthy as an oracle, with one blocking exception — CP1-8.**
**CP1-8 was closed on 2026-08-02** (test-side only, 291 → 298 passing), so **Slice 3 is no
longer blocked by it** — CP‑3 and the liveness audit still precede it, and S1's remaining
seven are still scheduled ahead of it. Slice 2 resumes. See
[§Second pass](#second-pass--2026-08-02) at the foot of this file for what was re-checked,
and the three new **S** items it produced alongside.

> **Amended 2026-08-02, after the verdict was first given.** The initial second-pass
> verdict was a clean "trustworthy", with S1 merely *scheduled* ahead of Slice 3 on a
> documentation-consistency argument. That was wrong, and the argument that corrected it
> was structural: **Slice 3 is the slice that moves `app/stepstone/`, and the only test
> covering `/search_stepstone` accepts the failure state of that relocation.** Following
> that up showed the accept set is not the whole problem — the test's stubs do not bind,
> so it reaches the live network. Promoted to **CP1-8** on the CP1-7 precedent: "wide
> accept set" and "wide accept set that hid a real defect" are not the same severity.

> **Numbering.** The blocking items are **CP1-1 … CP1-7**. They were originally written as
> a bare `B1`–`B7`, which collided with `backlog.md` bucket **B** — two unrelated sequences
> in which `B4` meant the log-chunk UTF-8 bug in one and "promote `_now_iso`" in the other.
> A convention ("always write the prefix") is not a fix, because a brief that drops the
> prefix once points a cold Codex run at the wrong task. Renumbered mechanically on
> 2026-08-02; every reference in `docs/`, `ci/` and `tasks/` moved with it. The `S`
> (should-fix) and `L` (latent) items keep their numbers — they collide with nothing.

The structural work is sound — the route inventory derives from the live app, the
`DEFAULT_FOCUS` guard is executable rather than conventional, the scoring invariants are
mostly relational, and the fixture comments record real traps instead of restating the
code. None of that is in question.

What fails the CP-1 bar is narrower and worse: **seven places where a test's name and
docstring claim a property the test cannot fail to detect the absence of.** An oracle
that is merely incomplete is safe — the gap is visible as low coverage. An oracle that
asserts a property it does not check is not, because it spends the reviewer's attention
and returns nothing. All seven read as covered.

Two of the seven are not test defects but live bugs, each found by following the test
that claims to pin it:

- **CP1-4** — data corruption in the log-streaming contract. Fixed test-first (`ea93e34`
  red, `a539f56` green).
- **CP1-7** — a live SQL Server connection inside the offline suite, concealed by an
  assertion accepting both outcomes. Environment half fixed (`bf88f64`); the assertion
  is still open. Originally filed under S1 and promoted, because "wide accept set" and
  "wide accept set that hid a real defect" are not the same severity.

Severities: **B** blocks Slice 3. **S** should land before Slice 5 (the first slice that
moves run-lifecycle code). **L** is hardening — latent today, cheap now, expensive after
the routers move.

> **Amended 2026-08-02 (second pass).** One instance of S1 is promoted to **CP1-8** and
> blocks Slice 3; the **remaining seven move to _before Slice 3_** as well.
>
> The promotion is positional, not stylistic. **Slice 3 moves `app/stepstone/` into
> `sources/`, and the one test covering `/search_stepstone` accepts `500` — the exact
> failure state that relocation produces at runtime.** The hole sits precisely where the
> next structural change lands. Everything else on the S list can wait for Slice 5; this
> cannot, because Slice 5 is after the damage.
>
> The remaining seven move with it for a weaker but real reason: the CP1-7 remediation
> wrote the general lesson into `AGENTS.md` §Conventions ("No assertion may accept both
> the success and the failure state… Never write an accept set containing 500") **without
> fixing the eight instances**. A conventions file that a cold Codex run reads before
> every task, carrying a live counterexample in the same repo, teaches the
> counterexample. See [§CP1-8](#cp1-8--the-stepstone-route-test-stubs-nothing-and-reaches-the-live-network)
> and [§S1](#s1--eight-assertions-that-cannot-fail).

---

## CP1-1 — The determinism invariant cannot detect A11, by construction

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

## CP1-2 — The profile-mutation assertion is a no-op

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

## CP1-3 — The A11 characterisation is shallow, and misses a real second mutation

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

That test is the one that should own the mutation-tolerance property. Once it exists, CP1-1's
fix is free of risk.

---

## CP1-4 — Live bug: log streaming corrupts multi-byte characters at chunk boundaries

> ✅ **FIXED** — `ea93e34` (test, red) then `a539f56` (code, green). The analysis below
> stands; **the suggested code fix does not** — see the note in §Exit criteria. It stalls
> the offset when `max_bytes` is narrower than the character, trading corruption for a
> poll loop that never terminates.

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

## CP1-5 — Cross-tenant isolation is asserted for 3 of 24 protected routes

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

## CP1-6 — The accept threshold exists in three unlinked places

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
| `app/pipeline/output.py:78` | `decide_potential(merged, final_cutoff=70.0, llm_cutoff=70.0)` |
| `app/pipeline/potential_bucket.py:34-35` | `final_cutoff: float = 70.0, llm_cutoff: float = 70.0` |

**Corrected 2026-08-02: three production copies, not two.** The row this table originally
missed is `output.py:78`, and it is the worst of the three — it passes `70.0` explicitly as
`decide_potential`'s `final_cutoff` and `llm_cutoff`, so it *overrides* the defaults on
lines 34-35 rather than inheriting them. Fixing only `potential_bucket.py` would therefore
have changed nothing at the one call site that matters, while looking like a complete fix.

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

**Two 70s that are deliberately *not* folded in** (checked 2026-08-02, left alone):

- `app/config/settings.py:63` — `score_keep_threshold`, default `70`, overridable via
  `JOBAGENT_SCORE_KEEP_THRESHOLD`. A separate, user-tunable knob read by `prefect_run.py`,
  `fastapi_run.py:525` and `output.py:135`. Collapsing it into `ACCEPT_THRESHOLD` would
  make a configurable value constant — a behaviour change, not a deduplication. Whether
  these two 70s *should* be one number is a product question for CP-3.
- `app/fastapi_run.py:2003` — `"final<70 and llm>70"`, a human-readable fallback string in
  a response body, not a threshold. Interpolating the constant into it is cosmetic and
  would widen this commit's blast radius into `fastapi_run.py` for no testable gain.

---

## CP1-7 — `test_health_db_reports_reachability` hid a live database connection

_Promoted out of S1 after the fact. Filed there as one of eight wide accept sets; it
turned out to be the assertion that concealed CP1-4's sibling finding, so it blocks Slice 3
with the rest of the B list rather than waiting for Slice 5._

`tests/contracts/test_health_and_pages_api.py`

```python
def test_health_db_reports_reachability(client_unauthed) -> None:
    """SQLite is reachable in tests, so this is the healthy path."""
    response = client_unauthed.get("/health/db")

    assert response.status_code in (200, 503), response.text
```

The docstring asserts a fact about the environment. The code asserts nothing: 200 and
503 are the only two codes the route can return, so the test passes unconditionally.

That gap is what made the environment leak invisible. `tests/conftest.py` seeded its
variables with `os.environ.setdefault`, so a sourced `.env.dev` — the shell AGENTS.md
§Commands documents as normal — kept its real `mssql+pyodbc` URL. `app/db/health.py`
binds `SessionLocal` at import, so `db_engine`'s monkeypatch cannot reach `check_db`,
and `check_db` is what `TestClient`'s lifespan calls. The "offline, no DB container"
suite therefore opened a live connection to the developer's SQL Server on every client
fixture, and **this test graded it green** — the accept set was wide enough to swallow
both the healthy SQLite path it claims to describe and a real database it never
mentions.

Measured: `check_db()` returned `ok=True` in 0.31s against a running container, and all
208 tests passed against mssql. With the container down the run blocks on the ODBC login
timeout instead. Same defect, two presentations, neither visible in the result.

The environment half is fixed (`bf88f64` — unconditional assignment plus
`tests/test_suite_hermeticity.py`, which asserts on the engine the app actually reaches
rather than on the strings conftest wrote). Backlog **A13** records why no fixture could
have fixed it. This item is the remaining half: the assertion that let it pass.

**Fix.** The suite now guarantees SQLite, so the docstring's claim is true and can be
asserted:

```python
def test_health_db_reports_reachability(client_unauthed) -> None:
    """The suite pins SQLite (tests/test_suite_hermeticity.py), so this is the healthy path."""
    response = client_unauthed.get("/health/db")

    assert response.status_code == 200, response.text
    assert response.json()["ok"] is True
```

If that ever fails, the DB the suite reaches is not the one it pinned — which is exactly
the signal that was missing.

**Note the general lesson, now in AGENTS.md §Conventions:** no assertion may accept both
the success and the failure state. CP1-1, CP1-2 and this item are three instances of one
failure mode, and it is the mode that produced this review's verdict.

---

## CP1-8 — The StepStone route test stubs nothing and reaches the live network

_Found at the second pass, 2026-08-02. Promoted out of **S1** on the CP1-7 precedent: it
is not one more wide accept set, it is a wide accept set concealing a live external
connection._

> ✅ **CLOSED 2026-08-02.** Both prescribed changes landed, plus the network guard. The
> analysis below stands as written. Three things are worth carrying forward that the fix
> discovered rather than confirmed:
>
> 1. **The guard was installed first, against the unmodified suite** — deliberately, to
>    measure the leak rather than assume CP‑1 had enumerated it. It reddened **exactly
>    two** tests, both named here. The blast radius was as described.
> 2. **Raising is not sufficient; the refusal has to be recorded.** asyncio resolves DNS
>    on an executor thread, so this route's leak raises where nothing is listening, and
>    the handler's `except Exception` absorbs whatever does reach it. `conftest` checks
>    the guard's record at the end of each phase instead — and in the **call** phase, so a
>    leaking test moves `pytest_passed` rather than sitting beside it as a teardown error.
> 3. **`BaseException` was tried and rejected.** It defeats `except Exception:` and it
>    also kills the anyio portal `TestClient` runs on, turning each leak into a red test
>    plus `RuntimeError: This portal is not running`. Reasoning in `tests/net_guard.py`.
>
> The generalisable rule is now in `AGENTS.md` §Conventions: `raising=False` is forbidden
> in a stub unless it is deliberately creating an attribute, and then the creation must be
> asserted.

`tests/contracts/test_health_and_pages_api.py:151`

```python
def test_search_stepstone_returns_the_adapter_result(client_unauthed, monkeypatch) -> None:
    import app.fastapi_run as fr

    monkeypatch.setattr(fr, "search_stepstone_http", lambda *a, **k: {...}, raising=False)
    monkeypatch.setattr(fr, "search_stepstone",      lambda *a, **k: {...}, raising=False)

    response = client_unauthed.get("/search_stepstone", params={"what": "data analyst"})

    assert response.status_code in (200, 422, 500), response.status_code
```

**Neither monkeypatch binds to anything the handler calls.** Measured:

| Name patched | What it actually is |
| --- | --- |
| `fr.search_stepstone_http` | **Does not exist.** `fastapi_run.py:79` imports it *as* `crawl_http`. `raising=False` swallows the `AttributeError` and creates a fresh attribute nothing reads. |
| `fr.search_stepstone` | The **route handler itself** (`async def search_stepstone`, line 385). FastAPI captured the function object at decoration time, so rebinding the module attribute changes nothing. |
| `fr.ss_search` — the actual callee | **Not patched.** `fastapi_run.py:81`: `from .stepstone.smoke import search_stepstone as ss_search`. |

So the test exercises the real adapter. Instrumented at `app.fetching.http_client.fetch`,
reproducing the test's patches exactly:

```
status: 200
outbound fetch calls: 1  ['https://www.stepstone.de/en/']
```

**A live HTTP request to stepstone.de, from inside the gated suite.** `conftest.py`'s
opening docstring says "Offline and deterministic. No live network, no DB container, no
Playwright, no LLM." `tests/test_suite_hermeticity.py` — the file written to make that
claim executable — asserts over environment variables, the database URL and the settings
flags. **It has no network assertion at all.**

The accept set exists to accommodate this. The handler wraps everything in
`except Exception: raise HTTPException(500)` (`fastapi_run.py:397-399`), so:

- **with** network egress, the fetch succeeds and the route returns `200`;
- **without** it, the fetch raises and the route returns `500`.

`in (200, 422, 500)` admits both. The test therefore passes identically whether or not the
machine can reach the internet — which is the same machine-dependent, result-invariant
shape as CP1-7, one layer out. `test_public_routes_do_not_401` hits the same route through
`sample_path` and asserts only `!= 401`, so it makes the request too.

**Why this blocks Slice 3 specifically.** Slice 3 moves `app/stepstone/` into `sources/`
behind an adapter interface, and backlog **D3** makes `stepstone/smoke.py` — the module
`ss_search` resolves to — a deletion candidate at CP-3. The route's only test accepts the
runtime failure state of exactly that work:

- A module-scope `ImportError` is the *benign* case: `fastapi_run.py:79-81` imports at
  module level, so a broken import fails collection loudly. That one is caught by accident.
- Everything else is not. A shim that resolves but returns a different shape, a changed
  signature, D3's deletion forcing a repoint at `search_http`/`search_playwright` — each
  lands in the handler's broad `except Exception` and becomes a `500` this test accepts.

**Fix.** Two changes, both small:

1. **Patch the name the handler calls.** `monkeypatch.setattr(fr, "ss_search", ...)`, with
   `raising=True` — the default — so the patch fails loudly when Slice 3 renames or
   relocates the symbol. `raising=False` is what let this rot silently; it should not
   appear in a stub whose whole job is to bind to a real name.
2. **Assert one code, and assert the body.** With the adapter genuinely stubbed the
   response is determinable: `== 200` and `response.json() == {"results": [], "count": 0}`.
   The test's name promises "returns the adapter result"; that is the assertion that
   delivers it.

Then extend `tests/test_suite_hermeticity.py` with a network guard — a session-scoped
`socket.socket.connect` block, or an autouse fixture failing on outbound connections — so
the next inert stub is caught by the suite rather than by a reviewer. That last part is
the CP1-7 lesson applied one layer out: the environment leak was fixed for the database
and left open for the network.

---

## Should fix

### S1 — Eight assertions that cannot fail

> ⚠️ **Amended 2026-08-02 by the second pass. One of the eight is now
> [CP1-8](#cp1-8--the-stepstone-route-test-stubs-nothing-and-reaches-the-live-network) and
> blocks Slice 3; the remaining seven move to _before Slice 3_ too.**
>
> The `search_stepstone` entry was the worst of the eight and turned out not to belong on
> this list at all — its stubs do not bind, so it reaches the live network and the accept
> set exists to absorb the difference between a machine that has egress and one that does
> not. That is CP1-7's shape, so it takes CP1-7's severity.
>
> The other seven move on a weaker argument: not that the tests got worse, but that
> `AGENTS.md` got stronger while they stayed the same. The CP1-7 remediation wrote the
> general rule into §Conventions and left the eight instances in the gated suite, so the
> conventions file and the oracle now disagree — in the one document every cold Codex run
> reads first. Fix them, or the rule is decoration.
>
> Measured while re-checking, so the fix does not over-claim: `/health/config` at
> `test_health_and_pages_api.py:28` is the mildest of the set and **not** vacuous —
> `health_config()` can return 200, 500 **or** 503 (`fastapi_run.py:209-213`), so
> `in (200, 503)` does still exclude one outcome. Unlike CP1-7's `/health/db`, which could
> return only the two codes it accepted.

_`test_health_db_reports_reachability` was one of these. It is now **CP1-7** above._

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

### S6 — Staged parse coverage that was never wired up ✅ CLOSED 2026-08-02

> **Closed by `tests/integration/test_pipeline_offline.py`.** `job_html` and
> `job_stepstone_1.html` are now read by §1 of that file, which takes `parsers.py` from
> zero coverage. The fixture was kept rather than deleted — it earns its place as the one
> real-world sample; the degenerate shapes (no ld+json, malformed JSON, `@graph` nesting,
> escaped JSON, `jobLocation` lists) are built inline in the test, where a two-line HTML
> string is more legible than a file. `tests/fixtures/profiles/` is still empty and stays
> that way: `profile_factory` is the better answer, per TEST-STRATEGY §6.



`conftest.py` defines `job_html` — "Raw HTML fixture, for the one place parsing itself is
under test." **No test uses it.** `tests/fixtures/jobs/job_stepstone_1.html` is read by
nothing; `tests/fixtures/profiles/` is empty. Unused fixtures in `conftest.py`:
`job_html`, `db_engine`, `sqlite_url`, `file_profile_store` (the last three are legitimate
— they are consumed transitively by `db_session` and `client`; `job_html` is not).

`app/pipeline/parsers.py` therefore has zero coverage in the new suite. Combined with the
missing `tests/integration/test_pipeline_offline.py`, the parse → score → artifact path is
covered at exactly one of its three stages.

Either wire it up or delete the fixture and the HTML file. Dead scaffolding in `conftest.py`
reads as coverage that does not exist — which is the same failure as CP1-1 … CP1-6, in a
cheaper form.

---

### S7 — ADR 0009's soft limit is a decision no test can defend _(new, second pass)_

`read_log_chunk` may exceed `max_bytes`, but **never by more than 3 bytes**. That bound is
the entire justification for accepting a soft limit at all — ADR 0009 argues
`LOG_CHUNK_MAX_BYTES` "remains a hard *safety* cap on memory per request. Three bytes of
slack does not weaken it." Nothing asserts it. Measured:

```python
# app/gui_runs/run_manager.py, the extend-to-complete-the-character branch
data += f.read(min(needed, size - read_upto))        # → min(needed + 64, ...)
```

Chunks then overshoot by 64 bytes instead of 3 and **all 291 tests stay green**.

The ADR anticipated a test here and guarded it in prose — *"Do not 'tighten' this to
`assert len(chunk) <= max_bytes`. The assertion looks correct and would be reintroducing
the bug."* That is exactly the shape `CHAT-CHECKPOINTS.md` §Unplanned escalation tells us
not to leave as prose: *"Gate green, outcome visibly wrong — close it with a new
executable check, not a note."* The note is right; it is just not executable.

Two related weaknesses in the same file:

- `test_http_honours_the_same_cap_as_the_function` asserts
  `<= LOG_CHUNK_MAX_BYTES + 3` against an **all-ASCII** body, so the `+ 3` is decorative —
  the assertion passes identically at `+ 0`. The slack is never exercised where it is
  asserted.
- ADR 0009 claims verification "over 300 random mixed-width bodies at every size from 1 to
  20, plus four malformed inputs." **That verification is not in the suite.** It was
  reproduced at the second pass and it holds — 400 bodies × sizes 1–20, lossless, always
  progressing, max overshoot exactly 3; six malformed inputs all drain — but Slice 5 moves
  this module and inherits none of it.

**Fix.** Port the randomised walk in as a test, assert the bound in both directions (a
chunk never exceeds `max_bytes + 3`, **and** a chunk containing a wide character at a
narrow `max_bytes` does exceed `max_bytes`), and put a 4-byte character in
`MULTIBYTE_LOG_BODY` — its own comment says "an emoji would be 4" and then does not use
one. The 4-byte path is handled correctly (verified), so this is closing the gap between
what the fixture names and what it constructs, not chasing a bug.

### S8 — A14 is pinned at one of its two call sites _(new, second pass)_

`test_staleness_raises_when_only_one_side_carries_a_timezone` pins
`pipeline.py:158`. The same comparison at `pipeline.py:69` — inside the cache
short-circuit — is documented in backlog **A14** and pinned by nothing.

Its symptom is different, and worse for an oracle: the `TypeError` is swallowed by the
surrounding broad `except Exception`, logged as `"cache_get failed; continuing without
cache"` — which is **false**, `cache_get` succeeded — and the call then does a full
refetch. Measured: 1 fetch to warm the cache, **2** after a cached call carrying a
`Z`-suffixed cutoff. So a mismatched cutoff turns every cache hit into a network fetch,
silently, and the log line blames the wrong function. Textbook AGENTS.md §Conventions
"no silent failure."

Backlog A14 prescribes normalisation in `_parse_iso8601`, which repairs both sites at once
and makes the single pin sufficient. **Nothing enforces that fix shape.** A `try/except`
at `:158` would satisfy the pin, go green, and leave the cache path exactly as it is.

**Fix.** One test: warm the cache, call again with an aware `cutoff_iso`, assert the
fetch count did not increase. It fails today for the right reason and keeps failing until
the normalisation fix lands.

### S9 — A15's pin has an unguarded premise _(new, second pass)_

`test_the_candidate_german_level_does_not_move_the_heuristic_score` asserts that six CEFR
levels produce one distinct score. Its meaning depends on the fixture posting actually
producing a German-language penalty — otherwise "all six are equal" is true for a reason
that has nothing to do with A15.

Nothing checks that premise. Measured against `tests/unit` + `tests/integration`:

| Mutation to `app/pipeline/scoring.py` | Result |
| --- | --- |
| `if penalty_key in _LANG_PENALTY:` → `if False:` (German penalty deleted outright) | **107 passed** |
| `english_bonus = 10` → `english_bonus = 0` | **107 passed** |
| customer-facing `penalty -= 5` → `penalty -= 0` | **107 passed** |

The language component is a scored component (`components["language"]`,
`english_ok`, `german_requirement`) **and** a blocker input, and the oracle contains no
invariant over it. `TEST-STRATEGY` §5.1 lists eight invariant families; language is not
one of them. So A15's characterisation would keep passing after the behaviour it
characterises had been removed.

This is a coverage gap, not a lying assertion — it does not meet the CP-1 bar and does not
block. It is filed because it is the one pinned-broken test in the suite that can go
quietly vacuous, and because the same three mutations show the gap is wider than A15.

**Fix.** One relational language invariant in `test_scoring_invariants.py` — a posting
stating a German requirement scores below an otherwise identical posting that does not —
and a line in the A15 test asserting the German penalty component is non-zero, so the pin
cannot outlive its premise.

Minor, same item: A14's `pytest.raises` shape self-documents as a bug pin; A15's **name**
reads as a specification. The §5 section comment, the docstring and the failure message
(`"A15 appears fixed — scores now differ"`) all mark it as characterisation, which is
adequate in place — but the name alone does not survive being grepped out of context.
Worth a naming convention for §5 pins if one is ever adopted; not worth a rename on its
own.

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
  ✅ **Written 2026-08-02.** The recommendation — write it rather than accept 22% — was
  taken. 42 tests across parse, parse → score, fetch → parse → score, and score → artifact.

  The recommendation's stated justification held up under contact: the parse → score
  handoff is genuinely the seam nothing else covers, because every scoring test builds its
  job with `job_factory`, which constructs a `UnifiedJobPosting` directly and parses
  nothing. Mutation-checked — deleting `description_text` from the parser's return leaves
  the entire scoring-invariant file green and turns the handoff tests red.

  Writing it also surfaced three defects the 22% figure gave no hint of, now filed as
  backlog **A14** (uncaught `TypeError` in the staleness comparison, reachable from the
  batch path), **A15** (`candidate_german_level` inert with the LLM off) and **A16**
  (`os.environ` read outranking settings). All three are pinned as-is, none fixed — which
  is TEST-STRATEGY §2.4, and the reason a characterisation pass is worth more than a
  coverage number.

- **A11 (`score_job` mutates the caller's dict).** The determinism invariant does **not**
  catch it — see CP1-1. The characterisation test that does catch it understates the surface
  — see CP1-3. Net: A11 is currently *under*-characterised, not over-.

- **A12 (18 of 42 unauthenticated, pinned as current contract).** The pinning mechanism is
  good: `PUBLIC_ROUTES` as a literal compared against the live app catches drift in both
  directions, and `test_public_routes_do_not_401` documents the reasoning without
  endorsing it. The `42 ≠ 38` reconciliation in the module docstring is correct (38 in
  `fastapi_run.py` + 4 from `auth_routes.py`; verified by decorator count). No change
  needed. The real auth gap is CP1-5, which is about the other 24.

---

## Exit criteria

Not trustworthy as-is. Trustworthy after **CP1-1 … CP1-7**.

CP1-1, CP1-2, CP1-3, CP1-6 and CP1-7 are test-side and can land as one commit against
`tests/` — only CP1-6's two constant promotions touch `app/`, so they do not collide with
Slice 2's ruff pass on `slice/02`.

**CP1-4 is different and should not ride along.** The test is test-side; the fix in
`read_log_chunk` is a behaviour change to a contract `AGENTS.md` names as
must-never-break. Test-first, own commit, own branch — same treatment A1 gets.

Ordering:

1. ✅ **CP1-4 test only**, committed red on `fix/log-chunk-utf8` (`ea93e34`, 7 failed).
   Proved the bug from the gate.
2. ✅ **CP1-4 code fix** on the same branch (`a539f56`). Gate green again.

   Note the fix sketched in §CP1-4 above is **not sufficient as written**: retreating to
   the last complete codepoint stalls the offset when `max_bytes` is narrower than the
   character, and the GUI polls that offset forever — the suggested test would hang
   rather than fail. The landed fix extends the read to complete the character instead.
   `max_bytes` is consequently a **soft** limit, by up to 3 bytes; recorded in
   [refactor-plan.md](refactor-plan.md) Slice 5 so a later test does not "correct" it.
3. ✅ **Suite hermeticity** (`bf88f64`) — not on the original list. Found while
   reproducing CP1-4's environment; it is the precondition for trusting any of these
   numbers, since the suite was grading a different database depending on the machine.
4. ✅ **CP1-5** — `other_user` fixture plus the cross-tenant sweep, in
   `tests/contracts/test_cross_tenant_isolation.py`. Run **first**, as a probe, because it
   was the one remaining item that could change what happened next. **Clean verdict: no
   DB-backed protected route serves the wrong tenant.** 15 wrong-tenant probes, all 404,
   plus 5 positive controls that seed the *same* resources for the caller and assert 200 —
   without those, a 404 from a mis-seeded fixture is indistinguishable from a 404 from a
   working ownership check, and the file would certify isolation while proving only that
   the URLs 404. Verified by mutation as well as by green: dropping the `user_id` filter
   from `get_resume_detail` turns exactly the matching probe red.

   Two findings worth carrying forward, neither a bug:

   - `POST /api/my/profile` and `POST /api/my/profile/{key}` return **200** for a key
     another user owns, correctly — they upsert a row for the *caller*, and identity is
     `(user_id, profile_key)`. A pure status-code sweep would have had to either skip them
     or assert something false; they are asserted on the stored state instead.
   - `/api/profile/{key}` GET/POST/DELETE (the file-backed store, no `/my/`) are **not**
     tenant-scoped at all — any authenticated user reads and writes the same global
     `focus_profiles.json`. Out of scope here (not DB-backed, and D1 has already decided
     the DB surface wins), but it belongs on the **CP-3** agenda beside A12.
5. ✅ **CP1-1, CP1-2, CP1-3, CP1-6, CP1-7** — one commit, `tests/` plus the constant
   promotions in `app/`. `ACCEPT_THRESHOLD` now lives in `app/pipeline/potential_bucket.py`
   (not `output.py` — `output.py` imports that module, so the reverse would be a cycle) and
   is re-exported from `app.pipeline`. All three production copies now resolve to it, and
   `fastapi_run.py`'s `64 * 1024` defers to `run_manager.LOG_CHUNK_MAX_BYTES`.
6. ✅ **`tests/integration/test_pipeline_offline.py`** — 43 tests, closing **S6** and the
   "already flagged" 22% gap in one file. Mutation-verified (29 mutations, whole suite per
   mutation, no unexpected reds, no-op control clean). Found backlog **A14**, **A15**,
   **A16** in `app/`, all pinned and none fixed — and two can't-fail assertions in its own
   first draft, both fixed and re-verified.
7. ✅ **Re-run CP-1** against the repaired suite (2026-08-02, `db3b908`). **Trustworthy,
   with one blocking exception found on the way out — CP1-8.** Slice 2 resumes now.
   Details in §Second pass below.
8. ✅ **CP1-8** — closed 2026-08-02, test-side only, no `app/` change. The stub binds to
   `ss_search` with `raising=True`; the assertion is `== 200`, the exact body, **and the
   recorded call args**, so the binding is observable rather than inferred. The network
   guard (`tests/net_guard.py` + `tests/test_suite_hermeticity.py` §3, 7 tests, 291 → 298)
   went in **first**, against the unmodified suite, and reddened exactly the two tests
   named in §CP1-8 — the leak was no wider than this review described.

   Mutation-verified, nine mutations, each reverted and checksummed byte-identical:
   uninstalling the guard, treating every host as local, unwrapping `getaddrinfo`,
   deleting `conftest`'s call hook, making the record check never raise, and treating
   loopback as remote each redden exactly their own tests, with a clean no-op control.
   Then, against the fix itself, restoring either original binding — `search_stepstone_http`
   with `raising=False`, or the route handler FastAPI captured at decoration time —
   reddens both repaired tests. So the fix is what makes them green.
9. ⬜ **S1's remaining seven** before **Slice 3**.
10. ⬜ **S2–S5, S7–S9** before Slice 5. (S6 closed.)
11. ⬜ **L1–L4** before Slice 7.

`ci/baseline.json` moves down, not up: CP1-4's test and CP1-5's sweep add tests, so the pytest
count rises, which is the ratchet moving in the permitted direction. Banked twice —
208 → 226 in `d9f4ce7`, then 226 → 248 with the batch above. The other four numbers did not
move (pyright 32, ruff 747, imports 2, failed 0), which is the expected result: CP1-6
promoted constants rather than changing behaviour.

---

## Second pass — 2026-08-02

_Against `db3b908`. **Verdict: trustworthy as an oracle, with one blocking exception
(CP1-8). Slice 2 resumes; Slice 3 waits on a one-test fix.**_

> **How this verdict changed, recorded because the process matters more than the result.**
> The pass first returned a clean "trustworthy", with S1 merely scheduled ahead of Slice 3
> on a documentation-consistency argument — `AGENTS.md` forbids a shape the suite still
> contains. That reasoning was true but weak, and it produced the wrong severity.
>
> The correction came from asking a question the review had not: **which slice moves the
> code each weak test covers?** `test_search_stepstone…` accepts `500`, and Slice 3 is the
> slice that moves `app/stepstone/`. A wide accept set is a general smell; a wide accept
> set on the one route whose module is about to be relocated is a hole positioned exactly
> where the next structural change lands. Following that up is what exposed the inert
> stubs and the live network call underneath.
>
> The generalisable lesson, and the one worth carrying into CP-3 and CP-4: **grade a weak
> test by what is about to move underneath it, not only by how weak it is.** The S/L split
> in this review is ordered by severity. It should also have been ordered by proximity to
> the next slice.

Method note, because it changes what this verdict is worth: the gate was **re-measured,
not read**. The environment was rebuilt from `requirements.lock.txt` on a clean Python
3.12 and the suite run from scratch — **291 passed, 0 failed, 1 deselected**, matching
`ci/baseline.json` exactly. CP1-1/2/3/6/7 were checked at the line each item named rather
than accepted from the commit log. Where a claim was empirical it was reproduced.

### CP1-4 / ADR 0009 — the landed behaviour is right, including the soft limit

The divergence from the first pass's suggested fix is correct and the ADR's reasoning
holds. Verified directly against `read_log_chunk`, not by reading it:

- **400 random mixed-width bodies** (1-, 2-, 3- and 4-byte characters including `😀` and
  `𝄞`) × chunk sizes 1–20: lossless reassembly, `next_offset` strictly increasing, no
  U+FFFD before EOF, **maximum overshoot exactly 3 bytes**.
- **Six malformed inputs** × five sizes: all drain, none stall.
- **The 64 KB hard cap is never exceeded, and that is structural rather than lucky.**
  `_last_sequence_start` inspects only the last four bytes, so `start == 0` — the sole
  branch that *extends* the read — is reachable only when the chunk is ≤4 bytes. At
  `LOG_CHUNK_MAX_BYTES` the trim branch always wins. ADR 0009's "three bytes of slack does
  not weaken it" is therefore **stronger than it claims**: at the cap the slack is zero.

Mutation-checked against `tests/contracts/test_log_streaming.py`:

| Mutation | Result |
| --- | --- |
| `if read_upto < size:` → `if False:` (the original CP1-4 bug) | **7 failed** |
| `if start > 0:` → `if start >= 0:` (the first pass's retreat-only fix) | **2 failed** |
| `min(max_bytes, LOG_CHUNK_MAX_BYTES)` → `... * 2` | **1 failed** |
| no-op control | clean |

The retreat-only mutant **fails rather than hangs** — the test carries a bounded loop plus
`assert next_offset > offset`, which converts a starvation defect from a CI timeout into a
gate failure with a readable message. That is the best-engineered thing in this suite and
is worth preserving verbatim through Slice 5.

Two equivalent mutants were investigated and are **not** oracle defects, recorded so the
next reader does not re-derive them:

- Narrowing `_last_sequence_start`'s lookback window from 4 to 3 bytes changes nothing for
  valid UTF-8: detecting an incomplete sequence never needs more than the last three
  bytes, because lead + three continuations is already complete.
- `_utf8_sequence_length`'s final `return 1` is **unreachable from its only call site** —
  `_last_sequence_start` returns an index whose byte is by construction not a continuation
  byte. So ADR 0009's stated mechanism for "malformed input must still advance" is not the
  mechanism that actually delivers it. The guarantee holds; the explanation is wrong.

Produced **S7**.

### A14 and A15 — both pins are the right call; A14's coverage and A15's premise are not

**A14: right, and the stronger of the two.** `pytest.raises(TypeError, match=...)` cannot
be misread as endorsement, and the parametrised companion bounds the characterisation so
the bug is not overstated as "staleness is broken." Reachability re-verified independently:
`fastapi_run.py:1124` (`_compute_cutoff_iso`) and `prefect_run.py:680` both emit
`Z`-suffixed aware cutoffs, the fixture's `datePosted` is date-only and therefore naive,
and the comparison raises. Produced **S8** — the second call site.

**A15: right in principle.** Pinning "the heuristic ignores `candidate_german_level`" as
*current* behaviour is correct. Making the field live is a scoring behaviour change that
moves every German posting at once — bucket **C**, wanting a deliberate decision about
intended semantics — and a tripwire that fires the day someone wires it up is precisely
what `TEST-STRATEGY` §2.4 asks for. The tripwire does fire: any plausible wiring compares
candidate rank against the posting's required level, which splits `A1 / B1 / B2 / C2 /
Native / Unknown` into more than one value. Produced **S9** — the premise, not the pin.

Neither pin should be converted to a fix here. Both bugs are live and both fixes are
behaviour changes that want their own commits, which is the whole point of §2.4.

### Mutation verification should be standing — but it needs a harness, not a policy

The campaign worked, and it is now a base rate rather than an anecdote: **two can't-fail
assertions in the file written to answer CP-1**, by an author who knew to look for exactly
that. Review does not catch this class. Mutation does.

The problem is that the evidence is **prose in a commit message**. The 29 mutations are
not enumerated, the harness is not in `scripts/`, and nothing is re-runnable. By this
repo's own standard that is a note where an executable check belongs, and it graded the
suite once, at `db3b908`. Slice 3 moves `stepstone/` into `sources/`; Slice 5 moves
`run_manager`. After either, the grade is stale and cannot be re-derived.

The second pass's own 12 mutations are the argument: three survivors the original campaign
did not have (the language subsystem, the ADR 0009 bound, A14's second site). That is not
a criticism of it — it was scoped to the L3 file's own claims and did that job cleanly —
it is evidence that a scoped one-off leaves everything outside its scope ungraded, which
is what "standing" fixes.

**Recommended shape:**

1. **Commit the harness as `scripts/mutate.py`, with the mutation set as data** — a list
   of `(file, anchor, replacement, expected-red tests)`. "29 mutations" becomes a
   reviewable artifact instead of a claim, and a diff to it is a reviewable change.
   **Use bytes I/O.** `db3b908`'s own commit message records that `read_text`/`write_text`
   normalised CRLF→LF and the revert assertion could not see it, both sides being
   normalised on read — a can't-fail assertion *inside the verification tool*. That is
   the failure mode recurring one level up and it is the best possible argument for
   committing the tool where it can be reviewed.
2. **Report the mutation score in `ci/baseline.json` as a reported metric, never a gate** —
   the status coverage already has under **R3**. As a ratchet key it would be gamed, by
   adding mutations that die easily; the number would become a target rather than a
   measurement. Reported, it does everything needed: it is visible when it moves.
3. **Gate it in exactly one narrow place**: a new or modified test file ships with its
   mutation set and a clean run, checked at review time rather than in CI. The failure
   mode is *a new assertion that cannot fail*, so the check belongs where new assertions
   appear — not spread across the whole suite on every commit.
4. **Re-run the set after Slices 3, 5 and 7** — the three that move code the mutations
   point at. Anchor-based mutations break when code moves, and that is a feature: a broken
   anchor demands re-aiming, where a line-number-based one would silently pass.

This is **AGENT-WORKFLOW/TEST-STRATEGY scope, not a CP-1 blocker.** It is recorded here
because CP-1 is where the evidence for it was generated, twice.

### The suite is not offline, and the file that says it is does not check

> ✅ **CLOSED 2026-08-02, and this was the more important half of CP1-8.** The fourth claim
> is now executable: `tests/net_guard.py` refuses egress suite-wide, and
> `tests/test_suite_hermeticity.py` §3 asserts the guard is installed, that `httpx` cannot
> get past it, that a refusal swallowed by a broad `except Exception:` still fails the
> test, and that `conftest`'s hooks are still wired — that last one because deleting them
> would leave every other network test green while leaks went back to invisible, which is
> this same failure repeating one level up.
>
> One boundary was chosen rather than inherited and is recorded in `net_guard.py`:
> **loopback is allowed.** What makes a gate machine-dependent is egress. A local database
> is already pinned by the engine assertion, and `-m external` needs a loopback port to
> reach Playwright's browser. `test_loopback_is_deliberately_allowed` pins it so the next
> reader meets a decision rather than an oversight.

Recorded separately from CP1-8 because it is the wider fact, and CP1-8 is only the
instance that was found first.

`conftest.py`: *"Offline and deterministic. No live network, no DB container, no
Playwright, no LLM."* `tests/test_suite_hermeticity.py` makes that executable for three of
the four — environment variables, the database URL, the settings flags. **Network is not
covered.** Measured: `/search_stepstone` issues a real request to
`https://www.stepstone.de/en/` during a normal gate run, from two different tests.

This is `bf88f64`'s job left half done. That commit fixed the environment leak for the
database because that was the leak CP1-7 exposed; the same class of leak on the network
was never looked for, because no failing test pointed at it. The fix is one autouse guard
that fails on an outbound connection, and it converts "offline" from a docstring into
something the gate enforces — which is the entire pattern of CP1-7 and A13.

Filed as part of **CP1-8**'s fix rather than as its own item, because the two want the
same commit and the guard is what stops the next inert stub from being invisible.

### What did not move

`ci/baseline.json` is unchanged and should stay unchanged: the second pass added no tests
and no `app/` code. pytest 291, pyright 32, ruff 747, imports 2, failed 0. Every mutation
and probe above was reverted and the working tree verified byte-identical by checksum.

Note for whoever closes CP1-8: **fixing it will change `pytest_passed` only if you add
tests, but it may change the suite's runtime materially** — two live HTTP requests leave
the gate. If the network guard turns anything else red, that is a find, not a regression.
