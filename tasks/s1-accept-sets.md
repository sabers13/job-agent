# s1-accept-sets — Narrow S1's wide accept sets in `tests/contracts/`

## Work class — read this before the objective

**This is CP‑1 oracle remediation, not a slice.** The deliverable *is* a change under
`tests/contracts/`. Two consequences, both deliberate:

- `scripts/slice_report.sh` will print `**VIOLATION** tests/ was modified` and list the
  three files below. For this work class **that line is expected output, not a finding.**
  Do not treat it as a scope violation and do not stop on it. Note it in the NARRATIVE
  under "Decisions taken that were not in the brief" and move on.
- `AGENTS.md`, R8 and `CHAT-CHECKPOINTS.md`'s escalation trigger forbid editing
  `tests/contracts/` **to make a slice pass**. That prohibition is about direction of
  authority: editing the oracle so a structural move goes green inverts it. This task is
  the opposite — the oracle is the subject, and **no `app/` path may appear in the diff**.
  The allowlist enforces that; if you find yourself wanting to change `app/`, stop.

Precedent: CP1-8's fix (`2c87308`) was test-side only under the same reasoning. See
`docs/AGENT-WORKFLOW.md` §"Oracle remediation is not a slice".

## Objective

Replace seven accept-set assertions that accept a success code and a failure code
together with assertions pinned to the measured outcome, and rebuild one test whose
asserted branch is unreachable. `AGENTS.md` §Conventions: "No assertion may accept both
the success and the failure state."

Every status code in this brief was **measured**, twice, byte-identical, by a Claude Code
probe on 2026-08-07 that reproduced each test's fixture setup with the net guard active.
They are not predictions. If any measurement below disagrees with what you observe, that
is a stop-and-ask condition — do not reconcile it silently.

## Step 0 — before touching anything

Run `make gate` and record all five numbers. Every "must not move" check below compares
against that measurement, not against any number written in this file or in
`docs/STATE.md`. `ci/baseline.json` is the source of truth.

`pytest_passed` **must not change.** This task adds and removes no tests; it only
rewrites assertions inside existing ones. A moved count means something else happened.

## Read first

- `AGENTS.md` — §Conventions, the "No assertion may accept both the success and the
  failure state" rule and the `raising=False` prohibition
- `docs/AGENT-WORKFLOW.md` §1b, including §"Oracle remediation is not a slice"
- `docs/STATE.md` §"Next three actions" item 1
- `docs/TEST-STRATEGY.md` §6 (no writes outside `tmp_path`)

## Allowlist

```allowlist
tests/contracts/test_health_and_pages_api.py
tests/contracts/test_runs_api.py
tests/contracts/test_resumes_api.py
ci/baseline.json
```

`tests/conftest.py` is **not** in the allowlist. If a change appears to require it, stop
and ask — see Stop and ask.

---

## Commit 1 — the six pure narrowings

No fixture changes. Each is a one-line assertion replacement.

| Site | Current | Replace with |
| --- | --- | --- |
| `test_health_and_pages_api.py:28` `/health/config` | `in (200, 503)` | `== 200` |
| `test_health_and_pages_api.py:178` `/search_stepstone_list` | `in (200, 400, 422)` | `== 422` |
| `test_health_and_pages_api.py:184` `/job_details` | `in (200, 400, 422)` | `== 422` |
| `test_health_and_pages_api.py:190` `/bundle` | `in (200, 400, 422)` | `== 422` |
| `test_health_and_pages_api.py:196` `/aggregate_report` | `in (200, 400, 422, 404)` | `== 422` |
| `test_runs_api.py:196` `/api/run_single` | `in (200, 400, 404, 422)` | `== 422` |

**The four 422s and `run_single` are Pydantic request-model rejections**, so add the
content the accept set was hiding: assert the response body names the missing field.
Measured missing fields, in order — `body.seed_url`, `body.url`, `body.job`,
`body.reports`, and for `run_single` both `profile_key` and `url`. Assert on the field
location FastAPI reports, not on the human-readable message string.

`/health/config` measured 200 with `config_ok`, `output_ok` and `db_ok` all true. Its
direct sibling `/health/db` is already pinned to `== 200` for this exact reason
(CP1-7 / A13) and its docstring is two lines above — follow that precedent and cross-
reference it.

Also in this commit, at `test_runs_api.py:197`: `assert response.status_code != 500` is
already dead, because the accept set on the line above excludes 500. Delete it. `== 422`
subsumes it.

**Do not touch the docstring text at `test_health_and_pages_api.py:34` and `:157`.**
Those are prose recording why earlier accept sets were wrong. They must survive verbatim.

## Commit 2 — rebuild `test_start_batch_run_returns_a_run_id_without_running_anything`

`test_runs_api.py:160–180`. This one is not a narrowing job. Measured:

```
as written        -> status 404, stub calls [], both monkeypatched stubs never ran
with Profile row  -> status 200, stub calls ["batch"], run_id minted
```

The `client` fixture seeds a `User` but no `Profile`, so `get_focus_profile_model_for_user`
(`app/db/crud_profiles.py:177`, called at `app/fastapi_run.py:1700`) returns `None` and the
handler raises 404. The `if response.status_code == 200:` branch is dead, no run is minted,
and the test's name and docstring describe behaviour it never reaches.

**Do not narrow this to `== 404`** — that pins the misnomer and keeps the branch dead.
Instead:

1. Seed a `Profile` row for `test_user` with `profile_key="junior_data_bi"` before the
   request. Request `db_session` alongside `client`; pytest hands back the same instance.
   Build `focus_config_json` by dumping a **`FocusProfileModel`**, not a hand-written dict
   literal — same reasoning as the fixture at `tests/conftest.py:196`, so a field rename
   breaks loudly instead of silently.
2. Assert `== 200`, a non-empty `run_id` in the body, and **that the stub actually fired** —
   record calls in the stub and assert on the recorded args, as `stub_stepstone_adapter`
   does for CP1-8. An unasserted stub is how CP1-8 happened.
3. Remove the `if response.status_code == 200:` guard. There is one outcome now.
4. Fix both `monkeypatch.setattr(..., raising=False)` calls at lines 170–171 to
   `raising=True` (i.e. drop the argument). This is an `AGENTS.md` violation as written.
   It is redundant *today* — both names exist — but Slice 8's `LocalOrchestrator` renames
   exactly these two symbols, and `raising=False` is precisely what would turn that rename
   into a silent no-op.
5. Update the docstring to describe what the test now does.

Artifact writes are already safe: `client` depends on `run_output_root`, which redirects
`run_manager`, `_resume_root` and `pipeline.state` under `tmp_path`. Verify no file
appears outside `tmp_path` — `git status` must be clean after the run.

## Commit 3 — tightening (not violations)

Label this commit **tightening**, not a fix. These six do not break the rule — each accepts
only success codes or only failure codes. They are narrowed so the file stops carrying
near-miss examples that invite the question to be re-opened.

| Site | Current | Measured | Replace with |
| --- | --- | --- | --- |
| `test_health_and_pages_api.py:67` signup | `in (200, 201)` | 201 | `== 201` |
| `test_health_and_pages_api.py:100` bad email | `in (400, 422)` | 422 | `== 422` |
| `test_health_and_pages_api.py:104` logout | `in (200, 204)` | 200 | `== 200` |
| `test_resumes_api.py:28` upload | `in (200, 201)` | 200 | `== 200` |
| `test_resumes_api.py:68` malformed uuid | `in (400, 404, 422)` | 400, `{"detail":"Invalid resume_id"}` | `== 400` + assert the detail |
| `test_runs_api.py:152` traversal ×4 | `in (400, 404, 422)` | see below | per-param expected code |

The traversal case is parametrized over four inputs that split **404 / 404 / 404 / 400**:
`../escape` 404, `a/b` 404, `..` 404, `%2e%2e` **400**. Carry the expected code in the
parametrize tuple. The `%2e%2e` difference is real behaviour — percent-decoding rejects
before routing does — and the current accept set hides it. Keep the parameter count at
four; `pytest_passed` must not move.

---

## Explicitly out of scope

- **`test_health_and_pages_api.py:117` and `:123`** — the GUI page routes,
  `in (200, 302, 303, 307)`. Measured: `/gui/login` 200; `/gui/run`, `/gui/profiles`,
  `/gui/logout` 303 → `/gui/login`. **Leave both exactly as they are.** Whether an
  unauthenticated GUI route should render or redirect is backlog **A12**, the auth-posture
  question, and it is on the CP‑3 agenda. Pinning these now would pin an answer CP‑3 has
  not given.
- Anything under `app/`. Not one line.
- Anything under `tests/legacy/`, `tests/unit/`, `tests/integration/`.
- The 78 broad `except` handlers (A3), A14/A15/A16 (pinned broken on purpose), A1, A5,
  A7, A13.

## Forbidden

- Do NOT edit `tests/conftest.py`.
- Do NOT edit any file under `app/`. The allowlist has no `app/` path; a diff containing
  one is a real scope violation, as distinct from the expected `tests/` line.
- Do NOT add or remove a test. `pytest_passed` must not move from your Step 0 reading.
- Do NOT use `monkeypatch.setattr(..., raising=False)` anywhere, including in code you
  are only moving.
- Do NOT write an accept set containing 500.
- Do NOT add dependencies.
- Do NOT raise a number in `ci/baseline.json`. Lowering is required if you improve one.
- Do NOT fix unrelated issues you notice — list them in the report.

## Stop and ask

Generously — a worker that stops is cheap here.

- Any measured status code in this brief disagrees with what you observe. Report both.
- Seeding the `Profile` row appears to require editing `tests/conftest.py`.
- Any file appears outside `tmp_path` after the suite runs (`git status` not clean).
- `pytest_passed` moves.
- A narrowed assertion fails intermittently — that means the route is not deterministic
  offline, which is a finding, not something to widen the assertion back for.
- You conclude one of the six "tightening" items is actually a violation, or one of the
  seven violations is actually legitimate.

## Gate

`make gate`

## When done

`make report SLICE=s1-accept-sets BASE=main`

Then fill in the NARRATIVE section only. The FACTS section is generated — do not edit it.
In "Decisions taken that were not in the brief", state explicitly that the `tests/`
violation line is expected for this work class, so the next reader does not re-derive it.
