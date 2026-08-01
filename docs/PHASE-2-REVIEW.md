# Phase 2 Review — Amendments to `refactor-plan.md`

Outcome of the playbook's Chat review step. The plan is sound; these are nine
amendments, four decisions, and one new slice.

Amendments are numbered **R1–R9** to avoid collision with the `A`/`B`/`C`/`D` bucket
IDs in [backlog.md](backlog.md).

Status as of 2026-07-31: engineering-practice work has landed (CI, lock file,
pre-commit config, ADRs, Dockerfile, backlog, liveness-audit prompt).

> **All amendments R1–R9 and decisions D1–D4 are now applied to
> [refactor-plan.md](refactor-plan.md), and the Housekeeping item is done.**
>
> Two claims in this document did not survive verification and were corrected rather
> than transcribed:
>
> - **R1** — `pyrightconfig.json` does *not* exist; Slice 0 correctly lists it as new
>   work. The 63-vs-67 file count is a scoping difference (`alembic/` included or not)
>   and the error count is **32 under every scoping**, so the gating number was never
>   ambiguous. Recorded in plan §1.1.
> - **R2** — the models are **not** DDL-portable. `Base.metadata.create_all` on SQLite
>   raises `CompileError` on 12 `UNIQUEIDENTIFIER` columns across all 6 tables, which
>   blocks PostgreSQL too. Logged as backlog **A6** and promoted to a standalone
>   bugfix. R2's "Slice 1 is not blocked" conclusion survives but narrows: the
>   artifact, log-streaming and scoring work is unblocked; the DB-touching contract
>   tests wait for A6.
>
> The Housekeeping premise was also slightly off — two of the four file pairs
> *differed*. `docs/` was the strict superset in both cases (it carried the
> "No silent failure" convention and the SQLite amendment), so deleting `refactor/`
> lost nothing.

---

## Decisions taken

**D1 — Canonical profile store: the database.** `db.crud_profiles` wins;
`config.profile_store` is demoted to import/export. Profiles are user-scoped and the DB
already carries the `user_id` FK; a non-technical user cannot edit `config/*.json`, so
the one-click goal requires UI-editable profiles; and `config/stepstone_seeds.json` has
a better life as seed *defaults* imported on first run than as a live parallel config
source. Unblocks Slice 6b, and resolves backlog **D2**.

**D2 — Slice 8 is verified differentially, not by unit tests.** Do not write unit tests
for a 0%-coverage Prefect path you are replacing. Run identical fixture seeds through
`PrefectOrchestrator` and `LocalOrchestrator`; assert the run directories are equivalent
— same file set, same `status.json` schema, identical scores per job. That tests backend
equivalence, which is the property that matters.

**D3 — Slice 1 gates on a contract checklist, not a coverage percentage.** See R3.

**D4 — Add Slice 2.5, a throwaway spike.** See R4.

---

# Amendments

## R1. Baseline reconciliation — partially done

The plan recorded **769 ruff findings / 63 pyright files @ `660a6a0`** where the session
measured **783 / 67**.

**Resolved:** ruff is **747**, not 769 or 783 — the `B008` per-file-ignores in
`pyproject.toml` suppress 22 FastAPI `Depends()` false positives. `refactor-plan.md` §1
and Slice 2 have been updated.

**Still open:** the pyright file-count discrepancy (63 vs 67), and the fact that Slice 0
lists creating `pyrightconfig.json` as new work when it already exists. Reconcile Slice 0
against actual repo state before starting it, and note in the §1 table which config files
were present at measurement time — a gate compared against a baseline measured under
different config is not a gate.

## R2. The SQLite pre-check — answered, and the answer changes Slice 1

This amendment originally proposed adding a check to Slice 0 to determine whether
contract tests could run on SQLite. **The question has since been answered by the CI
work, and my proposed check would have given the wrong answer.**

It used `create_engine()` directly, which bypasses `make_engine()`. Models are DDL-clean
on SQLite, so that check passes — while the application still cannot boot, because
`make_engine` ([app/db/engine.py:48](../app/db/engine.py#L48)) passes `pool_size`,
`max_overflow`, and `pool_timeout` unconditionally and `sqlite://` gets a
`SingletonThreadPool` that rejects all three. Logged as backlog **A5**.

Two distinct properties, and the plan should test both:

| Property | Check | Status |
| --- | --- | --- |
| Models are DDL-portable | `Base.metadata.create_all(create_engine(...))` | Passes |
| The app can build an engine for the dialect | `make_engine("sqlite:///...")` | **Fails on `:memory:`**, passes on a file |

**Consequences:**

- Slice 1 is **not blocked**. File-based SQLite on `tmp_path` works today and needs no
  application change. [TEST-STRATEGY.md](TEST-STRATEGY.md) §5.5 and §6 have been amended
  accordingly.
- The `make_engine` dialect dispatch is a **standalone bugfix commit**, like `_LogSink` —
  not a Slice 10 item. An engine factory that cannot construct an engine for the
  project's chosen default dialect is a defect, not a migration task.
- Add the second row of that table to Slice 0's verify block, so the distinction is
  recorded rather than rediscovered.

## R3. Replace Slice 1's exit criterion

**Delete:** *"Exit criterion: `fastapi_run.py` and `run_manager.py` both ≥ 60%."*

`fastapi_run.py` is 1,106 statements at 24%. Reaching 60% means covering ~400 more
statements in a module Slices 6–7 dismember. The 38 route contracts alone cannot get
there — you would have to test `_build_seeds_from_focus`, `_TemporaryEnv`,
`_compute_cutoff_iso`, `_run_prune_url_pool`, which are the exact symbols Slice 6 moves.
The percentage therefore **rewards tests bound to internals** — the F1 failure mode
TEST-STRATEGY exists to prevent, reintroduced as a number.

**Replace with a checklist. Slice 1 exits when all hold:**

- [ ] All 38 user-defined routes have a contract test, authenticated **and**
      unauthenticated (401), asserting status code and response shape
- [ ] Run artifacts asserted: `status.json`, `run.log`, `run_metrics.json`,
      `analysis_summary.json`, `REPORT_SUMMARY.md`
- [ ] Absent-optional-artifact case pinned — status polling must not 500 when an
      optional artifact is missing
- [ ] Run directory layout asserted as `output/<user_id>/<profile_key>/<run_id>/`
- [ ] Log streaming: offset 0, mid-file, at EOF, past EOF, append-between-reads
- [ ] All eight scoring invariants from TEST-STRATEGY §5.1, relational assertions only.
      No absolute constants except the accept/reject threshold
- [ ] No scoring test relies on `DEFAULT_FOCUS` implicitly — every one builds an
      explicit profile
- [ ] `DEFAULT_FOCUS` has one dedicated test asserting its shape
- [ ] Contract tests green against file-based SQLite (see R2)
- [ ] `pytest -q` → 0 failed, **0 skipped**, `--strict-markers` on
- [ ] `_experience_delta` question resolved and recorded (TEST-STRATEGY §8)

Report coverage as a metric. Do not gate on it.

## R4. New — Slice 2.5: single-process spike (throwaway)

Insert between Slice 2 and Slice 3. Depends on Slice 0 only.

Slice 8 delivers the one-click outcome and sits ninth; every slice before it is invisible
to a user. If the premise is wrong — if the Prefect flows need a reachable API and cannot
run in-process — that must surface now, not after seven slices.

The evidence says the premise is probably fine: `_run_prefect_inprocess_batch`
([app/fastapi_run.py:1367](../app/fastapi_run.py#L1367), lazy flow import at L1379)
already exists. This spike proves it.

**Branch:** `spike/inprocess-batch`. **Timebox:** 1 day. **Nothing is merged.**

```bash
pkill -f 'prefect server' || true
unset PREFECT_API_URL
# start FastAPI only, trigger a batch through the in-process path, inspect the run dir
```

**Success:** terminal `completed` status, a full trace in `run.log`, scored output in the
run directory — with no Prefect server running and no `PREFECT_API_URL` set.

**Record one paragraph in §5 of the plan:** whether it worked, and if not, the precise
failure (needs a reachable API / hangs / partial artifacts / silent subprocess fallback).
If it fails, Slice 8 is a rewrite rather than a refactor and both its estimate and its
risk rating change. Delete the branch either way — the output is a paragraph, not code.

## R5. Add `on_event` → `lifespan` to the head of Slice 8

Neither `architecture.md` nor `refactor-plan.md` mentions `lifespan` or `on_event`.
`fastapi_run.py:117` uses the deprecated `@app.on_event("startup")`.

`LocalOrchestrator`'s worker needs a start and a stop, `lifespan` is that hook, and
graceful shutdown is what stops an in-flight run being orphaned when uvicorn exits. It
also interacts with Slice 7, since app construction moves when routers extract.

Make it Slice 8 step one, with its own verify: worker starts on startup, drains on
shutdown, in-flight run reaches a terminal status rather than hanging.

## R6. Apply D1 to Slice 6b

Record the DB as canonical. `services/profiles.py` reads and writes through
`db.crud_profiles` only. Add a follow-up (out of scope for this plan): convert
`config.profile_store` into a seed-import path and `config/stepstone_seeds.json` into
first-run defaults. Cross-reference backlog **D2**.

## R7. Apply D2 to Slice 8

Replace the "add `LocalOrchestrator` tests or accept manual verification" ambiguity with
the differential check.

## R8. Harden risk #4's mitigation

`git diff --stat tests/contracts/` being empty does not prevent *adding* a compensating
file:

```bash
git diff --stat tests/ | tail -1                     # no modifications
git status --porcelain tests/ | grep '^??' && exit 1 # no additions
```

Applies to Slices 6 and 7.

## R9. Mark Slice 9 optional

Splitting `scoring.py` is justified by module size alone — no architectural rule requires
it, and at 87% coverage it is low-risk either way. It is the correct thing to cut if
energy runs short. Label it "opportunistic: any time after Slice 1, or never."

---

## R10. New — extract `app/domain/` (added post-Slice 0)  ✅ APPLIED

> Written into [refactor-plan.md](refactor-plan.md) as **Slice 2.9**, positioned after Slice 2
> and before Slice 3, with the mermaid diagram in §3 updated. Counts verified against the repo
> while writing it: 8 classes in `pipeline/models.py`, 8 absolute imports across 5 files, plus 2
> relative importers inside `pipeline/` that R10 did not mention — `pipeline/__init__.py:4` and
> `pipeline/pipeline.py:21`. Those two are why the shim survives the slice rather than being
> deleted in it.

Raised by backlog **A7**, which Slice 0's new import-linter gate surfaced.

`app/db/crud_profiles.py:12` imports `FocusProfileModel` from `app/pipeline/models.py`
and **constructs and returns it** at L210 — so a CRUD function's return type is a
pipeline type. That is not a stray edge; it is a structural statement, and it violates
`db/ must not import pipeline/`.

The underlying problem is that `pipeline/models.py` holds eight Pydantic types —
`UnifiedJobPosting`, `FetchMeta`, `LLMDetail`, `JobScoring`, `JobDetailsResponse`,
`BlockerCaps`, `Constraints`, `FocusProfileModel` — none of which are pipeline
internals. They are the project's shared vocabulary; `UnifiedJobPosting` was the
most-connected node in the dependency graph at 47 edges. They sit in `pipeline/` for
historical reasons, and the target architecture had nowhere else to put them.

**This gets worse without a fix.** Every service in Slice 6 and every router in Slice 7
that returns a job or a profile will import from `pipeline/`. One violation becomes a
dozen, each looking legitimate because no alternative exists.

**New slice — position: after Slice 2, before Slice 3.** Leaves-first: pure types, no
logic, no I/O.

- **Files:** `app/pipeline/models.py` → `app/domain/{job,profile,scoring,fetch}.py`;
  `app/domain/__init__.py` re-exports all eight.
- **Shim:** `app/pipeline/models.py` becomes a re-export of `app.domain`. Delete after
  Slice 7, once no caller uses the old path.
- **Verify:** shim identity (`app.pipeline.models.UnifiedJobPosting is
  app.domain.UnifiedJobPosting`); `app.domain` imports with no other `app` package in
  `sys.modules`; import-linter ratchet reaches **0**; gate otherwise unchanged.

`CLAUDE.md` has been amended — `domain/` added to the target tree, import rules updated
to `db, config, fetching -> {domain, common}`, and a rule added that shared types are
imported from `app.domain` and never from `app.pipeline`.

Once this lands, add a `domain-is-a-leaf` contract to `.importlinter` and uncomment the
contracts currently disabled for referencing unbuilt packages.

Note also that `crud_profiles.py` imports `config.profile_store` at L10 — so one module
bridges both profile stores. That is decision **D1** made concrete, and Slice 6b should
expect to find it there.

---

## Housekeeping — done

`refactor/` and `docs/` held four copies of the same files (`CLAUDE.md`,
`PHASE-0-RUNBOOK.md`, `TEST-STRATEGY.md`, `claude-refactor-playbook.md`).

**Two of the four were not byte-identical**, contrary to the premise above.
`docs/CLAUDE.md` (now `AGENTS.md`) carried a "No silent failure" convention the `refactor/` copy lacked,
and `docs/TEST-STRATEGY.md` carried the amended §5.5/§6 (file-based SQLite). `docs/` was
the strict superset in both cases, so `refactor/` held only stale content.

`refactor/` is deleted and the 9 `refactor/…` links in `refactor-plan.md` are repointed
at their `docs/` siblings, along with 10 link labels that still read `refactor/…`. All
link targets in `docs/` verified to resolve.

**Still stale elsewhere:** [AGENT-WORKFLOW.md](AGENT-WORKFLOW.md) lines 43 and 346 still
describe `refactor/` as existing and list its deletion as pending. Left untouched — that
file was not in scope for this pass.

---

## Unchanged, and correct

- Leaves-first ordering; Slices 1–2 as blocking preludes
- Ruff pass shipped early as its own blame-ignored commit
- `_LogSink` fix as a standalone commit, not folded into 6c
- The shim-identity check (`is` on both import paths)
- Cold-interpreter cycle verification
- The "services layer imports without FastAPI" check as Slice 6's acceptance criterion —
  the single best verification idea in the plan
- Slice 6's three-way split, and moving before deduplicating the two batch runners
- The ratchet gate over conventional pass/fail (ADR 0002) — correct call, and the reason
  CI is usable on day one
