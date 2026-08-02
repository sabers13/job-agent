# slice-02 — Lint and packaging hygiene

## Objective

Apply the deferred `ruff` autofix pass across the repo, add the two missing
`__init__.py` files, and delete the dead `get_db`. **No behaviour change whatsoever.**

This ships before the structural slices on purpose: 670 auto-fixable findings landing
inside a structural diff would make that diff unreviewable.

Work on branch `slice/02`, off `main`.

## Read first

- `AGENTS.md`
- `docs/refactor-plan.md` § "Slice 2 — Lint and packaging hygiene"
- `docs/TEST-STRATEGY.md` § 7 (why `tests/legacy/` must not be touched)

## Allowlist

Only these paths may change. Anything else is a scope violation.

```allowlist
.git-blame-ignore-revs
alembic/env.py
app/api/auth_routes.py
app/api/schemas.py
app/auth/deps.py
app/auth/security.py
app/common/logging_ctx.py
app/common/utils.py
app/config/__init__.py
app/config/focus.py
app/config/profile_store.py
app/db/crud_profiles.py
app/db/engine.py
app/db/health.py
app/db/models.py
app/db/session.py
app/fastapi_run.py
app/fetching/__init__.py
app/fetching/http_client.py
app/fetching/polite_fetch.py
app/gui_runs/__init__.py
app/gui_runs/run_manager.py
app/pipeline/__init__.py
app/pipeline/llm_enrich.py
app/pipeline/models.py
app/pipeline/output.py
app/pipeline/parsers.py
app/pipeline/pipeline.py
app/pipeline/potential_bucket.py
app/pipeline/resume_parse.py
app/pipeline/scoring.py
app/pipeline/state.py
app/pipeline/templating.py
app/pipeline/url_pool.py
app/pipeline/url_pool_maintenance.py
app/prefect_run.py
app/stepstone/__init__.py
app/stepstone/dates.py
app/stepstone/search_http.py
app/stepstone/search_playwright.py
app/stepstone/smoke.py
ci/baseline.json
scripts/build_url_pool_from_snapshots.py
scripts/filter_analysis_summary.py
```

> The allowlist is nearly the whole repo, so it is a **weak** scope check for this slice.
> The real scope proof is the reproduction check below — it proves the diff contains
> *only* what `ruff` produces. Treat that as the gate, not the allowlist.

## Symbols moved

None. Zero. If a symbol moves, changes signature, or changes module, you have left the
slice.

Rule categories being fixed (measured at `88d1d56`, 747 findings total, 670 fixable
outside `tests/legacy/`):

| rule | n | what |
| --- | ---: | --- |
| UP006 | 317 | `Dict` → `dict` |
| UP045 | 257 | `Optional[X]` → `X \| None` |
| UP035 | 58 | deprecated `typing` imports |
| I001 | 28 | import sorting |
| UP017 | 25 | `timezone.utc` → `datetime.UTC` |
| F401 | 15 | unused imports |
| UP037 | 14 | quoted annotations |
| E401, UP007, B007, UP047, UP015, F841, UP041 | 13 | assorted |

## Ship as two commits, in this order

**Commit 1 — pure mechanical.** Only what these two commands produce, nothing else:

```bash
ruff check  . --fix --extend-exclude tests/legacy,docs
ruff format .       --extend-exclude tests/legacy,docs
```

Message: `style: apply ruff --fix and format (no behavior change)`.
This is the commit whose SHA goes in `.git-blame-ignore-revs`, following the precedent
set by `cee14d1`:

```bash
git rev-parse <commit-1-sha> >> .git-blame-ignore-revs
```

Full 40-char SHA, one per line. `blame.ignoreRevsFile` is already configured, so
`git blame` picks it up with no further setup.

**Commit 2 — packaging, by hand.** `app/config/__init__.py`,
`app/gui_runs/__init__.py`, deleting `get_db`, `ci/baseline.json`,
`.git-blame-ignore-revs`.

Splitting them matters: `.git-blame-ignore-revs` should only ever point at a commit that
is *purely* mechanical. Hand edits inside a blame-ignored commit become invisible to
`git blame` forever.

## Packaging work (commit 2 only)

- Add `app/config/__init__.py` and `app/gui_runs/__init__.py`, empty. These are the only
  two packages under `app/` without one, and the "public API boundary is what
  `__init__.py` re-exports" rule cannot apply to a namespace package.
- Delete `get_db` from `app/db/session.py`. Confirmed dead — pyright `findReferences`
  returns exactly one result, its own definition. Remove any import it alone was using.
- `tests/__init__.py` already exists. Do not add more.

## Forbidden

- **Do NOT touch `tests/legacy/`.** Both ruff commands carry `--extend-exclude tests/legacy,docs`.
  Its contents are byte-identical to `660a6a0` and the quarantine commit says so; that
  is what makes it usable as an independent second opinion during Phase 4. It has 8
  findings. Leave all 8.
- **Do NOT edit `tests/contracts/` or `tests/unit/`.** The brief's dry run measured this:
  `ruff --fix` and `ruff format` change **zero** files anywhere under `tests/` — the
  Slice 1 suite was written ruff-clean. So the rule is not "beyond what ruff produces",
  it is simply **`tests/` must not change at all**. Any diff there is a violation, and
  check 3 below is a hard `wc -l` of 0. These files are the oracle every later slice is
  graded against; a lint pass has no business editing them.
- **Do NOT hand-fix any finding ruff cannot fix.** 31 findings survive by design:
  - `B904` (17) — raise-without-`from`. Real, but it touches error-handling semantics
    and gets its own commit (backlog **B3**).
  - `F821 Undefined name '_LogSink'` at `app/fastapi_run.py:1586` — **this is backlog
    A1, a live bug.** ruff has independently found the defect that keeps the URL-pool
    prune endpoint permanently broken. It must be fixed test-first in its own commit,
    between Slice 1 and Slice 5. Touching it here hides a behaviour change inside a
    lint diff.
  - `E722` (1), `E402` (1), and the rest — leave them.
- **Do NOT raise any number in `ci/baseline.json`.** Lowering is required where you
  improve one.
- **Do NOT run `pre-commit run --all-files`.** It rewrites 55 files in one pass and has
  already pulled this slice's work into an unrelated bugfix once. Hooks run on staged
  files at commit time.
- Do NOT add or remove dependencies.
- Do NOT fix unrelated issues you notice — list them in the report's NARRATIVE instead.

## Stop and ask

- The reproduction check below fails — it means something in the diff is not from ruff.
- The `pytest` result after the pass differs **in any way** from the one you recorded
  before starting (see Gate below). Do not compare against a number written in this
  brief — measure it yourself at slice start.
- Removing `get_db` breaks an import, or `findReferences` shows a second reference.
- A ruff fix changes a string literal, a default argument value, or anything inside
  `alembic/versions/` beyond import formatting.
- Any `ci/baseline.json` number would have to go **up**.

## Gate

```bash
make gate
```

**Step 0 — record the starting numbers yourself.** Run `make gate` before touching
anything and write the result into your report. Every "must be identical" below is
against *that* measurement, not against a figure in this document.

```bash
make gate     # record: pytest, ruff, pyright, imports
```

> This brief originally hard-coded `208 passed`. Between it being written and being
> executed, CP‑1 remediation added 90 tests, and Codex correctly refused to start on the
> contradiction. **Do not reintroduce a literal count here.** A brief that restates a
> gate number goes stale the moment an unrelated slice lands — the same defect as
> CP1‑6's duplicated constants, one level up. `ci/baseline.json` is the single source of
> truth; this document points at it.

Expected after both commits, relative to Step 0:

```
pytest    identical to Step 0 in every field   <- a change here is a behaviour change
ruff      ~31 findings  (measured; was 747 at Slice 0)
pyright   must not rise
imports   must not rise  (A7 is Slice 2.9's job)
```

The ruff figure comes from a full dry run of this slice at `88d1d56`. Treat it as an
expectation, not a target — measure and bank what you actually get.

Lower `ruff_findings` in `ci/baseline.json` to whatever you actually measure, in
commit 2.

## Slice-specific check — proves NO BEHAVIOUR CHANGE

The allowlist cannot police this slice — it is nearly the whole repo. These four checks
can. All four must pass before the slice is done.

**1. The suite is bit-for-bit unaffected.** Identical to your Step 0 measurement in every
field. Any difference is a behaviour change and a stop-and-ask.

```bash
.venv/bin/python -m pytest -q -p no:cacheprovider | tail -1
# must match the line you recorded in Step 0, exactly
```

**2. The diff is reproducible from the parent commit.** Re-run the same two ruff commands
against commit 1's parent in a scratch worktree; the resulting tree must hash identically
to commit 1's tree. If it does, the diff provably contains only what ruff produced — no
hand edit can hide in it.

```bash
STYLE_SHA=$(git rev-parse HEAD~1)          # commit 1, if commit 2 is HEAD
PARENT=$(git rev-parse "$STYLE_SHA"^)

rm -rf /tmp/s2verify
git worktree add --detach /tmp/s2verify "$PARENT"
( cd /tmp/s2verify \
  && ruff check . --fix --extend-exclude tests/legacy,docs -q >/dev/null 2>&1
     ruff format . --extend-exclude tests/legacy,docs -q >/dev/null 2>&1
     git add -A )

REPRO=$(git -C /tmp/s2verify write-tree)
CHILD=$(git rev-parse "$STYLE_SHA"^{tree})

[ "$REPRO" = "$CHILD" ] \
  && echo "REPRODUCIBLE — diff is exactly what ruff produces" \
  || { echo "NOT REPRODUCIBLE — stop and ask"; git diff --stat "$CHILD" "$REPRO"; }

git worktree remove --force /tmp/s2verify
```

This check was validated while writing the brief: two independent worktrees from
`88d1d56` both produced tree `cc06a8f90516bf97ab8d38a3f7d6aa2ee11fa052`.

**3. `tests/` is untouched — the whole directory, not just `legacy/`.**

ruff changes no test file, so this is a hard zero, not a judgement call.

```bash
git diff --name-only main...HEAD -- tests/ | wc -l   # must be 0
```

**4. `docs/` and `alembic/versions/` are untouched.**

Both are trapped by the same gotcha, which is why the commands use
`--extend-exclude` rather than `--exclude`:

- **`--exclude` REPLACES the `exclude` list in `pyproject.toml`; it does not add to it.**
  Using it silently re-enables `alembic/versions/`, and the dry run showed ruff then
  rewriting three existing migrations — flatly against the AGENTS.md prohibition on
  editing them.
- `ruff format` reformats **Python code blocks inside Markdown**, so a plain run edits
  `docs/refactor-plan.md` and `docs/TEST-STRATEGY.md`.

```bash
git diff --name-only main...HEAD -- docs/ alembic/versions/ | wc -l   # must be 0
```

## When done

```bash
./scripts/slice_report.sh slice-02 main
```

Then fill in the NARRATIVE section only. The FACTS section is generated — do not edit it.

In NARRATIVE, state explicitly:
- the measured `ruff` count and what you lowered the baseline to
- whether the reproduction check passed
- every finding you left unfixed and why
