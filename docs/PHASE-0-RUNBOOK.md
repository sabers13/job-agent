# Phase 0 — Foundation (Job Agent)

Run top to bottom. Do not start Phase 1 until every box is checked.

You said you haven't checked your baseline, so **step 1 is measurement, not
changes.** Nothing here modifies application code.

---

## Checklist

```
[ ] 0.1  Record the starting commit
[ ] 0.2  Record the pytest baseline
[ ] 0.3  Configure pyright and record the error baseline
[ ] 0.4  Measure coverage on the modules being restructured
[ ] 0.5  Decide: write tests first, or proceed
[ ] 0.6  Create the refactor worktree
[ ] 0.7  Install plugins and verify LSP is actually live
[ ] 0.8  Commit CLAUDE.md
```

---

## 0.1 — Record the starting commit

```bash
cd /path/to/job-agent
git status --porcelain          # must be clean; commit or stash first
git rev-parse HEAD
```

Write the SHA down somewhere outside the repo. Every "did the refactor break this?"
question resolves against it.

---

## 0.2 — pytest baseline

```bash
source .venv/bin/activate
set -a; source .env.dev; set +a

pytest -q 2>&1 | tail -30
```

Record: **passed / failed / errored / skipped counts**, and the names of anything
already failing.

Tests that were already red are not your problem to fix during the refactor — but
if you don't know they were red beforehand, you will spend hours assuming you broke
them.

> Your suite touches the DB and config. If collection errors out because
> `.env.dev` isn't loaded, that's a test-isolation problem worth noting, not a
> reason to skip this step. Record it and move on.

---

## 0.3 — pyright baseline

Install the binary first. The plugin is only wiring; if `pyright` isn't on PATH the
plugin appears enabled and does nothing.

```bash
npm install -g pyright
pyright --version               # must resolve
```

**Configure it before the first run.** Without config, pyright walks `.venv/`,
`output/`, and `.prefect/` and gives you a meaningless five-figure error count.
Create `pyrightconfig.json` at the repo root:

```json
{
  "include": ["app", "tests", "scripts"],
  "exclude": [
    "**/node_modules",
    "**/__pycache__",
    ".venv",
    "output",
    ".prefect",
    "alembic/versions"
  ],
  "typeCheckingMode": "basic",
  "pythonVersion": "3.12",
  "venvPath": ".",
  "venv": ".venv",
  "reportMissingImports": true,
  "reportMissingTypeStubs": false
}
```

Then:

```bash
pyright --outputjson | jq '.summary'
```

Record the error and warning counts.

**Start at `basic`, not `strict`.** Strict mode on a fast-written codebase produces
thousands of errors, which makes the number useless as a gate — you can't tell
whether a slice made things worse. Get to zero at `basic`, then ratchet up.

`alembic/versions` is excluded because generated migrations are noisy and you're not
editing them anyway.

---

## 0.4 — Coverage on the restructure targets

```bash
pip install pytest-cov
pytest --cov=app --cov-report=term-missing 2>&1 | tail -40
```

The numbers that matter are for the modules the first slices will touch:

| Module | Why it matters | Coverage |
|---|---|---|
| `app/fastapi_run.py` | Being split into routers + services — the largest slice | |
| `app/pipeline/scoring.py` | Behavior must not change; blocker/cap rules are load-bearing | |
| `app/pipeline/pipeline.py` | Stage boundaries are being redrawn | |
| `app/gui_runs/run_manager.py` | Owns the artifact contract that must never break | |
| `app/db/models.py` | About to be migrated to a second SQL dialect | |

Fill in the column. This table is the input to the next decision.

---

## 0.5 — Decision point: tests first?

**If `fastapi_run.py` or `scoring.py` is below ~60% line coverage, stop and write
tests before refactoring anything.**

This is not optional caution, it's the definition of the activity. Refactoring means
changing structure while preserving behavior — with no tests, you have no way to
observe behavior, so you aren't refactoring, you're rewriting and hoping.

For this project specifically, the highest-value tests to have in place first:

1. **Scoring invariants.** Given a fixed job fixture and profile, the heuristic
   score is exactly N; a blocker caps the score regardless of other signals; the
   potential-applications path triggers on the boundary case. These are pure
   functions and cheap to pin down.
2. **Artifact contract.** A completed run produces `status.json`, `run.log`,
   `run_metrics.json`, and `analysis_summary.json` with the expected shape —
   including the case where an optional artifact is absent, which is the Pydantic
   bug you already hit once.
3. **Log-streaming protocol.** `read_log_chunk` at offset 0, mid-file, at EOF, and
   past EOF returns correct chunks and offsets. Your GUI depends on this and it's
   trivially testable.
4. **Auth + route smoke.** Every route returns something other than 500 for an
   authenticated request. This is what catches a botched router split.

Items 1–3 are characterization tests: assert what the code does *now*, not what it
should do. If current behavior is wrong, note it, pin it anyway, and fix it in a
separate commit outside the refactor.

That's a full Claude Code session on its own, at Sonnet 5 / `medium` — it's
mechanical test authoring, not architecture.

---

## 0.6 — Worktree

```bash
cd /path/to/job-agent
git worktree add ../job-agent-refactor -b refactor/restructure
cd ../job-agent-refactor
```

You'll need to redo the environment in the worktree:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip && pip install -r requirements.txt
cp ../job-agent/.env.dev .env.dev
```

Confirm the worktree is genuinely independent before proceeding:

```bash
pytest -q                       # same result as step 0.2
```

---

## 0.7 — Plugins and LSP verification

```bash
/plugin install pyright-lsp@claude-plugins-official
/plugin install frontend-design@claude-plugins-official
/plugin install security-guidance@claude-plugins-official
/reload-plugins
```

Three, not more. `frontend-design` earns its slot because you're building the UI
from scratch in Phase 5.

**Verify LSP before anything else.** Ask Claude Code:

> Show me the definition of `create_run_dir` and every reference to it.

Working: an exact file and line, instantly, with `LSP` visible in the tool output.

Not working: "let me search the codebase," followed by grep or ripgrep. Restart
Claude Code and confirm `pyright` is on PATH from the same shell. There's a known
issue where the plugin reports enabled while the server never registers — trust the
go-to-definition test, not the plugin list.

Do not proceed without this. Every later phase depends on `find_references` being
semantic rather than textual, and `run_manager` is exactly the kind of module where
grep will find matches in comments and docstrings and send the agent down the wrong
path.

---

## 0.8 — Commit CLAUDE.md

Before committing, verify the **Commands** block against your actual repo. A wrong
command there gets executed every session.

Specifically check:

- Is it `pytest -q` at the root, or do you need a `-c` / rootdir flag?
- Do you have `ruff` installed at all? If not, either add it or delete the line —
  don't leave a command that fails.
- Is Python actually 3.12 in your venv? (`python -V`)
- Does `alembic upgrade head` run without extra env beyond `.env.dev`?

Then:

```bash
cp CLAUDE.md pyrightconfig.json /path/to/job-agent-refactor/
cd /path/to/job-agent-refactor
git add CLAUDE.md pyrightconfig.json
git commit -m "Add CLAUDE.md and pyright config for restructure"
```

---

## What Phase 0 does not include

- No application code changes.
- No orchestration work. The Prefect → `Orchestrator` protocol swap is a Phase 4
  slice, planned in Phase 2.
- No database migration. SQLite/dialect-agnostic is a Phase 4 slice and needs the
  architecture map from Phase 1 first.
- No UI. Separate track, Phase 5, separate sessions.

Resisting the urge to start on these is the entire point of Phase 0.

---

## Baseline record

Fill this in and keep it. Paste it into Phase 1's opening prompt.

```
Start commit:        ________________________________
pytest:              ____ passed  ____ failed  ____ error  ____ skipped
Pre-existing fails:  ________________________________
pyright (basic):     ____ errors  ____ warnings
Coverage, fastapi_run.py:   ____%
Coverage, pipeline/scoring.py: ____%
Coverage, run_manager.py:   ____%
Tests written first? (y/n)  ____
LSP verified live?   (y/n)  ____
```
