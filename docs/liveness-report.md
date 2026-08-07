# Liveness Report

Evidence table for keep / change / drop decisions. Produced by the prompt in
[liveness-audit.md](liveness-audit.md) §"Prompt for a Claude Code session"; the five sections
below are that file's §"Output contract".

Generated **2026-08-08** against `main` @ `fd85028`, working tree clean. **Read-only** — no
application code, test, or other document was modified.

Section 4 is the input to CP‑3. Sections 1–3 are its supporting evidence. **Section 5 is not
optional**: everything the static evidence could not settle is stated there as a question rather
than guessed at, because CP‑3 will act on this file.

---

## 0. Method, and what to distrust

| Question | How it was answered |
| --- | --- |
| Route inventory | AST walk of `@app.<method>` / `@router.<method>` decorators in `app/fastapi_run.py` + `app/api/auth_routes.py`. **42**, matching `tests/contracts/routes.py` and `test_all_42_user_routes_are_accounted_for` |
| Auth posture per route | Decorator `dependencies=[Depends(get_current_user)]`, signature `Depends(...)`, or a manual in-body call. Cross-checked against the live dependency graph in `tests/contracts/routes.py::live_public_routes()` — **18 public / 24 protected**, exact match |
| Symbol references | **pyright LSP** (`pyright-langserver --stdio`), `textDocument/references` with `includeDeclaration: false`, one call per symbol. 285 public + 128 private top-level symbols, 413 calls, **0 errors** |
| Grep cross-check | Word-boundary regex over `app/`, `tests/`, `scripts/`, `alembic/`, `templates/`, `n8n workflows/`, `ci/`. Used **only** to find disagreements with the LSP, reported in §2.4 — never as the primary evidence |
| Coverage | `pytest -q --cov=app --cov-report=json` → **298 passed, 1 deselected, 53% total**. Zero-execution functions computed by intersecting each function's body line range with `executed_lines` |
| Silent failure | AST scan for broad (`Exception` / `BaseException` / bare) `except` handlers, classified by whether the handler re-raises and whether it logs at ERROR |
| Template / client callers | Regex over `templates/*.html` and `n8n workflows/*.json`. This is a **string-literal** question — URL paths are not symbols, so the LSP cannot answer it and grep is the correct tool here |

**Four things to distrust in this report.**

1. **pyright's reference index does not cover `alembic/`.** `pyrightconfig.json` sets
   `include: ["app", "tests", "scripts"]`. `alembic/env.py:8` does `from app.db import models`,
   which is what keeps every ORM class alive for `target_metadata`. So **every reference count
   for `app/db/models.py` is an undercount**, and `RunItem` (1 ref) and `UrlPoolEntry` (1 ref)
   are *not* deletion candidates on that number.
2. **A zero-reference route handler means nothing.** FastAPI registers handlers by decorator, so
   all 42 handlers, both `@app.exception_handler` functions, and `_startup_checks` correctly
   show 0 references. They are listed in §2 for completeness and excluded from §4.
3. **Coverage is a reported metric, never a gate** (amendment R3), and low coverage is evidence
   of *untested*, not *unused*. §4 states which of the two each row actually supports.
4. **Static analysis shows what can be called, not what is.** The runtime pass described in
   `liveness-audit.md` §"Manual step the audit cannot do" has **not** been run. Nothing here is
   runtime proof of disuse.

### 0.1 Reconciliation with `STATE.md` and `architecture.md`

Checked before starting, as instructed.

**`STATE.md` agrees with disk on every claim that was checkable.** Nothing here blocks.

| `STATE.md` claim | On disk |
| --- | --- |
| `main`, working tree clean, `c1daf51` merged | ✅ `fd85028`, clean |
| No `slice/*` or `spike/*` branch | ✅ branches are `main`, `master`, `fix/log-chunk-utf8`, `refactor/restructure` |
| `refactor/restructure` worktree at `/home/saber/Asus/job-agent-refactor`, parked at `660a6a0` | ✅ |
| Gate: `298 passed, 0 failed, 0 skipped, 1 deselected` | ✅ reproduced exactly, 84.8 s |
| A12 — 18 of 42 routes unauthenticated | ✅ exactly 18, and exactly the 18 in `tests/contracts/routes.py::PUBLIC_ROUTES` |
| `prefect_run.py` at 0% | ✅ 333 statements, 333 missed |
| A1 (`_LogSink`) still open | ✅ `try` at `fastapi_run.py:1588` is still indented inside the `class _LogSink` body opened at :1571 |
| Coverage figures "last measured at `db3b908`" | Drifted as expected, in the stated direction: `fastapi_run` 50 → **54%**, `run_manager` 82 → **84%**, total 52 → **53%**. Every other figure unchanged |

**`architecture.md` is a dated snapshot (2026‑07‑31 @ `660a6a0`) and two of its §8 findings are
now stale.** Not a contradiction — the file says what it was generated against — but CP‑3 must
not act on them:

- **"`get_db` is dead code"** — `get_db` no longer exists. Removed in `bc301e3`
  ("chore: add package markers and remove dead get_db"), 2026‑08‑02, which is *not* an ancestor
  of `660a6a0`. The only surviving mention is a docstring at `tests/conftest.py:317`.
- **"Three packages are missing `__init__.py`"** — `app/config/`, `app/gui_runs/` and `tests/`
  all have one now, from the same commit.
- Also new since that snapshot: **`app/db/types.py`** (`UtcDateTime`, `utcnow`), from the A8
  work. It is not in `architecture.md`'s inventory. `app/` is 49 files, not 46.
- Its §8 `_LogSink` finding **is** still accurate, at shifted line numbers.
- Its §6.3 reference counts are all lower than §2 below (`settings` 104 → 111, `score_job`
  20 → 73). That is Slice 1's contract suite landing, not a methodological disagreement.

One measured drift in the `AGENTS.md` / backlog **A3** figure, resolved rather than left open:
A3 records "78 broad `except` handlers with no re-raise; 17 in `fastapi_run.py`, 8 in
`stepstone/search_playwright.py`, 7 in `prefect_run.py`". Today the total is **77**, and all
three sub-counts still match exactly. Measured at `660a6a0` the total is 78 with
`app/db/session.py` at 4; on `main` it is 3. Slice 2 narrowed or re-raised one handler in
`db/session.py`. **The number is 77, and the difference is accounted for** — no unexplained gap.
Total broad handlers including those that do re-raise: 87.

---

## 1. Route table

42 user-defined routes. Columns:

- **After split** — target module from `refactor-plan.md` §Slice 7
  (`app/api/routes/{health,search,runs,profiles,resumes,artifacts,gui,auth}.py`). Four
  assignments are genuinely ambiguous and are raised as **Q1** in §5.
- **Caller** — a `templates/*.html` fetch/link, or the `n8n workflows/` prototype. "—" means no
  first-party caller was found in the repo.
- **Test** — `behaviour` = a test asserts what the handler does; `422 only` = the only test posts
  an empty body and asserts schema rejection, so **the handler body never runs**; `401 sweep` =
  covered only by `test_route_inventory.py`'s parametrised auth sweep. Coverage is
  executed/missing statements in the handler body.
- **Silent-failure risk** — see §1.1 for the definitions and the evidence per level.

| # | Method | Path | Handler | After split | Caller | Test | Handler cov | Silent-failure risk |
| ---: | --- | --- | --- | --- | --- | --- | ---: | --- |
| 1 | GET | `/health` | `health` | health | — | behaviour | 6/0 | **LOW-2** — reports `db_ok` / `output_ok`, but `_startup_checks` L129 & L137 discard the reason with no log |
| 2 | GET | `/health/db` | `health_db` | health | — | behaviour | 3/1 | none — `check_db`'s broad handler *returns* the error type and message |
| 3 | GET | `/health/config` | `health_config` | health | `gui_run.html:676` | behaviour | 4/2 | **LOW-2** — same `_startup_checks` handlers |
| 4 | GET | `/playwright_check` | `playwright_check` | health | — | `external`, **deselected** | 2/18 | none — broad handler logs and re-raises |
| 5 | GET | `/search_stepstone` | `search_stepstone` | search | — | behaviour (stubbed) | 5/4 | **HIGH-1** — its own `HTTPException(400)` is swallowed and re-raised as 500. Measured, §1.2 |
| 6 | POST | `/search_stepstone_list` | `search_stepstone_list` | search | — | **422 only** | 0/6 | none in the handler |
| 7 | POST | `/job_details` | `job_details` | search | **`n8n workflows/`** | **422 only** | 0/35 | **MED-3** — L462 swallows a profile-load failure into `DEFAULT_FOCUS`; the response never says which profile scored the job |
| 8 | POST | `/bundle` | `bundle` | search | **`n8n workflows/`** | **422 only** | 0/14 | none — has the `except HTTPException: raise` guard that #5 lacks |
| 9 | POST | `/aggregate_report` | `aggregate_report` | search | — | **422 only** | 0/6 | none — logs and re-raises |
| 10 | GET | `/api/profiles` | `list_profiles` | profiles | `gui_profiles.html:174` | behaviour | 3/0 | none |
| 11 | GET | `/api/my/profiles` | `list_my_profiles` | profiles | `gui_profiles.html:163`, `gui_run.html:366` | behaviour | 8/2 | **MED-3** — L743 turns unparseable `focus_config_json` into `{}`, so the name silently degrades to the key |
| 12 | GET | `/api/my/me` | `get_my_me` | profiles / auth (**Q1**) | — | behaviour | 1/0 | none |
| 13 | GET | `/api/my/profile/{key}` | `get_my_profile` | profiles | `gui_profiles.html:201` | behaviour | 6/0 | **MED-3** — `_profile_payload_from_db` L688 |
| 14 | GET | `/api/my/profile/{profile_key}/latest` | `get_my_profile_latest` | profiles / runs (**Q1**) | — | behaviour | 5/2 | none |
| 15 | POST | `/api/my/profile/{profile_key}/url_pool/prune_stepstone` | `prune_profile_url_pool_stepstone` | profiles / runs (**Q1**) | `gui_run.html:894` | behaviour (202-shape only) | 4/10 | **HIGH-1** — backlog **A1**. Returns 200 `status:"running"`; the background task cannot execute at all |
| 16 | POST | `/api/my/resume` | `upload_resume` | resumes | `gui_run.html:930` | behaviour | 36/4 | **HIGH-1** — L909 `except Exception: pass` around `parse_resume_file`. §1.3 |
| 17 | GET | `/api/my/resumes` | `list_resumes` | resumes | `gui_run.html:639` | behaviour | 3/0 | none |
| 18 | GET | `/api/my/resume/{resume_id}` | `get_resume_detail` | resumes | — | behaviour | 14/2 | none |
| 19 | POST | `/api/my/resume/{resume_id}/activate` | `activate_resume` | resumes | `gui_run.html:954` | behaviour | 10/2 | none |
| 20 | POST | `/api/my/profile` | `upsert_my_profile` | profiles | `gui_profiles.html:311` | behaviour | 8/0 | **MED-3** — `_profile_payload_from_db` on the return path (L1042) |
| 21 | POST | `/api/my/profile/{key}` | `upsert_my_profile_by_key` | profiles | — | behaviour | 8/0 | **MED-3** — same, L1068 |
| 22 | DELETE | `/api/my/profile/{key}` | `delete_my_profile` | profiles | `gui_profiles.html:341` | behaviour | 6/0 | none |
| 23 | GET | `/api/profile/{key}` | `get_profile_api` | profiles | `gui_profiles.html:203` | behaviour | 3/1 | none |
| 24 | POST | `/api/profile/{key}` | `upsert_profile_api` | profiles | — | behaviour | 2/0 | none |
| 25 | DELETE | `/api/profile/{key}` | `delete_profile_api` | profiles | — | behaviour | 3/1 | none |
| 26 | GET | `/gui/login` | `gui_login` | gui | `gui_profiles.html:154`, `gui_run.html:237` | render-or-redirect accept set (**deferred**) | 1/0 | none |
| 27 | GET | `/gui/profiles` | `gui_profiles` | gui | `gui_run.html:753` | render-or-redirect accept set (**deferred**) | 5/3 | none |
| 28 | POST | `/api/run_single` | `run_single` | runs | `gui_run.html:774` | **422 only** | 0/29 | none in the handler |
| 29 | POST | `/api/start_batch_run` | `start_batch_run` | runs | `gui_run.html:828` | behaviour + backend-selection assertion | 22/7 | **HIGH-1** — returns 200 `status:"running"`; both background orchestrators lose the traceback (§1.4) |
| 30 | GET | `/api/run_status/{run_id}` | `get_run_status` | runs | `gui_run.html:447` | behaviour | 11/0 | **MED-3** — `_augment_with_potential_applications` L326 & L338 silently drop the potential-applications metrics |
| 31 | GET | `/api/run_logs/{run_id}` | `get_run_logs` | runs | `gui_run.html:491` | behaviour (offset contract) | 12/1 | none |
| 32 | GET | `/api/run_summary/{run_id}` | `get_run_summary` | runs / artifacts (**Q1**) | `gui_run.html:725` | behaviour | 13/4 | **MED-3** — `_read_json_file` L1913 returns `None` on any error |
| 33 | GET | `/api/run_artifacts/{run_id}/potential_applications` | `list_potential_applications` | artifacts | `gui_run.html:583` | behaviour | 25/3 | **MED-3** — `_read_json_file` + `_coerce_float` L1930 |
| 34 | GET | `/api/run_artifacts/{run_id}/potential_applications/{job_key}` | `get_potential_application_detail` | artifacts | `gui_run.html:621` | behaviour | 14/1 | **MED-3** — `_read_json_file` |
| 35 | GET | `/gui/run` | `gui_run` | gui | `gui_login.html`, `gui_profiles.html` | render-or-redirect accept set (**deferred**) | 5/3 | none |
| 36 | GET | `/gui/logout` | `gui_logout` | gui | `gui_profiles.html:23`, `gui_run.html:25` | render-or-redirect accept set (**deferred**) | 3/0 | none |
| 37 | GET | `/run_state` | `get_run_state` | runs | — | behaviour | 3/3 | none — logs and re-raises |
| 38 | POST | `/run_state` | `set_run_state` | runs | — | behaviour | 3/3 | none — logs and re-raises |
| 39 | POST | `/auth/signup` | `signup` | auth | `gui_login.html` | behaviour | 11/5 | none |
| 40 | POST | `/auth/login` | `login` | auth | `gui_login.html` | behaviour | 9/4 | none |
| 41 | GET | `/auth/me` | `me` | auth | — | behaviour | 1/0 | none |
| 42 | POST | `/auth/logout` | `logout` | auth | — (the GUI links `GET /gui/logout`) | behaviour | 2/0 | none |

**Routes with no first-party caller anywhere in the repo (16):** 1, 2, 4, 5, 6, 9, 12, 14, 18,
21, 24, 25, 37, 38, 41, 42.

**Routes whose only caller is `n8n workflows/` (2):** 7, 8 — a tracked but ten-month-old
prototype. See D6 in §4 and **Q5** in §5.

**Routes whose body never executes in the gate (5):** 6, 7, 8, 9, 28. All five have a test, and
all five tests assert a 422 on an empty body and nothing else. This is exactly the distinction
the audit exists to make: they are **untested, not unused** — #28 is wired to `gui_run.html:774`
and #7/#8 have the `n8n` caller. #6 and #9 have neither a caller nor an executing test, which is
the weakest evidence position in the table.

### 1.1 Silent-failure levels

- **HIGH-1** — the caller receives a success status and cannot tell the operation failed.
- **MED-3** — the caller receives a success status with silently degraded content (a missing
  field, an empty payload, a fallback value it was not told about).
- **LOW-2** — the failure *is* reported to the caller, but the reason is discarded (no ERROR log,
  no exception attached).

Handler census across `app/`: **87** broad handlers, **77** of which do not re-raise, **75** of
which neither re-raise nor log at ERROR. Distribution of the 77:

```
17  app/fastapi_run.py                 4  app/pipeline/pipeline.py       1  app/db/health.py
 8  app/stepstone/search_playwright.py 4  app/pipeline/state.py          1  app/pipeline/output.py
 7  app/prefect_run.py                 4  app/stepstone/search_http.py   1  app/pipeline/parsers.py
 6  app/pipeline/llm_enrich.py         3  app/common/utils.py            1  app/pipeline/potential_bucket.py
 4  app/config/focus.py                3  app/db/session.py              1  app/stepstone/dates.py
 4  app/db/crud_profiles.py            3  app/pipeline/scoring.py        1  app/config/settings.py
 4  app/fetching/polite_fetch.py
```

Of the 17 in `fastapi_run.py`, **6 are on a route's request path** (#5, #7, #11, #13/20/21, #30,
#32/33/34), **4 are in the two background orchestrators**, **1 is A1**, and the rest are in
`_startup_checks` and two small file readers.

### 1.2 New finding — `/search_stepstone` returns 500 where it means 400

`fastapi_run.py:395-403`. The handler raises `HTTPException(400, "backend must be 'pw' or
'http'")` **inside** its own `try`, and the `except Exception` at :401 catches it — `HTTPException`
is an `Exception` — and re-raises it as `HTTPException(500, detail=str(e))`.

Measured, not inferred:

```
GET /search_stepstone?backend=nonsense
→ 500 {"detail":"400: backend must be 'pw' or 'http'"}
```

`/bundle` has the identical shape at :525-552 and is **correct**, because it carries an
`except HTTPException: raise` clause first. `/search_stepstone` is the only handler in
`fastapi_run.py` that raises an `HTTPException` inside a broad `try` without that guard — an AST
scan for the pattern returns exactly those two sites.

No test catches it: the route's one behavioural test uses a valid backend. This route is also
D3's subject, so **fixing it and deleting it are competing options** — flagged as **Q4** in §5.

### 1.3 New finding — résumé upload cannot report a parse failure

`fastapi_run.py:902-909` wraps `parse_resume_file` in `try: … except Exception: pass`. On
failure the `Resume` row is still committed with `text_content=None`, `parsed_json=None`, and
`is_active=True`, and the route returns 200. `ResumeUploadResponse`
(`app/api/schemas.py:125-129`) carries `resume_id`, `is_active`, `filename`, `sha256` — **no
parse status field exists**, so a caller cannot distinguish a parsed résumé from an unparsed one
even in principle. Every downstream consumer then reads an empty résumé snapshot.

This is the direct answer to D5: `resume_parse.py` is not dead, but its only call site is
constructed so that a format it cannot handle is indistinguishable from one it can.

### 1.4 Confirmed — the batch orchestrators lose the traceback

Already recorded from the Slice 2.5 spike (`refactor-plan.md` §6, STATE.md); confirmed here
statically. `_run_prefect_batch` (L1258 `pass`, L1309, L1342) and `_run_prefect_inprocess_batch`
(L1455/L1463 `seeds = None`, L1505) all format the exception into a `status["error"]` string and
write no traceback to `run.log`. `POST /api/start_batch_run` has already returned 200 by then.

---

## 2. Symbol reference table

pyright LSP `textDocument/references`, `includeDeclaration: false`. Grouped by module, **lowest
reference count first**. "Own-module only" marks a symbol whose every reference is inside its own
defining file — public in name, internal in fact.

### 2.1 Zero-reference public symbols — the honest list

52 of 285 public symbols have zero references. **44 of them are false positives of the method**
and are excluded from §4:

- 42 route handlers, registered by decorator
- 2 `@app.exception_handler` functions — `sqlalchemy_operational_error_handler`,
  `sqlalchemy_dbapi_error_handler`

(`router` in `auth_routes.py` is *not* among them — 5 refs.)

**The remaining eight are genuinely unreferenced:**

| Symbol | Location | What the evidence supports |
| --- | --- | --- |
| `JobDetailsResponse` | `app/pipeline/models.py:82` | **Unused.** Shadowed by `app/api/schemas.py:103`; `fastapi_run.py:42` imports the schemas one |
| `ProfileListItem` | `app/fastapi_run.py:573` | **Unused.** `list_profiles` builds raw dicts instead |
| `set_run_ctx` | `app/common/logging_ctx.py:15` | **Unused.** `run_ctx_scope` duplicates the body inline rather than calling it |
| `clear_run_ctx` | `app/common/logging_ctx.py:31` | **Unused.** `run_ctx_scope` uses `ContextVar.reset(token)` |
| `get_profile_keys` | `app/config/profile_store.py:39` | **Unused** |
| `update_profile_for_user` | `app/db/crud_profiles.py:89` | **Unused.** `upsert_profile_for_user` is the live path |
| `CachePayload` | `app/pipeline/pipeline.py:29` | **Unused** type alias |
| `COMPONENT_FUNCS` | `app/pipeline/scoring.py:794` | **Unused** registry — `score_job` calls each `apply_*` directly |

**Private top-level symbols with zero references** — a separate LSP pass over 128 private
top-level defs, run because `get_db` showed this class of finding is real:

| Symbol | Location | Verdict |
| --- | --- | --- |
| `_startup_checks` | `app/fastapi_run.py:121` | **Not dead** — `@app.on_event("startup")` |
| `_resolve_focus_profile_model_for_user` | `app/fastapi_run.py:705` | **Unused.** 10 unexecuted statements. No caller anywhere |
| `_fallback_language_items` | `app/pipeline/scoring.py:307` | **Unused.** 7 unexecuted statements. Adjacent to **S9**, the missing language invariant |

Only 3 of 128 — the private surface is in much better shape than the public one.

### 2.2 Symbols that are public in name and internal in fact

Every reference inside the defining module. These are not deletion candidates; they are
`__init__.py` / naming candidates, and they matter because Slice 7's "public surface is what
`__init__.py` re-exports" rule will have to decide about each.

`profile_store.save_profiles` · `resume_parse.extract_text_from_file` ·
`resume_parse.parse_resume_text` · `smoke.search_stepstone_http` · `smoke.search_stepstone_pw` ·
`smoke.HEADERS` · `db.engine.make_engine` · `focus.load_focus_profiles` ·
`scoring.classify_blockers` · `scoring.apply_blocker_caps` · `scoring.compute_alpha` ·
`scoring.SCORING_VERSION` · `scoring.HEURISTIC_SCORING_VERSION` · `schemas.JobListItem` ·
`auth.deps.bearer` · `common.utils.atomic_write_text` · `models.FetchMeta` · `models.JobScoring` ·
`models.LLMDetail` · `llm_enrich.EnrichmentMeta` · plus 11 regex/constant module globals in
`stepstone/` and `polite_fetch`.

Two of these are load-bearing for §4: `models.FetchMeta` and `models.JobScoring` are referenced
**only** from inside the dead `models.JobDetailsResponse`. Removing that class makes both
zero-reference by cascade.

### 2.3 Most-referenced symbols

| Symbol | Defined in | Refs | app files | test files |
| --- | --- | ---: | ---: | ---: |
| `settings` | `config/settings.py` | 111 | 15 | 3 |
| `score_job` | `pipeline/scoring.py` | 73 | 2 | 8 |
| `app` | `fastapi_run.py` | 53 | 1 | 3 |
| `get_current_user` | `auth/deps.py` | 51 | 2 | 2 |
| `DEFAULT_FOCUS` | `config/focus.py` | 42 | 5 | 3 |
| `User` | `db/models.py` | 33 | 3 | 2 |
| `write_status` | `gui_runs/run_manager.py` | 32 | 1 | 5 |
| `extract_jobposting_from_html` | `pipeline/parsers.py` | 30 | 1 | 2 |
| `Resume` | `db/models.py` | 30 | 2 | 1 |
| `fetch_job_details` | `pipeline/pipeline.py` | 26 | 3 | 3 |
| `FocusProfileModel` | `pipeline/models.py` | 25 | 3 | 1 |
| `Base` | `db/base.py` | 22 | 1 | 2 |
| `FocusConfig` | `config/focus.py` | 21 | 4 | 2 |
| `Profile` | `db/models.py` | 19 | 2 | 1 |
| `db_session` | `db/session.py` | 18 | 2 | 0 |
| `FetchError` | `fetching/polite_fetch.py` | 18 | 5 | 0 |
| `ACCEPT_THRESHOLD` | `pipeline/potential_bucket.py` | 18 | 3 | 2 |

`settings` remains the universal sink: fan-out 0, referenced from 15 of the 25 non-`__init__`
`app` modules.

### 2.4 Where the LSP and grep disagree

Seventeen symbols where one method finds references and the other does not. **Sixteen are grep
false positives** — the symbol name is also a URL path segment, a template filename, or an
identically-named function elsewhere. Examples: `health` (27 grep hits, all `"/health"` strings),
`login` (26, all `/auth/login` and `/gui/login`), `gui_login` (1, the string `"gui_login.html"`),
`search_stepstone` (22, spread across four *different* functions with that name). On all sixteen,
**the LSP is right and grep is noise.**

**One disagreement is real and grep found something pyright's 0 could not explain:**

> `JobDetailsResponse` — LSP reports **0** references at `app/pipeline/models.py:82`; grep finds
> 7 occurrences of the name across `app/fastapi_run.py` and `app/api/schemas.py`.

Both are correct. There are **two distinct classes with the same name**:
`app/pipeline/models.py:82` and `app/api/schemas.py:103`. `fastapi_run.py:42` imports the schemas
one, so all seven grep hits belong to the live class and the `pipeline/models` class has no
consumer at all. Reported as a disagreement rather than resolved by picking a side — and the
resolution is that the disagreement was itself the finding.

### 2.5 Full table

<!-- Generated from pyright LSP findReferences; regenerate rather than hand-edit. -->

**`app/api/auth_routes.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 0 |  | 0 | 0 | `login` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `logout` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `me` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `signup` | route handler — registered by decorator |
| 5 |  | 2 | 0 | `router` |  |

**`app/api/schemas.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `JobListItem` |  |
| 2 |  | 1 | 0 | `AggregateReportRequest` |  |
| 2 |  | 1 | 0 | `BundleRequest` |  |
| 2 |  | 2 | 0 | `FetchMeta` |  |
| 2 |  | 1 | 0 | `JobDetailsRequest` |  |
| 2 |  | 1 | 0 | `LoginRequest` |  |
| 2 |  | 1 | 0 | `RunSingleRequest` |  |
| 2 |  | 1 | 0 | `SearchStepstoneListRequest` |  |
| 2 |  | 1 | 0 | `SignupRequest` |  |
| 3 |  | 1 | 0 | `LoginResponse` |  |
| 3 |  | 1 | 0 | `MeResponse` |  |
| 3 |  | 1 | 0 | `ResumeDetailResponse` |  |
| 3 |  | 1 | 0 | `ResumeListItem` |  |
| 3 |  | 1 | 0 | `SignupResponse` |  |
| 4 |  | 1 | 0 | `AggregateReportResponse` |  |
| 4 |  | 1 | 0 | `BundleResponse` |  |
| 4 |  | 1 | 0 | `ResumeUploadResponse` |  |
| 4 |  | 1 | 0 | `RunSingleResponse` |  |
| 4 |  | 2 | 0 | `ScoringResult` |  |
| 4 |  | 1 | 0 | `SearchStepstoneListResponse` |  |
| 4 |  | 2 | 0 | `UnifiedJobPostingOut` |  |
| 6 |  | 2 | 0 | `JobDetailsResponse` |  |

**`app/auth/constants.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 7 |  | 3 | 0 | `AUTH_COOKIE_NAME` |  |

**`app/auth/deps.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `bearer` |  |
| 51 |  | 2 | 2 | `get_current_user` |  |

**`app/auth/security.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 2 |  | 1 | 0 | `create_access_token` |  |
| 2 |  | 1 | 0 | `decode_token` |  |
| 2 |  | 1 | 0 | `hash_password` |  |
| 2 | yes | 1 | 0 | `pwd_context` |  |
| 2 |  | 1 | 0 | `verify_password` |  |

**`app/common/logging_ctx.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 0 |  | 0 | 0 | `clear_run_ctx` |  |
| 0 |  | 0 | 0 | `set_run_ctx` |  |
| 4 |  | 2 | 0 | `get_run_ctx` |  |
| 4 |  | 1 | 0 | `run_ctx_scope` |  |

**`app/common/utils.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `atomic_write_text` |  |
| 2 |  | 1 | 0 | `safe_filename` |  |
| 2 |  | 1 | 0 | `sha256_bytes` |  |
| 4 |  | 1 | 0 | `atomic_write_json` |  |
| 4 |  | 2 | 0 | `timestamp_iso` |  |
| 5 |  | 2 | 0 | `slugify` |  |
| 6 |  | 2 | 0 | `to_jsonable` |  |
| 15 |  | 5 | 0 | `ensure_dir` |  |

**`app/config/focus.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `load_focus_profiles` |  |
| 6 |  | 2 | 0 | `get_focus_config` |  |
| 21 |  | 4 | 2 | `FocusConfig` |  |
| 42 |  | 5 | 3 | `DEFAULT_FOCUS` |  |

**`app/config/profile_store.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 0 |  | 0 | 0 | `get_profile_keys` |  |
| 1 |  | 1 | 0 | `delete_profile` |  |
| 1 |  | 1 | 0 | `upsert_profile` |  |
| 2 |  | 1 | 0 | `get_default_profiles_dict` |  |
| 2 | yes | 1 | 0 | `save_profiles` |  |
| 3 |  | 1 | 0 | `get_profile` |  |
| 6 |  | 1 | 1 | `PROFILES_PATH` |  |
| 6 |  | 3 | 0 | `load_profiles` |  |

**`app/config/settings.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 2 | yes | 1 | 0 | `Settings` |  |
| 111 |  | 15 | 3 | `settings` |  |

**`app/db/base.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 22 |  | 1 | 2 | `Base` |  |

**`app/db/crud_profiles.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 0 |  | 0 | 0 | `update_profile_for_user` |  |
| 2 | yes | 1 | 0 | `create_profile_for_user` |  |
| 2 |  | 1 | 0 | `delete_profile_for_user` |  |
| 2 |  | 1 | 0 | `list_profiles_for_user` |  |
| 2 |  | 1 | 0 | `seed_default_profiles_for_user` |  |
| 3 |  | 1 | 0 | `get_focus_profile_model_for_user` |  |
| 5 |  | 1 | 1 | `upsert_profile_for_user` |  |
| 14 |  | 2 | 1 | `get_profile_for_user` |  |

**`app/db/crud_users.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 2 |  | 1 | 0 | `create_user` |  |
| 2 |  | 1 | 0 | `get_user_by_id` |  |
| 3 |  | 1 | 0 | `get_user_by_email` |  |

**`app/db/engine.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `make_engine` |  |
| 2 |  | 1 | 0 | `get_engine` |  |

**`app/db/health.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 3 |  | 1 | 0 | `check_db` |  |

**`app/db/models.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `RunItem` |  |
| 1 | yes | 1 | 0 | `UrlPoolEntry` |  |
| 3 | yes | 1 | 0 | `Run` |  |
| 19 |  | 2 | 1 | `Profile` |  |
| 30 |  | 2 | 1 | `Resume` |  |
| 33 |  | 3 | 2 | `User` |  |

**`app/db/session.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 2 | yes | 1 | 0 | `T` |  |
| 2 |  | 1 | 0 | `ping_db` |  |
| 5 |  | 2 | 1 | `SessionLocal` |  |
| 5 |  | 2 | 0 | `run_db_with_retries` |  |
| 11 |  | 5 | 0 | `is_transient_db_error` |  |
| 18 |  | 2 | 0 | `db_session` |  |

**`app/db/types.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 11 |  | 1 | 0 | `UtcDateTime` |  |
| 12 |  | 1 | 1 | `utcnow` |  |

**`app/fastapi_run.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 0 |  | 0 | 0 | `ProfileListItem` |  |
| 0 |  | 0 | 0 | `activate_resume` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `aggregate_report` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `bundle` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `delete_my_profile` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `delete_profile_api` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `get_my_me` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `get_my_profile` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `get_my_profile_latest` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `get_potential_application_detail` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `get_profile_api` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `get_resume_detail` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `get_run_logs` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `get_run_state` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `get_run_status` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `get_run_summary` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `gui_login` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `gui_logout` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `gui_profiles` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `gui_run` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `health` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `health_config` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `health_db` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `job_details` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `list_my_profiles` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `list_potential_applications` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `list_profiles` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `list_resumes` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `playwright_check` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `prune_profile_url_pool_stepstone` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `run_single` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `search_stepstone` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `search_stepstone_list` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `set_run_state` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `sqlalchemy_dbapi_error_handler` |  |
| 0 |  | 0 | 0 | `sqlalchemy_operational_error_handler` |  |
| 0 |  | 0 | 0 | `start_batch_run` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `upload_resume` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `upsert_my_profile` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `upsert_my_profile_by_key` | route handler — registered by decorator |
| 0 |  | 0 | 0 | `upsert_profile_api` | route handler — registered by decorator |
| 1 | yes | 1 | 0 | `MyProfileCreate` |  |
| 1 | yes | 1 | 0 | `MyProfileUpdate` |  |
| 1 | yes | 1 | 0 | `PRUNE_URL_POOL_CONCURRENCY_CAP` |  |
| 1 | yes | 1 | 0 | `PRUNE_URL_POOL_MAX_URLS_CAP` |  |
| 1 | yes | 1 | 0 | `PRUNE_URL_POOL_TIMEOUT_CAP` |  |
| 1 | yes | 1 | 0 | `PruneUrlPoolRequest` |  |
| 1 | yes | 1 | 0 | `StartBatchRunRequest` |  |
| 2 | yes | 1 | 0 | `Health` |  |
| 2 | yes | 1 | 0 | `MaintenanceRunResponse` |  |
| 2 | yes | 1 | 0 | `MeResponse` |  |
| 2 | yes | 1 | 0 | `PotentialApplicationDetailResponse` |  |
| 2 | yes | 1 | 0 | `RunLogsResponse` |  |
| 2 | yes | 1 | 0 | `RunSummaryResponse` |  |
| 2 | yes | 1 | 0 | `gui_login_redirect` |  |
| 2 | yes | 1 | 0 | `headless_mode` |  |
| 3 | yes | 1 | 0 | `PotentialApplicationListItem` |  |
| 3 | yes | 1 | 0 | `templates` |  |
| 3 | yes | 1 | 0 | `use_playwright_default` |  |
| 4 | yes | 1 | 0 | `BatchRunStatus` |  |
| 4 | yes | 1 | 0 | `PotentialApplicationsResponse` |  |
| 5 |  | 1 | 1 | `BatchSearchConfig` |  |
| 5 | yes | 1 | 0 | `RunState` |  |
| 11 | yes | 1 | 0 | `logger` |  |
| 17 | yes | 1 | 0 | `APP_STATE` |  |
| 53 |  | 1 | 3 | `app` |  |

**`app/fetching/http_client.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 4 |  | 2 | 0 | `fetch` |  |

**`app/fetching/polite_fetch.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `ACCESS_DENIED_MARKERS` |  |
| 1 | yes | 1 | 0 | `DEFAULT_ACCEPT_LANGUAGE` |  |
| 1 | yes | 1 | 0 | `DEFAULT_USER_AGENT` |  |
| 1 | yes | 1 | 0 | `DELAY_MAX` |  |
| 1 | yes | 1 | 0 | `DELAY_MIN` |  |
| 1 | yes | 1 | 0 | `FETCH_TIMEOUT` |  |
| 1 | yes | 1 | 0 | `HTTP_BACKOFF_BASE` |  |
| 1 | yes | 1 | 0 | `PLAYWRIGHT_TIMEOUT_MS` |  |
| 1 | yes | 1 | 0 | `PLAYWRIGHT_WAIT_UNTIL` |  |
| 1 | yes | 1 | 0 | `headless_mode` |  |
| 1 | yes | 1 | 0 | `use_playwright_default` |  |
| 2 | yes | 1 | 0 | `FAILURE_BACKOFF` |  |
| 2 | yes | 1 | 0 | `ROBOTS_TTL` |  |
| 2 | yes | 1 | 0 | `RobotsEntry` |  |
| 2 | yes | 1 | 0 | `STATE_INIT_LOCK` |  |
| 3 | yes | 1 | 0 | `DOMAIN_STATE` |  |
| 3 | yes | 1 | 0 | `DomainState` |  |
| 3 | yes | 1 | 0 | `HTTP_RETRIES` |  |
| 3 | yes | 1 | 0 | `ROBOTS_CACHE` |  |
| 3 | yes | 1 | 0 | `ROBOTS_LOCKS` |  |
| 6 |  | 3 | 0 | `fetch_job_html` |  |
| 8 |  | 3 | 0 | `TransientFetchError` |  |
| 9 |  | 5 | 0 | `RobotsDisallowedError` |  |
| 11 |  | 5 | 0 | `AccessDeniedError` |  |
| 18 |  | 5 | 0 | `FetchError` |  |

**`app/gui_runs/run_manager.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 2 | yes | 1 | 0 | `RUN_INDEX_DIR` |  |
| 2 | yes | 1 | 0 | `get_run_dir_from_index` |  |
| 2 | yes | 1 | 0 | `status_path` |  |
| 3 |  | 2 | 1 | `latest_path` |  |
| 4 | yes | 1 | 0 | `LEGACY_OUTPUT_ROOT` |  |
| 5 |  | 2 | 1 | `log_path` |  |
| 5 | yes | 1 | 0 | `run_output_root` |  |
| 6 |  | 1 | 3 | `create_run_dir` |  |
| 6 |  | 1 | 2 | `write_latest` |  |
| 8 | yes | 1 | 0 | `OUTPUTS_BASE` |  |
| 8 |  | 1 | 2 | `get_run_dir` |  |
| 10 |  | 2 | 1 | `LOG_CHUNK_MAX_BYTES` |  |
| 12 |  | 1 | 2 | `load_status` |  |
| 15 |  | 1 | 1 | `read_log_chunk` |  |
| 32 |  | 1 | 5 | `write_status` |  |

**`app/pipeline/llm_enrich.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 2 | yes | 1 | 0 | `EnrichmentMeta` |  |
| 2 |  | 1 | 0 | `llm_score_job` |  |
| 5 |  | 2 | 1 | `enrich_jobposting` |  |
| 7 |  | 2 | 0 | `LLM_SCORING_VERSION` |  |

**`app/pipeline/models.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 0 |  | 0 | 0 | `JobDetailsResponse` |  |
| 1 | yes | 1 | 0 | `FetchMeta` |  |
| 1 | yes | 1 | 0 | `JobScoring` |  |
| 1 | yes | 1 | 0 | `LLMDetail` |  |
| 2 | yes | 1 | 0 | `BlockerCaps` |  |
| 2 | yes | 1 | 0 | `Constraints` |  |
| 15 |  | 4 | 4 | `UnifiedJobPosting` |  |
| 25 |  | 3 | 1 | `FocusProfileModel` |  |

**`app/pipeline/output.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 8 |  | 3 | 1 | `write_summary` |  |
| 10 |  | 3 | 2 | `write_bundle` |  |

**`app/pipeline/parsers.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 30 |  | 1 | 2 | `extract_jobposting_from_html` |  |

**`app/pipeline/pipeline.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 0 |  | 0 | 0 | `CachePayload` |  |
| 6 |  | 2 | 1 | `write_job_bundle` |  |
| 26 |  | 3 | 3 | `fetch_job_details` |  |

**`app/pipeline/potential_bucket.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 2 |  | 1 | 0 | `decide_potential` |  |
| 5 | yes | 1 | 0 | `PotentialDecision` |  |
| 18 |  | 3 | 2 | `ACCEPT_THRESHOLD` |  |

**`app/pipeline/resume_parse.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `extract_text_from_file` |  |
| 1 | yes | 1 | 0 | `parse_resume_text` |  |
| 2 |  | 1 | 0 | `parse_resume_file` |  |

**`app/pipeline/scoring.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 0 |  | 0 | 0 | `COMPONENT_FUNCS` |  |
| 1 | yes | 1 | 0 | `DEFAULT_HEURISTIC_WEIGHTS` |  |
| 1 | yes | 1 | 0 | `HEURISTIC_SCORING_VERSION` |  |
| 1 | yes | 1 | 0 | `LANG_PATTERNS` |  |
| 1 | yes | 1 | 0 | `PUBLIC_SECTOR` |  |
| 1 | yes | 1 | 0 | `SCORING_VERSION` |  |
| 1 | yes | 1 | 0 | `apply_blocker_caps` |  |
| 1 | yes | 1 | 0 | `classify_blockers` |  |
| 1 | yes | 1 | 0 | `compute_alpha` |  |
| 2 | yes | 1 | 0 | `FALLBACK_VAGUE_PENALTY` |  |
| 2 | yes | 1 | 0 | `HeuristicWeights` |  |
| 2 | yes | 1 | 0 | `aggregate_heuristic` |  |
| 2 | yes | 1 | 0 | `apply_employment_type` |  |
| 2 | yes | 1 | 0 | `apply_experience` |  |
| 2 | yes | 1 | 0 | `apply_location` |  |
| 2 | yes | 1 | 0 | `apply_seniority` |  |
| 2 | yes | 1 | 0 | `apply_skills` |  |
| 2 | yes | 1 | 0 | `resolve_language_items` |  |
| 3 | yes | 1 | 0 | `apply_language` |  |
| 5 | yes | 1 | 0 | `GERMAN_HEAVY_CONTEXT` |  |
| 15 | yes | 1 | 0 | `HeuristicComponentResult` |  |
| 73 |  | 2 | 8 | `score_job` |  |

**`app/pipeline/state.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 2 | yes | 1 | 0 | `CACHE_DIR` |  |
| 2 | yes | 1 | 0 | `STATE_DIR` |  |
| 3 | yes | 1 | 0 | `STATE_FILE` |  |
| 4 | yes | 1 | 0 | `DEFAULT_STATE` |  |
| 5 |  | 2 | 1 | `cache_put` |  |
| 6 |  | 2 | 2 | `cache_get` |  |
| 7 |  | 3 | 1 | `load_state` |  |
| 7 |  | 3 | 1 | `save_state` |  |

**`app/pipeline/templating.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 2 | yes | 1 | 0 | `TEMPLATES_DIR` |  |
| 8 |  | 3 | 1 | `generate_bundle` |  |

**`app/pipeline/url_pool.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 2 |  | 1 | 0 | `append_pool_entries` |  |
| 2 |  | 1 | 0 | `load_pool_set` |  |
| 4 |  | 2 | 0 | `pool_path_for_profile` |  |
| 8 |  | 3 | 0 | `normalize_url` |  |

**`app/pipeline/url_pool_maintenance.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `UNAVAILABLE_MARKERS` |  |
| 2 |  | 1 | 0 | `prune_unavailable_stepstone_urls` |  |

**`app/prefect_run.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 3 | yes | 1 | 0 | `RUNS_BASE_DIR` |  |
| 3 |  | 2 | 0 | `crawl_and_save_flow` |  |
| 3 |  | 2 | 0 | `process_run_flow` |  |
| 15 |  | 2 | 0 | `SeedConfig` |  |

**`app/stepstone/dates.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `ABSOLUTE_DATE_RE` |  |
| 1 | yes | 1 | 0 | `RELATIVE_RE` |  |
| 1 | yes | 1 | 0 | `UNIT_TO_DELTA` |  |
| 6 |  | 3 | 0 | `isoformat_utc` |  |
| 6 |  | 3 | 0 | `parse_stepstone_listing_date` |  |
| 8 |  | 3 | 0 | `parse_iso8601_utc` |  |

**`app/stepstone/search_http.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `JSON_LD_RE` |  |
| 1 | yes | 1 | 0 | `PAGE_LAST_RE` |  |
| 1 | yes | 1 | 0 | `RESULT_COUNT_RE` |  |
| 2 | yes | 1 | 0 | `PER_PAGE_DEFAULT` |  |
| 3 | yes | 1 | 0 | `JOB_LINK_RE` |  |
| 4 |  | 3 | 0 | `search_stepstone` | route handler — registered by decorator |

**`app/stepstone/search_playwright.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `JSON_LD_RE` |  |
| 1 | yes | 1 | 0 | `PAGE_LAST_RE` |  |
| 1 | yes | 1 | 0 | `RESULT_COUNT_RE` |  |
| 2 | yes | 1 | 0 | `JOB_LINK_RE` |  |
| 2 | yes | 1 | 0 | `PER_PAGE_DEFAULT` |  |
| 4 |  | 3 | 0 | `search_stepstone_pw` |  |

**`app/stepstone/smoke.py`**

| Refs | Own-module only | app files | test files | Symbol | Note |
| ---: | :---: | ---: | ---: | --- | --- |
| 1 | yes | 1 | 0 | `HEADERS` |  |
| 1 | yes | 1 | 0 | `search_stepstone_http` |  |
| 1 | yes | 1 | 0 | `search_stepstone_pw` |  |
| 4 |  | 2 | 1 | `search_stepstone` | route handler — registered by decorator |

---

## 3. Zero-execution functions

From `pytest -q --cov=app --cov-report=json` on the gate configuration (`--ignore=tests/legacy`,
`-m 'not external'`). A function is listed when **no statement in its body executed**. 113
functions across 22 modules.

Read this as *untested*. It becomes evidence of *unused* only where §2 also shows no references,
and those rows are called out in §4.

### 3.1 Module coverage

| Module | Stmts | Miss | Cov | | Module | Stmts | Miss | Cov |
| --- | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: |
| `prefect_run.py` | 333 | 333 | **0%** | | `db/engine.py` | 36 | 18 | 50% |
| `stepstone/search_playwright.py` | 236 | 210 | 11% | | `resume_parse.py` | 113 | 55 | 51% |
| `stepstone/dates.py` | 92 | 79 | 14% | | `fastapi_run.py` | 1109 | 515 | 54% |
| `stepstone/search_http.py` | 192 | 166 | 14% | | `db/session.py` | 56 | 24 | 57% |
| `pipeline/url_pool_maintenance.py` | 150 | 128 | **15%** | | `auth/deps.py` | 39 | 16 | 59% |
| `fetching/polite_fetch.py` | 310 | 249 | 20% | | `pipeline/state.py` | 109 | 40 | 63% |
| `pipeline/llm_enrich.py` | 131 | 105 | 20% | | `db/health.py` | 12 | 4 | 67% |
| `pipeline/url_pool.py` | 41 | 32 | 22% | | `db/crud_profiles.py` | 112 | 28 | 75% |
| `stepstone/smoke.py` | 35 | 26 | **26%** | | `pipeline/scoring.py` | 535 | 113 | 79% |
| `fetching/http_client.py` | 6 | 4 | 33% | | `gui_runs/run_manager.py` | 142 | 23 | 84% |
| `common/logging_ctx.py` | 32 | 20 | 38% | | `db/types.py` | 25 | 4 | 84% |
| `config/focus.py` | 92 | 57 | **38%** | | `config/profile_store.py` | 41 | 6 | **85%** |

Remaining modules are ≥ 83%. **Total: 4,887 statements, 2,317 missed, 53%.**

Two rows worth stopping on, because they change what backlog D says:

- **`config/focus.py` at 38%** is not in bucket D but is lower than `profile_store.py`, which is.
  `FocusConfig.from_profile` (21 statements), `_load_focus_profile_override` (20),
  `load_focus_profiles` (9) and `get_focus_config` (7) never execute in the gate. `DEFAULT_FOCUS`
  has 42 references. **This is the least-tested well-used module in the repo.**
- **`config/profile_store.py` is 85%**, not the "32%" D2 records. D2's coverage premise is stale.

### 3.2 Per-module detail

**`app/auth/deps.py`** — module coverage 59%, 1 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 45 | `get_current_user._op` | 1 |

**`app/common/logging_ctx.py`** — module coverage 38%, 3 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 15 | `set_run_ctx` | 8 |
| 31 | `clear_run_ctx` | 1 |
| 36 | `run_ctx_scope` | 11 |

**`app/config/focus.py`** — module coverage 38%, 4 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 56 | `FocusConfig.from_profile` | 21 |
| 107 | `load_focus_profiles` | 9 |
| 121 | `_load_focus_profile_override` | 20 |
| 154 | `get_focus_config` | 7 |

**`app/config/profile_store.py`** — module coverage 85%, 1 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 39 | `get_profile_keys` | 1 |

**`app/db/crud_profiles.py`** — module coverage 75%, 1 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 89 | `update_profile_for_user` | 15 |

**`app/db/crud_users.py`** — module coverage 88%, 1 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 16 | `get_user_by_id` | 2 |

**`app/db/session.py`** — module coverage 57%, 1 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 34 | `is_transient_db_error` | 12 |

**`app/fastapi_run.py`** — module coverage 54%, 23 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 142 | `sqlalchemy_operational_error_handler` | 1 |
| 153 | `sqlalchemy_dbapi_error_handler` | 3 |
| 343 | `_TemporaryEnv.__init__` | 2 |
| 347 | `_TemporaryEnv.__enter__` | 3 |
| 352 | `_TemporaryEnv.__exit__` | 4 |
| 360 | `_filter_listings_by_cutoff` | 20 |
| 412 | `search_stepstone_list` | 6 |
| 449 | `job_details` | 35 |
| 524 | `bundle` | 14 |
| 561 | `aggregate_report` | 6 |
| 705 | `_resolve_focus_profile_model_for_user` | 10 |
| 1133 | `_slugify` | 2 |
| 1138 | `_build_seeds_from_focus` | 15 |
| 1167 | `_build_seeds_from_urls` | 10 |
| 1189 | `_run_prefect_batch` | 101 |
| 1371 | `_run_prefect_inprocess_batch` | 105 |
| 1542 | `_run_prune_url_pool` | 41 |
| 1572 | `_run_prune_url_pool._LogSink.__init__` | 1 |
| 1575 | `_run_prune_url_pool._LogSink._write` | 2 |
| 1579 | `_run_prune_url_pool._LogSink.info` | 1 |
| 1582 | `_run_prune_url_pool._LogSink.warning` | 1 |
| 1585 | `_run_prune_url_pool._LogSink.error` | 1 |
| 1627 | `run_single` | 29 |

**`app/fetching/http_client.py`** — module coverage 33%, 1 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 6 | `fetch` | 4 |

**`app/fetching/polite_fetch.py`** — module coverage 20%, 14 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 87 | `_get_domain_state` | 9 |
| 99 | `_get_robots_lock` | 9 |
| 111 | `_fetch_robots` | 18 |
| 139 | `_robots_parser` | 14 |
| 157 | `_ensure_robots_allowed` | 6 |
| 170 | `_mark_success` | 3 |
| 176 | `_mark_failure` | 3 |
| 182 | `_respect_rate_limit` | 17 |
| 205 | `_looks_access_denied` | 2 |
| 210 | `_http_attempt` | 42 |
| 320 | `_playwright_attempt` | 55 |
| 417 | `_decide_backend_order` | 7 |
| 427 | `_http_retry_backoff` | 3 |
| 433 | `fetch_job_html` | 61 |

**`app/pipeline/llm_enrich.py`** — module coverage 20%, 7 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 18 | `_client` | 3 |
| 34 | `_build_system_prompt` | 2 |
| 73 | `_safe_jsonable` | 9 |
| 86 | `_load_resume_snapshot` | 11 |
| 105 | `_build_user_prompt` | 15 |
| 141 | `enrich_jobposting` | 29 |
| 213 | `llm_score_job` | 36 |

**`app/pipeline/output.py`** — module coverage 87%, 1 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 27 | `_safe_folder` | 2 |

**`app/pipeline/resume_parse.py`** — module coverage 51%, 1 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 113 | `_collect_bullets` | 6 |

**`app/pipeline/scoring.py`** — module coverage 79%, 1 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 307 | `_fallback_language_items` | 7 |

**`app/pipeline/state.py`** — module coverage 63%, 2 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 57 | `_stable_json` | 14 |
| 78 | `_focus_fingerprint` | 4 |

**`app/pipeline/url_pool.py`** — module coverage 22%, 4 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 9 | `pool_path_for_profile` | 1 |
| 13 | `normalize_url` | 6 |
| 22 | `load_pool_set` | 16 |
| 41 | `append_pool_entries` | 9 |

**`app/pipeline/url_pool_maintenance.py`** — module coverage 15%, 9 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 30 | `_RateLimiter.__init__` | 4 |
| 36 | `_RateLimiter.wait_turn` | 5 |
| 44 | `_now_iso` | 1 |
| 48 | `_append_unavailable` | 6 |
| 63 | `_load_pool_entries` | 22 |
| 88 | `_telemetry_status_hint` | 9 |
| 100 | `_check_unavailable_polite` | 17 |
| 134 | `prune_unavailable_stepstone_urls` | 64 |
| 157 | `prune_unavailable_stepstone_urls._run` | 33 |

**`app/prefect_run.py`** — module coverage 0%, 12 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 49 | `_iso_timestamp` | 1 |
| 53 | `_resolve_seed_configs` | 24 |
| 89 | `_load_state_task` | 1 |
| 94 | `_save_state_task` | 1 |
| 99 | `_search_seed_task` | 3 |
| 128 | `_write_seed_urls` | 25 |
| 176 | `_process_job_task` | 29 |
| 303 | `crawl_and_save_flow` | 18 |
| 340 | `process_run_flow` | 135 |
| 600 | `_load_seeds_from_path` | 2 |
| 605 | `_parse_cli_args` | 21 |
| 669 | `_cli_entry` | 18 |

**`app/stepstone/dates.py`** — module coverage 14%, 7 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 30 | `_now_utc` | 1 |
| 34 | `parse_iso8601_utc` | 12 |
| 49 | `isoformat_utc` | 1 |
| 53 | `_apply_months` | 7 |
| 64 | `_apply_years` | 6 |
| 74 | `_days_in_month` | 5 |
| 83 | `parse_stepstone_listing_date` | 47 |

**`app/stepstone/search_http.py`** — module coverage 14%, 8 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 35 | `_abs` | 1 |
| 39 | `_extract_job_links` | 11 |
| 58 | `_with_page` | 9 |
| 74 | `_extract_posted_label` | 11 |
| 88 | `_extract_job_entries` | 28 |
| 144 | `_estimate_total_pages` | 30 |
| 188 | `_find_total_in_jsonld` | 21 |
| 212 | `search_stepstone` | 55 |

**`app/stepstone/search_playwright.py`** — module coverage 11%, 8 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 34 | `_with_page` | 9 |
| 49 | `_estimate_total_pages_from_html` | 30 |
| 82 | `_find_total_in_jsonld` | 21 |
| 106 | `_accept_cookies` | 8 |
| 122 | `_extract_links` | 12 |
| 140 | `_extract_job_entries` | 41 |
| 202 | `_safe_goto` | 22 |
| 237 | `search_stepstone_pw` | 67 |

**`app/stepstone/smoke.py`** — module coverage 26%, 3 zero-execution functions

| Line | Function | Unexecuted statements |
| ---: | --- | ---: |
| 18 | `search_stepstone_http` | 7 |
| 40 | `search_stepstone_pw` | 13 |
| 69 | `search_stepstone` | 6 |

---

## 4. Ranked deletion candidates

Ranked by (weakness of the usage evidence) × (simplification value). **Every row states which
the evidence supports: `UNUSED` — no reference and no caller; `UNTESTED` — used, but the gate
never executes it; `MISWIRED` — used, but the only call path is broken.** No row is recommended
for deletion on coverage alone.

| Rank | Candidate | Verdict | Evidence | What depends on it | What removing it simplifies |
| ---: | --- | --- | --- | --- | --- |
| **1** | `JobDetailsResponse` in `app/pipeline/models.py:82`, and by cascade `FetchMeta:87` and `JobScoring:86` in the same file | **UNUSED** | LSP: 0 refs. `app/api/schemas.py:103` defines a live class of the same name; `fastapi_run.py:42` imports *that* one. `FetchMeta`/`JobScoring` in `pipeline/models.py` are referenced only from inside the dead class | Nothing | Removes a same-name shadow pair. Directly reduces **Slice 2.9**: `AGENTS.md` lists all three as `domain/` types to move, and three of the eight would be moving dead code |
| **2** | `app/pipeline/url_pool_maintenance.py` (**D4**) | **MISWIRED, then UNTESTED** | 15% coverage; `prune_unavailable_stepstone_urls` has 2 refs, both in `fastapi_run.py` — the import at :88 and the call at **:1593, inside the block A1 makes unreachable**. So the feature has never run through its endpoint | Only route #15, which returns 200 and a `failed` artifact | Nothing yet — **decide after A1 is fixed, not before.** Deleting now would delete a feature nobody has ever been able to evaluate. This is D4's own stated condition |
| **3** | `app/stepstone/smoke.py` (**D3**) | **UNTESTED, and misnamed rather than unused** | 26% coverage. `smoke.search_stepstone` has 4 refs: `fastapi_run.py:92` (as `ss_search`), two in `stepstone/__init__.py`, one legacy test. **But read the body** — it GETs a URL and returns `{"ok","backend","url","final_url","title"}`. It parses a page `<title>`. It returns no job listings | Route #5 `/search_stepstone` — a public, unauthenticated dev smoke endpoint — and `tests/conftest.py::stub_stepstone_adapter` | **D3's premise needs correcting.** It is *not* the backend-dispatch façade: the real search is `search_http.search_stepstone` / `search_playwright.search_stepstone_pw`, imported separately as `crawl_http` / `crawl_pw` and used by route #6. `smoke.py` is exactly the "dev scratch harness" D3 suspected. Removing it deletes a route, a module and (per §1.2) an open 400-becomes-500 bug |
| **4** | `_resolve_focus_profile_model_for_user` (`fastapi_run.py:705`) | **UNUSED** | LSP: 0 refs across `app/`, `tests/`, `scripts/`. 10 unexecuted statements. It is the *only* consumer of the `profile_store.get_profile` fallback at :715 | Nothing | Removes one of the four `_profile_payload_from_db` call sites and one of the two "DB profile, else file profile" precedence rules in the codebase. Makes **D2** a smaller question |
| **5** | `GET`/`POST` `/run_state` (routes #37, #38) + `pipeline/state.py::load_state`/`save_state` as an HTTP surface | **UNUSED as a route** | No template, no client, no `n8n` node references either path. Unauthenticated. Backed by a single global `STATE_FILE` with no tenant scoping | `load_state`/`save_state` themselves have 7 refs each and are used by `prefect_run.py` — **the functions stay**; only the two routes are candidates | Removes 2 of the 18 unauthenticated routes and one un-tenanted global-state surface. Feeds **A12** |
| **6** | `set_run_ctx`, `clear_run_ctx` (`common/logging_ctx.py`), `ProfileListItem` (`fastapi_run.py:573`), `CachePayload` (`pipeline/pipeline.py:29`), `COMPONENT_FUNCS` (`scoring.py:794`), `get_profile_keys` (`profile_store.py:39`), `update_profile_for_user` (`crud_profiles.py:89`), `_fallback_language_items` (`scoring.py:307`) | **UNUSED** (all eight) | LSP: 0 refs each; all eight also have zero executed statements | Nothing | Small but free. `COMPONENT_FUNCS` and `_fallback_language_items` are worth flagging to whoever picks up **S9** — a dead component registry beside a scoring component with no invariant is a hint about how that gap arose |
| **7** | `app/config/profile_store.py`, the file-backed store (**D2**) | **NOT a deletion candidate as recorded** | 85% coverage, not 32%. Live consumers: `list_profiles` (#10), `get_profile_api`/`upsert_profile_api`/`delete_profile_api` (#23–25), `_resolve_focus_profile_model_for_user`, `config/focus.py:110`, and — decisively — **`db/crud_profiles.py:166`, where `seed_default_profiles_for_user` seeds every new user's DB profiles from `get_default_profiles_dict()`** | Signup (`auth_routes.py:44`) depends on it | **D2 is not "which store is canonical"** — the file store is the *seed source and read fallback* for the DB store, not a parallel implementation of it. It cannot be deleted without deciding what a new user's default profiles come from. Restated as **Q2** in §5 |
| **8** | SQL Server / `pyodbc` (**D1**) | **Cannot be settled statically** | `mssql+pyodbc` appears in exactly one place in `app/`: `_ensure_connect_timeout` in `db/engine.py` (~30 lines, `50%` covered). `pyodbc~=5.3` is pinned in `pyproject.toml` and `requirements.lock.txt`. `app/db/types.py` already exists to make the ORM dialect-neutral | The `unixodbc` layer in the Dockerfile; the byte-identical-DDL exception in `AGENTS.md`; backlog A10 | **One measured correction.** `Dockerfile:54-56` justifies installing `unixodbc` with "`import pyodbc` happens at import time via `app.db`". It does not: importing `app.fastapi_run` under a SQLite URL leaves `pyodbc` out of `sys.modules`. The stated reason for that apt layer is wrong. Whether the pin is needed at all is **D1**, a deployment question — **Q3** in §5 |
| **9** | `n8n workflows/` (**D6**) | **UNUSED by the app; and D6's premise is wrong** | Not untracked — `git ls-files` returns `n8n workflows/job_agent_l7.json`, committed in `5d9facb` ("Initial commit"), last modified **2025‑10‑27**, 2,418 bytes, one file. It posts to `http://127.0.0.1:5001/job_details` and `/bundle` | Nothing in `app/` or `tests/` depends on it. It is, however, **the only first-party caller of routes #7 and #8** | Deleting it makes `/job_details` and `/bundle` caller-less, which changes their status in §1. Sequence D6 **before** any decision about those two routes, not after |
| **10** | `app/pipeline/resume_parse.py` (**D5**) | **UNTESTED, definitely not unused** | 51% coverage — not "12%, the lowest of any non-zero module"; that figure is stale and five modules are now lower. `parse_resume_file` has 2 refs, both in `fastapi_run.py`; `extract_text_from_file` and `parse_resume_text` are called only from inside `parse_resume_file` | Route #16, and every downstream résumé snapshot | **Do not delete.** The real defect is §1.3: the single call site swallows every parse failure into `pass` and the response has no field that could report it. D5 asks "which formats are really used" — **that cannot be answered from this code, because the code cannot tell you when a format failed.** Fix the swallow first; the telemetry it unblocks is the answer to D5 |
| — | Prefect orchestration (**D7**) | **Not a candidate** | 0% coverage, 333 statements. `crawl_and_save_flow` and `process_run_flow` have 3 refs each — `prefect_run.py` itself plus the lazy import at `fastapi_run.py:1383` | Routes #29 (both orchestrator branches) | Confirmed as recorded: ADR 0006 makes it an opt-in backend. Listed only to close the loop |

### 4.1 What this list deliberately does not say

- **Nothing is recommended for deletion on coverage alone.** The five lowest-coverage modules
  (`prefect_run` 0%, `search_playwright` 11%, `dates` 14%, `search_http` 14%,
  `polite_fetch` 20%) are all **UNTESTED and demonstrably used** — they are the crawl path, which
  is `external`-marked by design (TEST-STRATEGY §3). Ranks 2, 3, 7, 8, 9 and 10 all *revise*
  a bucket-D premise rather than confirming it.
- **`app/db/models.py` symbols are excluded** from every ranking. `alembic/` is outside pyright's
  `include`, so `RunItem` (1 ref) and `UrlPoolEntry` (1 ref) are undercounts by construction.
- **Ranks 1, 4 and 6 are the only rows where deletion is supported by the evidence as it stands.**
  Everything else needs either a decision (2, 7, 8, 9) or a bugfix first (2, 10).

---

## 5. Open questions

Everything the static evidence could not settle. **Stated as questions, not guesses** — a guess
recorded here as a finding is worse than a gap, because CP‑3 will act on it.

**Q1 — Which router module do the four ambiguous routes land in?**
`refactor-plan.md` §Slice 7 names seven modules for 38 handlers but does not assign them. Four
have no obvious home, and the choice is a real API-shape decision, not a filing one:
`/api/my/me` (profiles or auth?); `/api/my/profile/{profile_key}/latest` (profiles, or runs —
it reads a run artifact); `/api/my/profile/{profile_key}/url_pool/prune_stepstone` (profiles, or
runs — it mints a `run_id`); `/api/run_summary/{run_id}` (runs, or artifacts — the two
`/api/run_artifacts/` routes are unambiguously artifacts and this reads the same directory).
Slice 7's acceptance test is that the route table is byte-identical, so this does not block the
slice — but the answer should be written down before it, not discovered during it.

**Q2 — If `profile_store.py` goes, where do a new user's default profiles come from?**
D2 is recorded as "which is canonical". The evidence says that is the wrong question: the file
store is the **seed source** for the DB store (`crud_profiles.py:166` →
`get_default_profiles_dict()`, called from signup at `auth_routes.py:44`) and the **read
fallback** when a user has no row (`_resolve_focus_profile_model_for_user:715`,
`get_profile_api:1086`). Deleting it requires deciding whether defaults become a checked-in JSON
asset, a migration, or code. Which?

**Q3 — Is any real deployment on SQL Server?** Unanswerable from the repo, and it is D1's actual
gate. What the repo does say: `app/db/types.py` already removes the dialect-specific column
types, the mssql-specific code in `app/` is one ~30-line function in `db/engine.py`, and the
`unixodbc` layer's stated justification is measurably false (§4 rank 8). If the answer is "no",
`pyodbc`, that function and the byte-identical-DDL exception all go together.

**Q4 — `/search_stepstone`: fix the 400-becomes-500 bug, or delete the route with `smoke.py`?**
Both are defensible and doing both wastes the fix. The route is public and unauthenticated (A12),
it returns a page `<title>` rather than jobs, and `stub_stepstone_adapter` — built during CP1-8
precisely to keep this route offline — binds `ss_search`, so a deletion breaks that fixture
loudly and deliberately. Deciding D3 first makes the bugfix unnecessary; deciding it late means
fixing code that is about to be removed.

**Q5 — Is `n8n workflows/` live?** It is tracked, ten months old, and the only first-party caller
of `/job_details` and `/bundle`. Whether those two routes have a real consumer depends entirely
on whether that prototype still runs somewhere. Nothing in the repo can answer it. If it is dead,
routes #7 and #8 join the caller-less set and their status in §1 changes.

**Q6 — What is `config/focus.py` for, given `FocusConfig.from_profile` never runs?**
38% coverage, four functions with zero executed statements, 42 references to `DEFAULT_FOCUS`, and
a `TYPE_CHECKING` guard plus two function-local imports that exist solely to keep it out of
`app.pipeline`'s import cycle. It is not in bucket D and probably should not be deleted — but it
is less exercised than two modules that *are* in bucket D, and the audit found no answer to what
`_load_focus_profile_override` (20 unexecuted statements) is meant to do or who sets it.

**Q7 — Should the four `422 only` routes get behavioural tests before or after the split?**
`/search_stepstone_list`, `/job_details`, `/bundle`, `/aggregate_report` and `/api/run_single`
have tests that assert schema rejection and nothing else — 0 of 90 handler statements execute
across the five. `/job_details` and `/api/run_single` are the two heaviest (35 and 29 statements)
and both go through the full fetch → enrich → score path. Slice 6 will extract services from
exactly these bodies with no behavioural oracle in place. This is not a deletion question; it is
the one place where the audit found coverage that would not catch a refactor breaking something.

**Q8 — Does the runtime pass get run?** `liveness-audit.md` §"Manual step the audit cannot do"
specifies a `coverage run` session against the real GUI. It has not been done, and nothing in
§1–§4 substitutes for it: static analysis distinguishes *unreferenced* from *referenced*, never
*used* from *unused*. Routes #12, #14, #18, #21, #24, #25, #37, #38, #41 and #42 have no caller
in the repo and would be the first rows that pass could settle.
