# Chat Checkpoints

Where execution stops and a decision goes back to Chat.

Four planned checkpoints, plus rules for unplanned escalation. Everything between them
runs in Claude Code and Codex with no Chat involvement.

---

## How this works

Add to `AGENTS.md` (and therefore `CLAUDE.md`):

> **Before starting any slice, read `docs/CHAT-CHECKPOINTS.md`.** If a checkpoint blocks
> it, stop, print the handoff prompt from §Handoff below with the placeholders filled in,
> and do not begin the slice. If a §Escalation trigger fires mid-slice, stop at that
> point and do the same.

Chat has direct read access to this repo, so a handoff prompt is a **pointer, not a
paste**. Name the files; do not copy their contents in.

---

## The checkpoints

| ID | When | Blocks | Size |
| --- | --- | --- | --- |
| **CP‑1** | After Slice 1 is green | Slice 3 and everything after | ~20 min |
| **CP‑2** | After the Slice 2.5 spike | Slice 3 | 5 min, or long if it failed |
| **CP‑3** | Before Slice 3 | Slices 3–7 | **Long — the big one** |
| **CP‑4** | Before Phase 5 | The whole UI track | Long |

---

### CP‑1 — Oracle review

**Trigger:** Slice 1's 11-item checklist is complete and the gate is green.

**Why.** The contract suite is what every later gate is graded against. If it is wrong,
every green result after it is meaningless. This is the one place
[AGENT-WORKFLOW.md](AGENT-WORKFLOW.md) §6 says Chat should read code rather than a
report.

**Chat reads:** `tests/contracts/`, `tests/unit/test_scoring_invariants.py`,
`tests/conftest.py`.

**Looking for:** tests that bind to internals rather than contracts; absolute-value
assertions that should be relational; anything depending on `DEFAULT_FOCUS` implicitly;
routes silently missing from the 38.

**Exit:** the suite is trustworthy as an oracle, or a fix list exists.

---

### CP‑2 — Spike verdict

**Trigger:** the `spike/inprocess-batch` branch has produced a result.

**Passed** (batch completes with no Prefect server, no `PREFECT_API_URL`): a one-line
confirmation, delete the branch, carry on. Five minutes.

**Failed:** this is a real conversation. Slice 8 becomes a rewrite rather than a
refactor, its risk rating changes, and it may need to move earlier in the order so the
one-click goal is not stranded behind seven slices of restructuring.

**Chat reads:** the paragraph recorded in `refactor-plan.md` §6, plus the spike branch's
run directory if it failed.

---

### CP‑3 — Feature and deletion session ⟵ the big one

**Trigger:** Slices 0, 1, 2, 2.5 complete. **Slice 3 must not start.**

**Why here.** Slice 3 moves `stepstone/` into `sources/`, and backlog **D3**
(`stepstone/smoke.py`) is a deletion candidate inside it. Every feature dropped after
it has been moved, wrapped in a service, and given a router is three slices of wasted
work. Deletions are cheapest decided before the first structural move touches them.

**Prerequisite — the liveness audit. ✅ DONE 2026-08-07**, output at
[liveness-report.md](liveness-report.md), 1,174 lines. Walk in confirming or overruling its
table, not reviewing 40 features from memory.

**It corrected four bucket-D premises** (D2, D3, D5, D6) and found two new bugs (A17, A18).
`backlog.md` bucket D is amended in place with pointers; **read the report's §4 and §5 anyway**
— a corrected premise leaves the decision open, and three D items now turn on a different
question than the one first recorded.

**Chat reads:** [liveness-report.md](liveness-report.md) §4 and §5 **first**, then
`docs/backlog.md` buckets C and D, `docs/refactor-plan.md` §3. `docs/architecture.md` §7 is
stale in three known places — see the banner at the top of that file.

**One thing the audit could not do, and it is yours, not an agent's.**
[liveness-audit.md](liveness-audit.md) §"Manual step the audit cannot do" specifies a
`coverage run` pass against the real GUI. **It has not been done** (report §5 Q8). Ten routes
have no caller anywhere in the repo, and static analysis distinguishes *unreferenced* from
*referenced* — never *used* from *unused*. Doing it is one careful hour and it is the only
evidence that can settle those ten. Decide before the session whether to spend it or to defer
those ten rows; do not discover mid-session that the evidence is missing.

**Agenda:**

1. Bucket D, item by item — keep, drop, or defer. D1 (SQL Server), D3 (`smoke.py`),
   D4 (`url_pool_maintenance.py`), D5 (résumé parsing paths), D6 (`n8n workflows/`).

   **Take D1 first — it changes the shape of other work.** Backlog A10 is direct
   evidence: PostgreSQL validates default functions at `CREATE TABLE` where SQLite
   defers to first `INSERT`, so making the migration chain three-dialect-clean means
   maintaining dialect-conditional defaults indefinitely. That cost exists *only*
   because mssql must stay byte-identical. Drop SQL Server and the entire
   proof-based migration exception becomes moot — the history can be squashed to one
   clean SQLite/PostgreSQL baseline, and A6, A8, A9 and A10 collapse into it.

   So: **do not fix A10 before this session.** The right fix depends on the answer.
2. Triage the 78 broad `except` handlers (backlog A3) — which hide dead features, which
   are load-bearing.
3. **Auth posture — backlog A12.** 18 of 42 routes are unauthenticated. Slice 1 pinned
   that as the *current* contract, deliberately without endorsing it. Decide which of
   the 18 should stay public. It is a product question, not a code one, and it must be
   settled before Phase 5 builds a UI against those routes — and before the container
   image is offered to anyone. Route-by-route, using the inventory
   `tests/contracts/test_route_inventory.py` already derives.
3. Bucket C — behavior and feature changes. Currently empty and deliberately yours to
   fill. Decide *what* changes; the changes themselves still land after Phase 4. **A18
   belongs here** — fixing the résumé parse swallow needs a response-schema change, so it
   is a behaviour decision, and D5 cannot be answered until it lands.
4. **Q7 — the five `422 only` routes, and it is a sequencing question, not a deletion one.**
   `/search_stepstone_list`, `/job_details`, `/bundle`, `/aggregate_report` and
   `/api/run_single` have tests that assert schema rejection and nothing else: **0 of 90
   handler statements execute across the five.** Slice 6 extracts services from exactly
   those bodies. The `s1-accept-sets` pass made those assertions precise — it pinned the
   validation contract and named the missing fields — but precision at the schema layer is
   not coverage of the handler, and the two are easy to confuse. So Slice 6's stated
   acceptance criterion ("the contract tests pass unchanged") is **weaker than it looks for
   these five**: the refactor could break all 90 statements and the gate stays green.
   Decide here whether behavioural tests go in before Slice 6 or after, because after means
   writing them against already-moved code.
5. Amend slice scope for anything dropped.

**Exit:** bucket D closed, bucket C populated and scheduled, `refactor-plan.md` scope
amended, Slice 3 unblocked.

---

### CP‑4 — UI design session

**Trigger:** Slice 7 complete (routers extracted, API boundary stable). Phase 5 must not
start.

**Why here.** The UI is built against the API surface, so that surface has to stop
moving first. And it must only cover features that survived CP‑3.

**Chat reads:** the surviving feature list from CP‑3, `docs/architecture.md` route table,
the Log Monitor screenshots in `job agent.md` (Figures 7–8).

**Agenda:**

1. Screen inventory — what exists, what merges, what is dropped.
2. Design system on screen one: spacing scale, type scale, colour tokens, component
   variants, interaction states. Everything after inherits it.
3. Run dashboard specifics — the Log Monitor merge. Accepted / potential / rejected tabs,
   progress, latest-error panel, log tail.
4. First-run experience — the Playwright download and the LLM API key both need onboarding
   UI, not `.env` editing. This is what "one click" actually means in practice.
5. Then write `ui-components/SKILL.md` so screens 2–20 match screen 1.

**Exit:** screen inventory agreed, design system decided, Phase 5 slices drafted.

---

## Unplanned escalation

Stop mid-slice and come to Chat when any of these fire. They are deliberately narrow —
if ten slices pass without one, the briefs are over-specified or the reports are not being
read.

| Trigger | Why it needs Chat |
| --- | --- |
| A move would create an import cycle | Structural. Working around it locally hides the problem. |
| A symbol has an importer the brief did not list | The scope analysis was wrong; the shim strategy may be too. |
| A contract test fails and the fix is not obviously in the moved code | Either the move broke behavior or the oracle is wrong. Both are Chat-level. |
| Fixing a slice requires editing `tests/contracts/` | Forbidden by R8. If it seems necessary, the plan is wrong. |
| A `ci/baseline.json` number would have to go **up** | The ratchet only moves down. Raising it silently widens tolerance for every later slice. |
| Behavior changes that no checklist item covers | Undetectable by the gate — exactly what a human is for. |
| Gate green, outcome visibly wrong | The gate has a hole. Close it with a new executable check, not a note. |

---

## Handoff prompt

What Claude Code prints when it stops. Chat reads the repo directly, so this is a pointer.

```
CHECKPOINT <CP-n> — <name>

State
  Slices complete: <list>
  Gate: pytest <p>/<f> · pyright <n> · ruff <n> · imports <n>
  Blocked: <what cannot start>

Read
  <file paths — do not paste contents>

Decision needed
  <the specific question, one or two sentences>

Options considered so far
  <what the executing agent already ruled in or out, and why>
```

The last block matters. Without it Chat re-derives ground the agent already covered, and
you pay for the same reasoning twice.

---

## When Chat is done

After **CP‑4** closes, Chat has no scheduled role. Slices 8–10 and the Phase 5 build run
on Claude Code and Codex, interrupted only by escalations.

Realistically: CP‑3 and CP‑4 are the last two substantial conversations. Everything else
is minutes.
