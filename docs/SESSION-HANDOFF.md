# Session Handoff

How to end one session and start the next without re-explaining the project or
re-analysing the codebase.

The principle: **the repo is the memory.** A session is cheap to restart in exact
proportion to how much of it landed in files. Three things have already been lost to
this — A10's deferral, R10's slice, and A11/A12 — each existing only in conversation
until someone noticed.

Do not use `/compact` as the primary strategy. It summarises lossily, discarding the
detail that made the session expensive. End the session and start clean instead.

**The chain rule:** every closing prompt ends by printing the *next* session's opening
prompt with its placeholders filled. You should never have to compose one by hand.

---

## When to transition

| Surface | Transition | Hard signals |
| --- | --- | --- |
| **Claude Code** | End of every slice or bugfix | Re-reads a file it already read · contradicts an earlier decision from the same session · re-derives an established conclusion · past ~half the context window |
| **Codex** | Every task — by design | n/a. The brief is the context; it starts cold on purpose |
| **Chat** | Every checkpoint (CP‑1 … CP‑4) | A new topic needing a lot of file reading · you are scrolling to find an earlier decision |

Quality, not just cost. Past a certain context size more input makes output *worse*, so
restarting buys accuracy and money at the same time.

**Never paste state into a new session.** Point at files. Pasting costs tokens for the
90% that is irrelevant and dilutes attention across all of it.

---

## Claude Code

### CLOSE — paste this verbatim, no edits needed

```
Session close, per docs/SESSION-HANDOFF.md. Do these in order, then stop.

1. Run `make gate`. Record the numbers.

2. Commit everything uncommitted, split by work unit — one logical change
   per commit, with verification evidence in the message (hashes,
   before/after counts). Stage per commit and check
   `git diff --cached --stat` before each one; git leaves things staged
   across commands.

3. Update docs/STATE.md from `git log` and `make gate` — never from memory
   of what was discussed. Delete what is no longer true rather than
   appending. Keep it to one screen. It must contain: what landed, current
   gate numbers, what is blocked and why, and the next three actions.

4. Any decision made this session that is not yet in a file — write it in
   now:
     - a convention or prohibition -> AGENTS.md
     - an architectural choice     -> docs/adr/
     - a parked item               -> docs/backlog.md (mark BLOCKED if it
                                      must not be picked up, and say what
                                      unblocks it)
     - a plan change               -> docs/refactor-plan.md

5. Do not summarise the session. STATE.md is the summary.

6. Finally, print ONLY this block, with every placeholder filled from what
   you just recorded:

   --- NEXT SESSION: <claude-code | codex | chat> ---
   Read AGENTS.md and docs/STATE.md.

   Task: <item 1 of STATE.md "Next three actions", stated concretely>

   Warnings: <anything the next session would otherwise get wrong —
   work in progress on a branch, an item it might pick up that is
   BLOCKED, a stale assumption just corrected, a gate number about to
   move>

   Stop and report if STATE.md disagrees with what you find on disk
   rather than reconciling silently.
   ---
```

Step 4 is the one that matters. Everything else is recoverable from git.

### OPEN — paste the block the previous session printed

If you don't have one, use this and fill it yourself:

```
Read AGENTS.md and docs/STATE.md.

Task: <TASK>

Read other docs only if the task needs them — do not survey the codebase
first. Stop and report if STATE.md disagrees with what you find on disk
rather than reconciling silently.
```

---

## Codex

Cold every time by design. The brief is the whole context.

### OPEN

```
Read AGENTS.md, then tasks/<SLICE-ID>.md. Execute it on branch slice/<ID>.

The Allowlist block is exhaustive — anything changed outside it is a scope
violation. Stop and report on any Stop-and-ask condition rather than
resolving it yourself.
```

### CLOSE

```
Run: make report SLICE=<SLICE-ID> BASE=main

Then fill in ONLY the NARRATIVE section of tasks/<SLICE-ID>.report.md.
The FACTS section is generated — do not edit it. Be specific about:
decisions not in the brief, stop-and-ask conditions hit, slack banked or
left on the table, problems noticed but not fixed, and work left undone.

Print the report path and stop.
```

Interrupted mid-slice? The next session opens with the same brief plus:
`Work already in progress on branch slice/<ID>; check git status before starting.`

---

## Chat

Chat reads the repo directly, so a handoff is a pointer, not a paste.

### OPEN

```
Read docs/STATE.md and AGENTS.md.

I'm at <CP-n | question>. <one line of context>
```

For a checkpoint, Claude Code prints the handoff block from
[CHAT-CHECKPOINTS.md](CHAT-CHECKPOINTS.md) §Handoff prompt — paste that instead.

### CLOSE

```
Write anything decided this session that is not yet in a file into the
right file — AGENTS.md, docs/adr/, docs/backlog.md, docs/refactor-plan.md
or docs/STATE.md. Then print the opening prompt for the next session with
its placeholders filled. Nothing else.
```

---

## Note on slash commands

`/slice-brief` and `/slice-review` do **not** exist yet — `.claude/commands/` is deferred
to Phase 3 ([AGENT-WORKFLOW.md](AGENT-WORKFLOW.md) §8 step 7). Until then, write briefs
from an explicit prompt against §2's schema. `make report SLICE=… BASE=…` does exist.

---

## The invariant

At any moment, a person or agent who has read only `AGENTS.md` and `docs/STATE.md`
should be able to pick up the next action correctly.

If that is not true, STATE.md is stale — and a stale STATE.md is worse than none,
because it gets trusted.
