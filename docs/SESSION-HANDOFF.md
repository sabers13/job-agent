# Session Handoff

How to end one session and start the next without re-explaining the project or
re-analysing the codebase.

The principle: **the repo is the memory.** A session is cheap to restart in exact
proportion to how much of it landed in files. Two things have already been lost to
this — A10's deferral and R10's slice both existed only in conversation until someone
noticed.

Do not use `/compact` as the primary strategy. It summarises lossily, discarding the
detail that made the session expensive. End the session and start clean instead.

---

## When to transition

| Surface | Transition | Hard signals |
| --- | --- | --- |
| **Claude Code** | End of every slice or bugfix | Re-reads a file it already read · contradicts an earlier decision from the same session · re-derives an established conclusion · past ~half the context window |
| **Codex** | Every task — by design | n/a. The brief is the context; it starts cold on purpose |
| **Chat** | Every checkpoint (CP‑1 … CP‑4) | A new topic needing a lot of file reading · you are scrolling to find an earlier decision |

Quality, not just cost. Past a certain context size more input makes output *worse*, so
restarting is not a trade-off — it buys accuracy and money at the same time.

**Never paste state into a new session.** Point at files. Pasting costs tokens for the
90% that is irrelevant and dilutes attention across all of it.

---

## Claude Code

### Closing prompt

```
Session close. Do these in order, then stop.

1. Run `make gate` and record the numbers.
2. Commit everything uncommitted, split by work unit — one logical
   change per commit, with the verification evidence in the message
   (hashes, before/after counts).
3. Update docs/STATE.md: what landed, current gate numbers, next three
   actions, anything newly blocked and why. Keep it to one screen.
   Delete what is no longer true rather than appending.
4. Any decision made this session that is not yet in a file — write it
   in now:
     - a convention or prohibition  -> AGENTS.md
     - an architectural choice      -> docs/adr/
     - a parked item                -> docs/backlog.md (mark BLOCKED if
                                       it must not be picked up, and say
                                       what unblocks it)
     - a plan change                -> docs/refactor-plan.md
5. Print nothing else. Do not summarise the session — STATE.md is the
   summary.
```

Step 4 is the one that matters. Everything else is recoverable from git.

### Opening prompt

```
Read AGENTS.md and docs/STATE.md.

Then execute <TASK> per docs/refactor-plan.md.

Read other docs only if the task needs them — do not survey the
codebase first. If STATE.md and the plan disagree with what you find
on disk, stop and report rather than reconciling silently.
```

That last sentence catches the case where the previous session's STATE.md was written
optimistically.

---

## Codex

Cold every time, by design. Nothing to close beyond the report.

### Closing

```
Run ./scripts/slice_report.sh <slice-id>, then fill in only the
NARRATIVE section of tasks/<slice-id>.report.md. Do not edit FACTS —
it is generated. Print the path and stop.
```

### Opening

```
Read AGENTS.md, then tasks/<slice-id>.md. Execute it.

The Allowlist block is exhaustive — changing anything outside it is a
scope violation. Stop and report on any Stop-and-ask condition rather
than resolving it yourself.
```

If a session is interrupted mid-slice, the next one opens with the same brief plus:
`Work already in progress on branch slice/<id>; check git status before starting.`

---

## Chat

Chat reads the repo directly, so a handoff is a pointer, not a paste.

### Closing

```
Update docs/STATE.md with anything decided this session that is not yet
in a file, then stop.
```

### Opening

```
Read docs/STATE.md and AGENTS.md.

I'm at <CP-n / question>. <one line of context>
```

For a checkpoint, Claude Code prints the handoff block from
[CHAT-CHECKPOINTS.md](CHAT-CHECKPOINTS.md) §Handoff prompt — paste that instead.

---

## The invariant

At any moment, a person or agent who has read only `AGENTS.md` and `docs/STATE.md`
should be able to pick up the next action correctly.

If that is not true, STATE.md is stale — and a stale STATE.md is worse than none,
because it gets trusted.
