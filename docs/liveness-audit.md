# Liveness Audit — spec

Produces the evidence table that the feature session runs on. Read-only; changes nothing.

**Why this exists.** Reviewing ~40 features from recollection is exhausting and unreliable. Two
findings already surfaced from a light structural pass, neither of which anyone was looking for:
`get_db` is dead code with exactly one reference (its own definition), and `_LogSink` means the
URL-pool prune endpoint has *always* silently failed while returning HTTP 200.

Both were mechanical to find. The audit does that systematically, so the product session becomes
"confirm or overrule 40 rows of evidence" instead of "think hard about every feature".

**Sequencing.** Before the extraction slices — see [ADR 0008](adr/0008-deletions-precede-extraction.md).
Every feature dropped is a slice's worth of work not done, so this likely saves more time than it
costs.

---

## Prompt for a Claude Code session

Run at Opus 5 / `xhigh`, in its own session, after `docs/architecture.md` is in context.

> Produce `docs/liveness-report.md`: an evidence table covering every route and every feature in
> this repo, so I can make keep/change/drop decisions without relying on memory.
>
> Use the pyright LSP for reference analysis — not grep. Do not modify any application code.
>
> For each of the 42 user-defined routes, report:
>   - path, method, handler, and the module it will live in after the router split
>   - whether any template under `templates/` or any client calls it
>   - whether it is covered by any test
>   - **whether it can succeed at all** — specifically, flag handlers whose body is wrapped in a
>     broad `except Exception` that returns a success status. An AST scan already found 78 such
>     handlers with no re-raise, 17 of them in `fastapi_run.py`. `_LogSink` was one. Check the
>     rest.
>
> For each public symbol in `app/`, report its LSP reference count, excluding its own definition.
> Zero-reference symbols are deletion candidates. Group them by module.
>
> Cross-reference against coverage: run
> `pytest --cov=app --cov-report=json` and report, per module, which functions have **zero**
> executed statements.
>
> Then produce a ranked deletion-candidate list. For each candidate give: the evidence, what
> depends on it, and what removing it would simplify. Rank by (low usage evidence) x (high
> simplification value).
>
> Explicitly verify the open candidates already recorded in `docs/backlog.md` bucket D — the
> SQL Server path, the two parallel profile stores, `stepstone/smoke.py`,
> `pipeline/url_pool_maintenance.py`, `pipeline/resume_parse.py`, and `n8n workflows/`.
>
> Do not recommend deleting anything on coverage alone. Low coverage means untested, which is not
> the same as unused — say which one the evidence actually supports for each row.

---

## Manual step the audit cannot do

Static analysis shows what *can* be called, not what *is*. One pass of runtime evidence makes the
table far stronger:

```bash
# exercise the app by hand -- every screen, every button you actually use
coverage run --source=app -m uvicorn app.fastapi_run:app --port 5001
# ... use it, then Ctrl-C ...
coverage json -o /tmp/live-usage.json
```

Anything at zero after a genuine session is a deletion candidate with runtime proof rather than a
hunch. Worth one careful hour.

Caveat: this measures *your* usage on *one* pass. A feature used twice a year reads identical to
a dead one. Treat zero-usage as a prompt to decide, never as the decision.

---

## Output contract

`docs/liveness-report.md` containing:

1. **Route table** — 42 rows: path, method, handler, template/client caller, test coverage,
   silent-failure risk.
2. **Symbol reference table** — grouped by module, zero-reference symbols first.
3. **Zero-execution functions** — per module, from coverage JSON.
4. **Ranked deletion candidates** — evidence, dependents, simplification value.
5. **Open questions** — anything the evidence cannot settle, stated as a question rather than
   guessed at.

Section 4 is the input to the feature session. Sections 1–3 are its supporting evidence.
