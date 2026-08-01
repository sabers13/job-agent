# Large Python Refactor + UI Build — Claude Playbook

A phase-by-phase plan for restructuring a large Python codebase and building a UI
for it, with explicit guidance on **which Claude surface to use for each step**,
what every plugin and skill actually does, and how to keep token cost down.

---

## Surface legend

| Surface | What it is | Use it for |
|---|---|---|
| **Chat** | claude.ai / desktop / mobile chat | Thinking, deciding, reviewing. Nothing touches your repo. You are in the loop every turn. |
| **Claude Code** | Agentic coding tool — terminal, VS Code, JetBrains, or the Code tab in the desktop app | Everything that reads, writes, or verifies code. Full system access: runs your tests, your type checker, your git. |
| **Cowork** | Agentic desktop app for knowledge work, runs in an isolated VM | Documents, spreadsheets, reports, recurring file operations. **Not used in this project's critical path.** |

**The rule:** if the output is code, Claude Code. If the output is a decision, Chat.
If the output is a document or spreadsheet for humans, Cowork.

Cowork is the same execution engine as Claude Code wrapped in a GUI, but it runs
sandboxed and does not expose LSP plugins, the `/effort` dial, or your real Python
environment. Those three things are the backbone of this plan, so Cowork sits out.

---

## Quick reference — where each phase runs

| Phase | Surface | Model | Effort | Output |
|---|---|---|---|---|
| 0. Foundation | Claude Code | Opus 5 | `high` | `CLAUDE.md`, green baseline, plugins installed |
| 1. Map | Claude Code | Opus 5 | `xhigh` | `docs/architecture.md` |
| 2. Plan | Claude Code → **review in Chat** | Opus 5 | `xhigh` | `docs/refactor-plan.md` |
| 3. Skills | Claude Code | Sonnet 5 | `medium` | `.claude/skills/*` |
| 4. Execute | Claude Code, one session per slice | Opus 5 | `xhigh` | Merged slices |
| 4b. Mechanical passes | Claude Code, separate sessions | Sonnet 5 | `low`/`medium` | Docstrings, type hints, renames |
| 5. UI | Claude Code, separate track | Opus 5 | `high` | Components + screens |

---

## Phase 0 — Foundation

**Surface: Claude Code** (plus a text editor for you)
**Model: Opus 5 @ `high`**

Do not skip this. Everything downstream is cheaper and more accurate because of it.

### 0.1 Write `CLAUDE.md` at the repo root

This file loads automatically into every Claude Code session in this repo. It is
the single highest-return artifact you will produce, because facts written here
stop being facts you retype every session — and it caches cleanly, so you pay
about 10% of normal input cost for it after the first turn.

```markdown
# Project
<one paragraph: what this does, who uses it, what it must never break>

## Commands
- Test:       `pytest -q`
- Typecheck:  `pyright --outputjson`
- Lint:       `ruff check . && ruff format .`
- Run:        `<your entry point>`

## Architecture
<layering rules — which packages may import which>
<where the public API boundary is>

## Conventions
- Python 3.x; type hints required on all public functions
- Never introduce `Any` to silence pyright — use `Protocol` or `TypedDict`
- <logging approach, error handling policy, test layout>

## Do not
- Do not modify files outside the scope you were asked about
- Do not add dependencies without asking
- Do not delete a compatibility shim until its slice's tests pass
```

That **Do not** block matters more than it looks. The most common large-refactor
failure is orthogonal changes — the agent "improves" files that were never in
scope, and those edits hide inside a 40-file diff where you will not review them
carefully.

### 0.2 Establish a green baseline

```bash
pytest -q                    # record pass/fail count
pyright --outputjson | jq '.summary'   # record error count
git rev-parse HEAD           # record the commit you started from
```

If you do not know your starting state, you cannot tell whether the refactor
broke something or whether it was already broken.

**If coverage is thin on the modules you plan to restructure, write tests first.**
This is a legitimate use of a whole Claude Code session and it is the cheapest
insurance available. Refactoring without tests is not refactoring; it is rewriting
and hoping.

### 0.3 Work in a git worktree

```bash
git worktree add ../project-refactor -b refactor/restructure
cd ../project-refactor
```

Isolated branch, isolated working directory. A bad session gets thrown away
instead of contaminating your main checkout.

### 0.4 Install plugins

```bash
# Binary FIRST — the plugin is only the wiring
npm install -g pyright
pyright --version            # must resolve on PATH or the plugin silently no-ops

/plugin install pyright-lsp@claude-plugins-official
/plugin install frontend-design@claude-plugins-official
/plugin install security-guidance@claude-plugins-official
/reload-plugins
```

**Verify LSP is actually live before doing anything else.** Ask Claude for
"the definition of `<some function>`". If you get an exact file and line
instantly and see `LSP` in the tool output, it works. If it says "let me search
the codebase" and you see grep or ripgrep, it is not connected — restart Claude
Code and confirm the binary is on PATH. Everything downstream gets slower, less
accurate, and more expensive without it.

---

## Phase 1 — Map before you touch

**Surface: Claude Code** | **Model: Opus 5 @ `xhigh`** | **New session**

Do not ask for changes yet. Ask for understanding, and make the output a file
you commit.

> Explore this repository and write `docs/architecture.md`. Include: a module
> inventory, the real dependency graph, the public API surface, any import
> cycles, and the three modules most coupled to everything else. Use LSP
> `find_references` rather than grep. **Do not modify any code.**

Commit the result. It is now cached context for every later session — you paid
once to discover the architecture instead of rediscovering it every time.

Why `xhigh`: this is a long-horizon exploration task over a large codebase, which
is exactly what the high end of the effort ladder exists for. A wrong map costs
you every token spent executing against it.

---

## Phase 2 — Plan, as a reviewable artifact

**Surface: Claude Code to draft → Chat to review** | **Opus 5 @ `xhigh`**

```
Based on docs/architecture.md, write docs/refactor-plan.md.

Break the restructure into independently shippable slices. For each slice:
  - files touched
  - symbols moved
  - the re-export shim strategy
  - the exact command that verifies it worked

Order slices by dependency — leaves first. Do not write code yet.
```

### Then move to Chat and argue with it

This is the highest-leverage **human** step in the entire project, and it is the
one place where Chat beats Claude Code. Paste the plan into a chat session and
attack it:

- Which slice has the largest blast radius, and can it be split further?
- What breaks if slice 4 ships and slice 5 never does?
- Where does this plan assume behavior that no test currently covers?
- What would you do differently if the goal were minimum risk instead of
  minimum work?

A wrong plan costs you every token spent executing it. Spending an hour in chat
here is the best trade available in the whole project.

---

## Phase 3 — Write your project skills

**Surface: Claude Code** | **Model: Sonnet 5 @ `medium`** (this is authoring
markdown, not reasoning about architecture)

### What skills are, mechanically

A skill is a folder containing a `SKILL.md` file. Only its **name and one-line
description** stay resident in context; the body loads on demand when the
description matches what you are doing. That is the opposite cost profile from
plugins, which put every tool schema in context on every request.

Practical consequence: you can carry a dozen skills cheaply. What you cannot
carry cheaply is a dozen *vaguely described* skills, because a description that
matches everything loads its body constantly.

Project skills live in `.claude/skills/` and get committed, so they apply to
anyone working in the repo. Personal ones live in `~/.claude/skills/`.

### Write these three

```
.claude/skills/
  module-extraction/SKILL.md
  conventions/SKILL.md
  ui-components/SKILL.md      # after Phase 5 screen 1
```

**`module-extraction/SKILL.md`** — the procedure you will repeat 20 times:

```markdown
---
name: module-extraction
description: Use when extracting, moving, or splitting a module in this repo.
  Covers the re-export shim pattern, import-cycle rules, and verification gates.
---

## Procedure
1. Run LSP `find_references` on every public symbol being moved. Do not grep.
2. Create the new module. Leave a re-export shim at the old import path.
3. Migrate call sites in dependency order, leaves first.
4. Run `pyright --outputjson` and `pytest tests/<affected>`. Both must be clean.
5. Remove the shim only after step 4 passes on a full test run.

## Repo constraints
- <your layering rules>
- <your public API boundary>
- Never introduce `Any` to silence pyright; use Protocol or TypedDict.
- If a move would create an import cycle, stop and report rather than working around it.
```

**Description-writing rule:** name the *situation*, not the domain.
"Use when extracting or moving a module" is a good trigger.
"Helps with Python code" matches every request you ever make and loads its body
every time — pure waste.

### Skill anti-patterns

- Skills that try to do too many things
- Skills with vague trigger descriptions
- Skills that duplicate what a well-named slash command would do better
- Skills that restate what is already in `CLAUDE.md`

---

## Phase 4 — Execute, one slice per session

**Surface: Claude Code** | **Model: Opus 5 @ `xhigh`** | **One slice = one session**

### Session discipline

| Rule | Why |
|---|---|
| One slice per session | Fresh context; avoids context rot degrading quality deep into the work |
| Hold effort constant within a session | Changing effort mid-conversation invalidates the prompt cache |
| Enable tool-result clearing | Old file reads stop consuming context and stop diluting attention |
| Gate every slice on `pytest` + `pyright` | A clean-looking diff that type-checks can still be behaviorally wrong |
| Review the diff yourself before merge | Especially for changes outside the slice's stated scope |

### 4b — Mechanical passes go in separate sessions

Docstrings, type hints, renames, test scaffolding. **Sonnet 5 @ `low` or
`medium`**, batched where latency does not matter. No architectural judgment is
required, so you are paying Opus rates for nothing if you leave these in the
main session. Separate sessions also keep your `xhigh` cache prefix intact.

### At milestones, run `/simplify`

Built into Claude Code, no install needed. It spawns three parallel review agents
— code reuse, code quality, efficiency — aggregates the findings and applies
fixes. Post-refactor is exactly when duplicated helpers and dead compatibility
shims accumulate, so this is well aimed.

Run it at slice boundaries, not per file: three parallel agents multiply token use.

---

## Phase 5 — UI, as a completely separate track

**Surface: Claude Code** | **Model: Opus 5 @ `high`** | **Separate sessions,
separate cached context**

`high` rather than `xhigh` here because UI work is an iteration loop and latency
per cycle matters more than depth per cycle.

### The loop

1. Implement the screen
2. Render it and take a screenshot
3. Feed the screenshot back and compare against intent
4. Fix, repeat

Opus 5 is strongest on visual work when given tools to iteratively analyze, crop,
and verify its own output — so the screenshot-verify loop is not optional polish,
it is the mechanism that makes this work at all. One-shotting a UI from a text
description gets you something plausible and wrong.

### Sequence

Settle the design system on **screen one**. Then write `ui-components/SKILL.md`
capturing the decisions — spacing scale, type scale, color tokens, component
variants, interaction states. Then build screens two through twenty.

Without that, every screen is a fresh negotiation and screen 12 will not match
screen 3.

### Why a separate track

Different context needs from the refactor. Mixing them means your cached prefix
carries dead weight through both, and the model's attention is split across two
unrelated mental models.

---

## Plugin reference

Install these three. Not more.

| Plugin | Source | What it actually does | Why you need it here |
|---|---|---|---|
| **`pyright-lsp`** | Official | Wires Microsoft's Pyright static type checker into Claude Code via the Language Server Protocol. Gives real go-to-definition, complete find-all-references, hover types, and live type diagnostics after every edit. | **The single most important install.** Replaces text search with semantic lookup. Before a rename, Claude knows every file that will break rather than guessing from grep hits — including matches it would otherwise find inside comments and follow down the wrong path. Also the biggest token saving available, because grep-based exploration is most of your spend on a large repo. |
| **`frontend-design`** | Official | Shapes aesthetic decisions — typography, spacing, color, layout — toward intentional design rather than defaults. | Raw Claude Code produces functional but generic UI. Since you are building from scratch, this is the difference between your app and every other AI-generated dashboard. |
| **`security-guidance`** | Official | Flags insecure patterns as code is written. | Cheap safety net on a large restructure where you are moving auth, input handling, and data access across module boundaries. |

### Known gotchas

- **Install the language server binary before the plugin.** The plugin is only
  configuration; if `pyright` is not on PATH, the plugin appears enabled and
  does nothing.
- There has been a reported issue where `pyright-lsp` shows as enabled but the
  LSP server never registers, leaving only a README in the plugin cache. Verify
  with a go-to-definition request rather than trusting the plugin list.
- Pyright is memory-hungry on very large codebases. If your machine struggles,
  that is a real trade-off, not a misconfiguration.

### Security note on plugins generally

Anthropic's own docs classify plugins and marketplaces as highly trusted
components that execute code with your user privileges. Treat them like
dependencies you vendor:

- Prefer the official marketplace (auto-registered) over third-party ones
- Check the source repo for recent commits and a clear license
- Wide shell or GitHub access from an unknown publisher is a hard no
- **Install counts and star counts in blog roundups are unreliable** — the same
  plugin is cited at wildly different numbers across articles that copy each
  other. Verify in `/plugin` and on the actual repo.

---

## Optional additions — evaluate after week one

Do not install these on day one. Each one adds context overhead; earn it.

| Add-on | Type | What it does | When it earns its place |
|---|---|---|---|
| **Superpowers** | Plugin (community) | A full methodology as composable skills: brainstorm → design spec → implementation plan → subagent execution → review → merge, plus git-worktree management and TDD loops. | If you find yourself skipping the plan step or Claude jumping straight to code. Front-loads planning and spawns subagents, so it costs tokens — worth it on a multi-week refactor, overkill on a bugfix. |
| **Context7** | Plugin | Fetches current library documentation on demand. | If your project has meaningful third-party dependencies. Opus 5's knowledge cutoff is May 2026; anything newer is guesswork without this. |
| **Karpathy skill** | Skill (community) | A behavioral guardrail file targeting three failure patterns: silent wrong assumptions, over-engineering a 50-line fix into 500, and orthogonal changes to files that were never in scope. | Zero runtime dependencies, it is one markdown file. Read it, keep the parts you agree with, fold them into your `CLAUDE.md`. |
| **`python-lsp-pyright-refactor`** | Skill (third-party) | A shim-first refactor discipline: inventory all definitions and references before changing anything, keep public surfaces stable with re-exports, use static analysis to eliminate type leaks. | Very close to your task. Third-party, so read the SKILL.md before installing — it is just markdown, two minutes to audit. Largely overlaps with the `module-extraction` skill you write in Phase 3. |
| **Trail of Bits `static-analysis`** | Plugin (partner) | Security-focused static analysis, differential review, Semgrep rule creation. | Differential review is the relevant piece — built for answering "did this large change introduce something bad". |
| **`skill-creator`** | Skill (official) | Interactive Q&A that generates well-formed SKILL.md files with proper frontmatter and trigger conditions. | Useful if writing your Phase 3 skills by hand feels fiddly. |

---

## Cost discipline

Ordered by impact. Most people are done after the first two.

### 1. Prompt caching — roughly 80% of available savings

A cache hit costs 10% of standard input. On Opus 5 that is $0.50/MTok instead of
$5.00. The output is byte-identical, so there is no quality dimension to this.

To actually get hits, keep your prefix stable and ordered:

```
system prompt → tool definitions → CLAUDE.md → architecture.md → conversation
                                                                  ↑ volatile stuff last
```

Anything that changes per-request (timestamps, "current file", random IDs) goes
at the very end, or it invalidates everything after it.

**Check that it is working:** every API response reports
`cache_read_input_tokens`. If that is near zero on turn five of a session, your
prefix is unstable and nothing else on this list matters yet.

### 2. Context hygiene — cuts tokens *and* improves quality

This is the counterintuitive one. Past a certain context size, more input makes
output worse, so trimming is not a trade-off.

- **Tool-result clearing** drops old file reads once context passes a threshold.
  Runs server-side; your local history stays intact.
- **Compaction** summarizes the conversation at a threshold and drops everything
  before the summary.
- **Do not paste the repo.** Give a repo map plus grep/LSP tools and let Claude
  pull what it needs. Pasting 200 files means paying for 195 irrelevant ones and
  diluting attention across all of them.

Note: cached tokens still occupy the context window. Caching solves cost, not
context rot. You need both levers.

### 3. Free wins

- **Batch the mechanical work** — 50% off input and output for anything not
  latency-sensitive.
- **Delete verification instructions from old prompts.** Phrases like "include a
  final verification step" or "use a subagent to verify" cause over-verification
  on Opus 5, which already verifies its own work. Pure waste.
- **Trim your tool list.** Every tool schema rides along on every request.
- **Keep `max_tokens` high.** It is a ceiling, not a target — setting it low does
  not reduce usage, it truncates you mid-refactor. 64k is a sane default at
  `xhigh`.

### 4. Effort tuning — the one real trade-off

Lowering effort genuinely trades capability for tokens. Do it by *workload*, not
by *turn*:

- `xhigh` — architecture, planning, multi-file structural slices
- `high` — UI iteration, code review
- `low` / `medium` — docstrings, type hints, renames, test scaffolds

Never economize on effort during architectural work. A bad plan costs more in
rework than the tokens ever saved.

---

## Anti-patterns

| Mistake | Consequence |
|---|---|
| One long session for everything | Context rot degrades quality exactly when the work is hardest |
| Editing before Phases 1–2 exist | You discover the plan was wrong eight files in |
| Trusting a clean diff over a green test run | Code that looks right and type-checks can still be behaviorally wrong |
| Installing ten plugins on day one | Every tool schema rides along on every request |
| Changing `/effort` mid-session | Invalidates your cache; often costs more than the effort step saves |
| Removing a shim before the full suite passes | Turns a reversible slice into a broken main branch |
| Compressing your *input* prompt to save tokens | Clarity that prevents one wrong-direction attempt is worth far more than 200 tokens |

---

## Startup checklist

```
[ ] CLAUDE.md written and committed
[ ] pytest green, pyright error count recorded, start commit noted
[ ] test coverage adequate on modules being restructured (write tests if not)
[ ] git worktree created on a refactor branch
[ ] pyright binary installed and on PATH
[ ] pyright-lsp / frontend-design / security-guidance installed
[ ] LSP verified live via a go-to-definition request
[ ] docs/architecture.md generated and committed          (Phase 1)
[ ] docs/refactor-plan.md generated, reviewed in Chat, revised   (Phase 2)
[ ] .claude/skills/module-extraction + conventions written  (Phase 3)
[ ] cache_read_input_tokens confirmed non-zero in a real session
```

Work through it top to bottom. Do not start Phase 4 until every box above it is
checked — the whole value of Phases 0–3 is that they make Phase 4 cheap,
verifiable, and reversible.
