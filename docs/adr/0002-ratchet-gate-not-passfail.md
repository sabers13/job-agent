# 0002 — CI gates on a ratchet, not pass/fail

**Status:** Accepted · **Date:** 2026-07-31

## Context

The project had no CI. Adding the obvious thing — a workflow running `pytest && pyright && ruff`
— produces a permanently red build, because the measured baseline is:

```
pytest   18 passed, 6 failed
pyright  32 errors
ruff     747 findings
```

A build that is red on day one gets ignored by day seven. At that point it is worse than no CI,
because it provides the appearance of a safety net.

Fixing all three to green before adding CI is also wrong: the 6 failures are the input to the
test-strategy work (they encode five distinct design flaws), and 672 of the ruff findings are a
mechanical `pyupgrade` pass that must not land inside a structural diff.

## Decision

CI runs `ci/gate.py`, which measures all three and compares against `ci/baseline.json`. **The
build fails only when a number goes up.** Improvements are reported as slack to bank by lowering
the baseline in the same commit that earns it.

## Consequences

- CI is green from the first commit and still catches every regression.
- The baseline file is a visible, reviewable record of technical debt. Lowering it is a
  deliberate act that shows up in a diff.
- The gate is the same in CI and locally — one implementation, no drift between them.

**Cost:** the baseline can be raised to make a build green, which is the exact failure mode it
exists to prevent. Nothing enforces this but review discipline. The comment block in
`baseline.json` says so explicitly.

**Rejected alternative:** marking the 6 known failures `xfail`. It hides them, and an `xfail` that
starts passing is easy to miss. The ratchet keeps the number in view.
