# 0009 — Log chunks are codepoint-aligned; `max_bytes` is a soft limit

**Status:** Accepted · **Date:** 2026-08-01

## Context

`read_log_chunk(run_id, offset, max_bytes)` is the offset-based log-streaming protocol —
one of the four things `AGENTS.md` names as must-never-break. The GUI polls it in a loop:
read a chunk, keep `next_offset`, pass it back.

It cut the read at `offset + max_bytes` and decoded with `errors="replace"`. When that
boundary landed mid-codepoint the partial sequence became U+FFFD **and** `next_offset`
still pointed mid-sequence, so the following poll resumed inside the same character and
produced more replacement bytes. The character was destroyed on both sides of the
boundary and never recovered.

Whether a given poll corrupted anything depended on byte alignment, so it presented as
intermittent garbling rather than a reproducible failure. This is a German-market job
board: run logs carry city names, job titles and posting text, so multi-byte content is
the normal case.

Found at CP‑1 (B4) by following a test that claimed to pin the property. The two relevant
tests did not intersect — one used multi-byte text but never constructed a boundary, the
other constructed boundaries out of ASCII.

## Decision

**`next_offset` is always a UTF‑8 codepoint boundary.** A chunk that would end mid-character
is shortened to the last complete one; the remainder arrives on the next poll.

**Consequently `max_bytes` is a soft limit — a chunk may exceed it by up to 3 bytes.**

That last part is the decision, not an implementation detail. Shortening alone is not
sufficient: when `max_bytes` is narrower than the character at `offset`, retreating empties
the buffer, so the call returns an empty chunk at an *unchanged* offset and the poll loop
spins forever. That trades a corruption bug for an availability bug. The read is therefore
extended to complete the character instead.

`LOG_CHUNK_MAX_BYTES` (64 KB) remains a hard *safety* cap on memory per request. Three
bytes of slack does not weaken it.

Two boundary cases:

- **At EOF, do not shorten.** A truncated tail is genuinely truncated — there is no later
  poll to complete it — so it decodes to U+FFFD and the offset reaches the end. Withholding
  it would leave `finished` false and the GUI polling indefinitely.
- **Malformed input must still advance.** A stray continuation byte is treated as width 1,
  so a corrupt log drains rather than stalling.

## Consequences

- The protocol is lossless: a client walking the file with the returned offsets reassembles
  it byte-identically at any chunk size. Verified over 300 random mixed-width bodies at
  every size from 1 to 20, plus four malformed inputs.
- **Do not "tighten" this to `assert len(chunk) <= max_bytes`.** The assertion looks correct
  and would be reintroducing the bug. This is the reason the ADR exists — the invariant is
  counter-intuitive and cheap to break from the test side. Also assert on *byte* length, not
  `len(chunk)`: the two agree only for ASCII, which is how the original
  `test_max_bytes_is_capped` passed while measuring the wrong quantity.
- Two private helpers, `_utf8_sequence_length` and `_last_sequence_start`, now carry the
  guarantee. Slice 5 moves this module and must move them with it; they are listed in
  [refactor-plan.md](../refactor-plan.md) §Slice 5 precisely because internal symbols are
  what a move drops silently.
- The fix sketched in [CP-1-REVIEW.md](../CP-1-REVIEW.md) §B4 is retreat-only and therefore
  has the starvation bug described above. That section is annotated, but anyone
  implementing from the review rather than from the code should read this first.

## Alternatives considered

- **Decode with `errors="ignore"`.** Silently drops the partial bytes instead of marking
  them. Same data loss, less visible.
- **Buffer the partial bytes server-side between polls.** Requires per-client state in a
  protocol whose entire virtue is that it is stateless — the offset *is* the state.
- **Return the partial bytes and let the client reassemble.** Changes the response type from
  `str` to something byte-oriented and pushes UTF‑8 handling into the GUI. The protocol
  boundary is the right place to solve this once.
