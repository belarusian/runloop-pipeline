# TICKET-20: No tests for the streaming + composition surface

## Title
`tests/test_transform.py` covers the batch ops and `apply_transforms`, but has
no tests for the Cycle 4 streaming/composition surface (`streamable`,
`stream_transforms`, `compose`, `Composed`). These will ship untested.

## Evidence
- `ls tests/` → `test_transform.py` exists (14 KB) but
  `grep -n "stream_transforms\|compose\|streamable\|Composed"
  tests/test_transform.py` → no matches.
- `pipeline/transform.py` currently has no streaming/composition symbols at
  all (TICKET-16/17/18), so there is nothing to test yet — but the gap will
  persist unless tests are written alongside the implementation.
- `pipeline/ingest.py:91` `iter_rows` is the intended streaming source, but no
  test wires `iter_rows` through a transform stream.

## Impact
- The lazy semantics of `stream_transforms` (yield one record at a time, drop
  on `None`, reject non-streamable ops up front) would have no coverage.
- The `Composed`/`compose` chaining and its `streamable` aggregation would be
  untested, so a regression that materializes the stream or mis-computes
  `streamable` would go unnoticed.

## Suggestion
- Add tests in `tests/test_transform.py` covering, at minimum:
  - `stream_transforms` over a generator yields lazily (assert the source is
    not fully consumed before the first yield).
  - `stream_transforms` drops records where any op's `apply_one` returns
    `None`.
  - `stream_transforms` raises `TransformError` when given a non-streamable op
    (e.g. `Aggregate`), before yielding.
  - `compose`/`Composed` chains `apply_one` and `apply` correctly and reports
    `streamable` as `False` if any member is non-streamable.
  - An end-to-end test wiring `iter_rows` (from a temp CSV) through
    `stream_transforms`.
- Add a purity test asserting the source records are not mutated.

---
_GitHub issue: TBD_
