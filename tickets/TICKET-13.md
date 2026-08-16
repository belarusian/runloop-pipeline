# TICKET-13: No tests for the Transformation phase

## Title
Every implemented module has a dedicated test module, but the Transformation
phase has none. `tests/test_transform.py` does not exist, so the transform ops
and their failure contract will ship untested.

## Evidence
- `ls tests/` → `test_api.py`, `test_errors.py`, `test_ingest.py`,
  `test_schema.py`, `test_smoke.py`. There is no `test_transform.py`.
- Convention: one test module per source module (`test_ingest.py` ↔
  `pipeline/ingest.py`, `test_schema.py` ↔ `pipeline/schema.py`).
- `pipeline/transform.py` does not exist yet (TICKET-11), so there is nothing
  to test — but the gap will persist unless tests are written alongside the
  implementation.
- `tests/test_api.py:10-21` asserts a fixed set of expected exports; it does
  not yet include any transform symbols, so it will not catch a missing
  transform export.

## Impact
- The transform ops (Filter, MapColumn, Rename, Select, Aggregate) and the
  `TransformError` failure contract (TICKET-12) would have zero coverage.
- The "pure, allocation-based, never mutates input" invariant has no test to
  enforce it, so a regression that mutates input records would go unnoticed.

## Suggestion
- Add `tests/test_transform.py` covering, at minimum:
  - each op's happy path (Filter keeps/drops, MapColumn rewrites, Rename
    renames, Select projects, Aggregate groups);
  - purity: input list and its records are not mutated by any op or by
    `apply_transforms`;
  - composition: `apply_transforms` applies ops in order and returns a new
    list;
  - per-record: `apply_one` matches the per-record slice of `apply`;
  - failures: each failure mode raises `TransformError` (see TICKET-12).
- Extend `tests/test_api.py` to include the new transform exports in the
  expected set.

---
_GitHub issue: TBD_
