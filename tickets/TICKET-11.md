# TICKET-11: Transformation phase module `pipeline/transform.py` does not exist

## Title
The pipeline has an Ingestion phase (`pipeline/ingest.py`) but no
Transformation phase. `pipeline/transform.py` is missing, so there is no way to
filter, map, rename, select, or aggregate the records that ingestion produces.

## Evidence
- `ls pipeline/transform.py` → `No such file or directory`.
- `pipeline/__init__.py:1` describes the package as "ingesting and
  **transforming** CSV datasets", but no transform code exists.
- `grep -rniE "Filter|MapColumn|Rename|Select|Aggregate|apply_transforms"
  pipeline/` returns no matches — none of the required ops exist.
- The only "transform" references in the codebase are the `TransformError`
  class (`pipeline/errors.py:23`) and the package docstring.
- `pipeline/ingest.py:44` produces `records: list[dict[str, int | float | str]]`
  that nothing downstream consumes.

## Impact
- The pipeline stops at ingestion. There is no composable, typed way to shape
  records, so the "transforming" half of the package's stated purpose is
  unimplemented.
- Cycle 4 (streaming) has nothing to wire into: there are no per-record
  transform operations to compose over `iter_rows`.

## Suggestion
- Create `pipeline/transform.py` providing:
  - a `Transform` protocol/ABC with `apply(records) -> list[record]` and a
    per-record `apply_one(record) -> record | None`;
  - concrete ops `Filter`, `MapColumn`, `Rename`, `Select`, and `Aggregate`;
  - `apply_transforms(records, transforms)` composing ops in order (pure,
    returns a new list).
- All ops must be pure and allocation-based (new dicts/lists), and every
  failure must raise `TransformError`.
- Export the new public symbols from `pipeline/__init__.py`.
- See `docs/API.md` ("Transformation phase") for the intended interface and
  `docs/ARCHITECTURE.md` for the phase model.

---
_GitHub issue: https://github.com/belarusian/runloop-pipeline/issues/13_
