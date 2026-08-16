# TICKET-26: No validation stage in the pipeline (no Validator, no validate/iter_validate)

## Title
The pipeline has no validation stage. There is no `Validator` that checks
records against column-level rules (type, presence, range, membership), no
batch entry point `validate(records) -> list[ValidationIssue]`, and no lazy
streaming entry point `iter_validate(source) -> Iterator[ValidationIssue]`.

## Evidence
- `ls pipeline/` shows only `__init__.py`, `errors.py`, `ingest.py`,
  `output.py`, `pipeline.py`, `schema.py`, `transform.py`. There is no
  `validate.py` / `validator.py`.
- `grep -rn "validate\|Validation" pipeline/ docs/ tests/` returns no matches
  (the only "valid" hits in `tickets/` are about encoding validity and the
  `schema()` accessor, TICKET-4).
- The phase model in `docs/ARCHITECTURE.md` lists exactly three phases —
  Ingest, Transform, Output — and the phase table in `docs/README.md` has no
  Validation row.
- The existing phases each expose a batch + streaming pair that a validation
  stage would mirror: `transform.py` has `apply_transforms` (line 272) and
  `stream_transforms` (line 291); `output.py` has `write_csv` and
  `iter_write_csv`. No analogous `validate` / `iter_validate` pair exists.
- `pipeline/__init__.py` `__all__` exports no validation symbol.

## Impact
- Records that violate column-level invariants (wrong type, missing required
  column, out-of-range value, value not in an allowed set) flow straight
  through to the Output stage and are written to CSV unchecked. A bad record
  is silently persisted rather than surfaced.
- There is no way to run a data-quality gate between Transform and Output, or
  to collect a report of issues without materializing and hand-checking.
- The batch/streaming symmetry that the rest of the package relies on
  (bounded-memory streaming) is broken for validation: a caller who wants to
  validate a large stream has no lazy path and must materialize the whole
  record list to check it.

## Suggestion
- Add `pipeline/validate.py` with a `Validator` that holds an ordered list of
  column-level rules and exposes:
  - `validate(records: Sequence[record]) -> list[ValidationIssue]` — batch:
    check every record against every rule and return all issues (a record with
    several violations yields several issues; an empty/valid input yields `[]`).
  - `iter_validate(source: Iterator[record]) -> Iterator[ValidationIssue]` —
    lazy streaming: pull one record at a time and yield its issues, so the
    source is never fully materialized (mirrors `stream_transforms`,
    `transform.py:291`).
- A validation stage is a *reporting* stage, not a *filtering* stage: it
  yields `ValidationIssue` values rather than dropping records, so it should
  not be a `Transform` (whose `apply_one` contract returns a record or `None`).
  Keep it a standalone module with its own batch/streaming pair, consistent
  with how `output.py` is a standalone stage rather than a `Transform`.
- Every failure in the module raises `ValidationError` (TICKET-27), never a
  bare `Exception`.
- Export `Validator` (and the rule factories, TICKET-28) from
  `pipeline/__init__.py`.

---
_GitHub issue: TBD_
